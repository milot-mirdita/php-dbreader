#include "DBReader.h"

#include <sstream>
#include <fstream>
#include <algorithm>
#include <climits>
#include <cstring>

#include <sys/mman.h>
#include <sys/stat.h>

bool fileExists(const char *name) {
    struct stat st;
    return stat(name, &st) == 0;
}

size_t countLines(std::string name) {
    std::ifstream index(name);
    if (index.fail()) {
        std::ostringstream message;
        message << "Could not open file " << name;
        throw Php::Exception(message.str());
    }

    size_t cnt = 0;
    std::vector<char> buffer(1024 * 1024);
    index.read(buffer.data(), buffer.size());
    while (ptrdiff_t r = index.gcount()) {
        for (size_t i = 0; i < r; i++) {
            const char *p = buffer.data();
            if (p[i] == '\n') {
                cnt++;
            }
        }
        index.read(buffer.data(), buffer.size());
    }
    index.close();

    return cnt;
}

char *mmapData(FILE *file, ssize_t *dataSize, bool writable) {
    struct stat sb;
    fstat(fileno(file), &sb);
    *dataSize = sb.st_size;

    int fd = fileno(file);
    int mode = PROT_READ;
    if (writable) {
        mode |= PROT_WRITE;
    }
    return static_cast<char *>(mmap(NULL, static_cast<size_t>(*dataSize), mode, MAP_PRIVATE, fd, 0));
}


template<typename T>
void DBReader<T>::__construct(Php::Parameters &params) {
    dataFileName = (const char *) params[0];
    indexFileName = (const char *) params[1];
    dataMode = (int32_t) params[2];

    cacheFileName = indexFileName;
    cacheFileName.append(".cache.");
    cacheFileName.append(std::to_string(dataMode));
    cacheFileName.append(".");
    cacheFileName.append(typeid(T).name());

    if (dataMode & USE_DATA) {
        dataFile = fopen(dataFileName.c_str(), "r");
        if (dataFile == NULL) {
            std::ostringstream message;
            message << "Could not open data file " << dataFileName;
            throw Php::Exception(message.str());
        }
        bool writable = static_cast<bool>(dataMode & USE_WRITABLE);
        data = mmapData(dataFile, &dataSize, writable);
    }

    if (fileExists(cacheFileName.c_str())) {
        loadCache(cacheFileName);
        loadedFromCache = true;
        return;
    }

    size = countLines(indexFileName);

    index = new Index[size];

    readIndex(indexFileName, index);

    sortIndex();

    saveCache(cacheFileName);

    loadedFromCache = false;
}

template<typename T>
void DBReader<T>::__destruct() {
    if (dataMode & USE_DATA) {
        munmap(data, static_cast<size_t>(dataSize));
        fclose(dataFile);
    }

    if(loadedFromCache) {
        munmap(index, static_cast<size_t>(size));
    } else {
        delete[] index;
    }
}

template<typename T>
void DBReader<T>::loadCache(std::string fileName) {
    FILE *file = fopen(fileName.c_str(), "rb");
    if (file != NULL) {
        index = (Index*) mmapData(file, &size, true);
        size /= sizeof(Index);
        fclose(file);
    } else {
        std::ostringstream message;
        message << "Could not load index cache from " << fileName;
        throw Php::Exception(message.str());
    }
}

template<typename T>
void DBReader<T>::saveCache(std::string fileName) {
    FILE *file = fopen(fileName.c_str(), "w+b");
    if (file != NULL) {
        Index *tmp = new Index[size];
        for (size_t i = 0; i < size; i++) {
            assignVal(&tmp[i].id, &index[i].id);
            tmp[i].length = index[i].length;
            tmp[i].offset = index[i].offset;
        }
        fwrite(tmp, sizeof(Index), static_cast<size_t>(size), file);
        delete[] tmp;
        fclose(file);
    } else {
        std::ostringstream message;
        message << "Could not save index cache to " << fileName;
        throw Php::Exception(message.str());
    }
}

template<typename T>
void DBReader<T>::assignVal(T *id1, T *id2) {
    *id1 = *id2;
}

template<>
void DBReader<char[32]>::assignVal(char (*id1)[32], char (*id2)[32]) {
    memcpy(id1, id2, 32);
}

template<typename T>
Php::Value DBReader<T>::getId(Php::Parameters &params) {
    T dbKey = params[0];

    Index val;
    val.id = dbKey;
    size_t id = std::upper_bound(index, index + size, val, compareById) - index;

    return (int64_t)((index[id].id == dbKey) ? id : UINT_MAX);
}

template<>
Php::Value DBReader<char[32]>::getId(Php::Parameters &params) {
    Index val;
    memcpy(&val.id, static_cast<const char *>(params[0]), 32);

    size_t id = std::upper_bound(index, index + size, val,
                                 [](Index x, Index y) {
                                     return strcmp(x.id, y.id) <= 0;
                                 }) - index;

    return (int64_t)((strcmp(index[id].id, val.id) == 0) ? id : UINT_MAX);
}

void checkBounds(size_t id, size_t size) {
    if (id >= size) {
        std::ostringstream message;
        message << "Read index " << id << " out of bounds";
        throw Php::Exception(message.str());
    }
}

template<typename T>
Php::Value DBReader<T>::getDbKey(Php::Parameters &params) {
    size_t id = static_cast<size_t>((int64_t) params[0]);

    checkBounds(id, static_cast<size_t>(size));

    return index[id].id;
}

template<typename T>
Php::Value DBReader<T>::getData(Php::Parameters &params) {
    size_t id = static_cast<size_t>((int64_t) params[0]);

    if (!(dataMode & USE_DATA)) {
        throw Php::Exception("DBReader is not open in USE_DATA mode");
    }

    checkBounds(id, static_cast<size_t>(size));

    if ((size_t)(index[id].offset) >= dataSize) {
        throw Php::Exception("Invalid database read");
    }

    return (char*)((size_t)index[id].offset + (size_t)data);
}

template<typename T>
Php::Value DBReader<T>::getLength(Php::Parameters &params) {
    size_t id = static_cast<size_t>((int64_t) params[0]);

    checkBounds(id, static_cast<size_t>(size));

    return (int64_t) index[id].length;
}

template<typename T>
Php::Value DBReader<T>::getOffset(Php::Parameters &params) {
    size_t id = static_cast<size_t>((int64_t) params[0]);

    checkBounds(id, static_cast<size_t>(size));

    return static_cast<int64_t>(index[id].offset);
}

template<typename T>
void DBReader<T>::readIndex(std::string indexFileName, Index *index) {
    std::ifstream indexFile(indexFileName);

    if (indexFile.fail()) {
        std::ostringstream message;
        message << "Could not open index file " << indexFileName;
        throw Php::Exception(message.str());
    }

    char *save;
    size_t i = 0;
    std::string line;
    while (std::getline(indexFile, line)) {
        char *l = (char *) line.c_str();
        readIndexId(&index[i].id, l, &save);
        size_t offset = strtoull(strtok_r(NULL, "\t", &save), NULL, 10);
        size_t length = strtoull(strtok_r(NULL, "\t", &save), NULL, 10);
        if (i >= size) {
            std::ostringstream message;
            message << "Could not read index entry in line " << i;
            throw Php::Exception(message.str());
        }

        index[i].length = length;

        if (dataMode & USE_DATA) {
            index[i].offset = offset;
        } else {
            index[i].offset = 0;
        }

        i++;
    }

    indexFile.close();
}

template<>
void DBReader<int32_t>::readIndexId(int32_t *id, char *line, char **save) {
    *id = strtoull(strtok_r(line, "\t", save), NULL, 10);
}

template<>
void DBReader<char[32]>::readIndexId(char (*id)[32], char *line, char **save) {
    const char *identifier = strtok_r(line, "\t", save);
    memcpy(id, identifier, 32);
}

template<typename T>
void DBReader<T>::sortIndex() { }

template<>
void DBReader<int32_t>::sortIndex() {
    Index *sortArray = new Index[size];
    for (size_t i = 0; i < size; i++) {
        sortArray[i] = index[i];
    }
    std::sort(sortArray, sortArray + size, compareIndexLengthPairById());
    for (size_t i = 0; i < size; ++i) {
        index[i].id = sortArray[i].id;
        index[i].length = sortArray[i].length;
        index[i].offset = sortArray[i].offset;
    }
    delete[] sortArray;
}

template
class DBReader<int32_t>;

template
class DBReader<char[32]>;

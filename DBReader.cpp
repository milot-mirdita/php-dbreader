#include "DBReader.h"

#include <fstream>
#include <algorithm>
#include <climits>
#include <cstring>
#include <cstdio>

#include <sys/mman.h>
#include <sys/stat.h>

#include <typeinfo>

bool fileExists(const char *name) {
    struct stat st;
    return stat(name, &st) == 0;
}

size_t countLines(std::string name) {
    std::ifstream index(name);
    if (index.fail()) {
        throw Php::Exception("Could not open index file");
    }

    size_t cnt = 0;
    std::vector<char> buffer(1024 * 1024);
    index.read(buffer.data(), buffer.size());
    while (size_t r = index.gcount()) {
        for (int i = 0; i < r; i++) {
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

char *mmapData(FILE *file, size_t *dataSize, bool writable) {
    struct stat sb;
    fstat(fileno(file), &sb);
    *dataSize = sb.st_size;

    int fd = fileno(file);
    int mode = PROT_READ;
    if (writable) {
        mode |= PROT_WRITE;
    }
    return (char *) mmap(NULL, *dataSize, mode, MAP_PRIVATE, fd, 0);
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
            throw Php::Exception("Could not open database");
        }
        data = mmapData(dataFile, &dataSize, dataMode & USE_WRITABLE);
    }

    if (fileExists(cacheFileName.c_str())) {
        loadCache(cacheFileName);
        return;
    }

    size = countLines(indexFileName);

    index = new Index[size];

    readIndex(indexFileName, index, data);

    sortIndex();

    saveCache(cacheFileName);
}

template<typename T>
void DBReader<T>::__destruct() {
    if (dataMode & USE_DATA) {
        munmap(data, dataSize);
        fclose(dataFile);
    }
    delete[] index;
}

template<typename T>
void DBReader<T>::loadCache(std::string fileName) {
    FILE *file = fopen(fileName.c_str(), "rb");
    if (file != NULL) {
        fread(&size, sizeof(size_t), 1, file);
        index = new Index[size];
        fread(index, sizeof(Index), size, file);
        for (size_t i = 0; i < size; i++) {
            index[i].data = (char *) (((size_t) index[i].data + (size_t) data));
        }
        fclose(file);
    } else {
        throw Php::Exception("Could not load index cache");
    }
}

template<typename T>
void DBReader<T>::saveCache(std::string fileName) {
    FILE *file = fopen(fileName.c_str(), "w+b");
    if (file != NULL) {
        fwrite(&size, sizeof(size_t), 1, file);
        Index *tmp = new Index[size];
        for (size_t i = 0; i < size; i++) {
            assignVal(&tmp[i].id, &index[i].id);
            tmp[i].length = index[i].length;
            tmp[i].data = (char *) (((size_t) index[i].data - (size_t) data));
        }
        fwrite(tmp, sizeof(Index), size, file);
        delete[] tmp;
        fclose(file);
    } else {
        throw Php::Exception("Could not save index cache");
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

template<typename T>
Php::Value DBReader<T>::getDbKey(Php::Parameters &params) {
    size_t id = (int64_t) params[0];

    if (id >= size) {
        throw Php::Exception("Invalid database read");
    }

    return index[id].id;
}

template<typename T>
Php::Value DBReader<T>::getData(Php::Parameters &params) {
    size_t id = (int64_t) params[0];

    if (!(dataMode & USE_DATA)) {
        throw Php::Exception("DBReader is just open in INDEXONLY mode");
    }

    if (id >= size) {
        throw Php::Exception("Invalid database read");
    }

    if ((size_t)(index[id].data - data) >= dataSize) {
        throw Php::Exception("Invalid database read");
    }

    return index[id].data;
}

template<typename T>
Php::Value DBReader<T>::getLength(Php::Parameters &params) {
    size_t id = (int64_t) params[0];

    if (id >= size) {
        throw Php::Exception("Invalid database read");
    }

    return (int64_t) index[id].length;
}

template<typename T>
Php::Value DBReader<T>::getOffset(Php::Parameters &params) {
    size_t id = (int64_t) params[0];

    if (id >= size) {
        throw Php::Exception("Invalid database read");
    }

    return (int64_t)(index[id].data - data);
}

template<typename T>
void DBReader<T>::readIndex(std::string indexFileName, Index *index, char *data) {
    std::ifstream indexFile(indexFileName);

    if (indexFile.fail()) {
        throw Php::Exception("Could not open index file");
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
            throw Php::Exception("Corrupted Memory");
        }

        index[i].length = length;

        if (dataMode & USE_DATA) {
            index[i].data = data + offset;
        } else {
            index[i].data = NULL;
        }

        i++;
    }

    indexFile.close();
}

//template<>
//void DBReader<std::string>::readIndexId(std::string* id, char* line, char* save) {
//    *id = strtok_r(line, "\t", &save);
//}
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
        index[i].data = sortArray[i].data;
    }
    delete[] sortArray;
}

template
class DBReader<int32_t>;
//
//template
//class DBReader<std::string>;

template
class DBReader<char[32]>;

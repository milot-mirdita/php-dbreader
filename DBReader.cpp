#include "DBReader.h"

#include <sstream>
#include <fstream>

#include <sys/mman.h>
#include <sys/stat.h>

bool fileExists(const std::string& name) {
    struct stat st;
    return stat(name.c_str(), &st) == 0;
}

size_t countLines(const std::string& name) {
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
    if (params.size() < 2) {
        throw Php::Exception("Not enough parameters");
    }

    dataFileName = (const char *) params[0];
    indexFileName = (const char *) params[1];

    if (params.size() == 2) {
        dataMode = USE_DATA;
    } else {
        dataMode = (int32_t) params[2];
    }

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

    std::string cacheFileName = indexFileName;
    cacheFileName.append(".cache.");
    cacheFileName.append(std::to_string(dataMode));
    cacheFileName.append(".");
    cacheFileName.append(typeid(T).name());

    if (fileExists(cacheFileName)) {
        loadCache(cacheFileName);
        loadedFromCache = true;
        return;
    }

    size = countLines(indexFileName);
    index = new Index[size];
    readIndex();
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

    if (loadedFromCache) {
        munmap(index, static_cast<size_t>(size));
    } else {
        delete[] index;
    }
}

template<typename T>
void DBReader<T>::loadCache(std::string fileName) {
    FILE *file = fopen(fileName.c_str(), "rb");
    if (file != NULL) {
        index = (Index *) mmapData(file, &size, true);
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
        fwrite(index, sizeof(Index), static_cast<size_t>(size), file);
        fclose(file);
    } else {
        std::ostringstream message;
        message << "Could not save index cache to " << fileName;
        throw Php::Exception(message.str());
    }
}

template<typename T>
Php::Value DBReader<T>::getId(Php::Parameters &params) {
    if (params.size() < 1) {
        throw Php::Exception("Not enough parameters");
    }

    T dbKey = params[0];

    Index val;
    val.id = dbKey;
    size_t id = std::upper_bound(index, index + size, val, compareById) - index;

    if (id < size && index[id].id == dbKey) {
        return (int64_t) id;
    } else {
        std::ostringstream message;
        message << "Key " << id << " not found in index";
        throw Php::Exception(message.str());
    }
}

template<>
Php::Value DBReader<char[32]>::getId(Php::Parameters &params) {
    if (params.size() < 1) {
        throw Php::Exception("Not enough parameters");
    }

    Index val;
    memcpy(&val.id, static_cast<const char *>(params[0]), 32);

    size_t id = std::upper_bound(index, index + size, val,
                                 [](const Index &x, const Index &y) {
                                     return strcmp(x.id, y.id) <= 0;
                                 }) - index;

    if (id < size && strcmp(index[id].id, val.id) == 0) {
        return (int64_t) id;
    } else {
        std::ostringstream message;
        message << "Key " << id << " not found in index";
        throw Php::Exception(message.str());
    }
}

void checkBounds(size_t id, size_t size) {
    if (id >= size) {
        std::ostringstream message;
        message << "Index " << id << " out of bounds";
        throw Php::Exception(message.str());
    }
}

template<typename T>
Php::Value DBReader<T>::getDbKey(Php::Parameters &params) {
    if (params.size() < 1) {
        throw Php::Exception("Not enough parameters");
    }

    size_t id = static_cast<size_t>((int64_t) params[0]);

    checkBounds(id, static_cast<size_t>(size));

    return index[id].id;
}

template<typename T>
Php::Value DBReader<T>::getData(Php::Parameters &params) {
    if (params.size() < 1) {
        throw Php::Exception("Not enough parameters");
    }

    size_t id = static_cast<size_t>((int64_t) params[0]);

    if (!(dataMode & USE_DATA)) {
        throw Php::Exception("DBReader is not open in USE_DATA mode");
    }

    checkBounds(id, static_cast<size_t>(size));

    if ((size_t) (index[id].offset) >= dataSize) {
        throw Php::Exception("Invalid database read");
    }

    return (char *) ((size_t) index[id].offset + (size_t) data);
}

template<typename T>
Php::Value DBReader<T>::getLength(Php::Parameters &params) {
    if (params.size() < 1) {
        throw Php::Exception("Not enough parameters");
    }

    size_t id = static_cast<size_t>((int64_t) params[0]);

    checkBounds(id, static_cast<size_t>(size));

    return (int64_t) index[id].length;
}

template<typename T>
Php::Value DBReader<T>::getOffset(Php::Parameters &params) {
    if (params.size() < 1) {
        throw Php::Exception("Not enough parameters");
    }

    size_t id = static_cast<size_t>((int64_t) params[0]);

    checkBounds(id, static_cast<size_t>(size));

    return static_cast<int64_t>(index[id].offset);
}

template<typename T>
void readIndexId(T *, char *, char **) { }

template<>
void readIndexId(int32_t *id, char *line, char **save) {
    *id = static_cast<int32_t>(strtol(strtok_r(line, "\t", save), NULL, 10));
}

template<>
void readIndexId(char (*id)[32], char *line, char **save) {
    const char *identifier = strtok_r(line, "\t", save);
    memcpy(id, identifier, 32);
}

template<typename T>
void DBReader<T>::readIndex() {
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
        readIndexId<T>(&index[i].id, l, &save);
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

template<typename T>
void DBReader<T>::sortIndex() { }

template<>
void DBReader<int32_t>::sortIndex() {
    std::sort(index, index + size, compareIndexLengthPairById());
}

template
class DBReader<int32_t>;

template
class DBReader<char[32]>;

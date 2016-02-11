#ifndef DBREADER_H
#define DBREADER_H

// Written by Martin Steinegger & Maria Hauser mhauser@genzentrum.lmu.de
//
// Manages DB read access.
//

#include <cstddef>
#include <string>

#include <phpcpp.h>

template<typename T>
class DBReader : public Php::Base {
public:
    static const int USE_INDEX = 0;
    static const int USE_DATA = 1;
    static const int USE_WRITABLE = 2;

    void __construct(Php::Parameters &params);

    void __destruct();

    Php::Value getSize() {
        return (int64_t) size;
    }

    Php::Value getDataSize() {
        return (int64_t) dataSize;
    }

    // does a binary search in the ffindex and returns index of the entry with dbKey
    // returns UINT_MAX if the key is not contained in index
    Php::Value getId(Php::Parameters &params);

    Php::Value getData(Php::Parameters &params);

    Php::Value getDbKey(Php::Parameters &params);

    Php::Value getLength(Php::Parameters &params);

    Php::Value getOffset(Php::Parameters &params);


private:
    std::string dataFileName;
    std::string indexFileName;
    std::string cacheFileName;
    int dataMode;

    // size of all data stored in ffindex
    size_t dataSize;
    char *data;
    FILE *dataFile;

    struct Index {
        T id;
        size_t length;
        size_t offset;
    };

    // number of entries in the index
    size_t size;
    Index *index;
    bool loadedFromCache;

    static bool compareById(Index x, Index y) {
        return (x.id <= y.id);
    }

    size_t bsearch(const Index *index, size_t size, T value);

    void readIndex(std::string indexFileName, Index *index, char *data);

    void readIndexId(T* id, char* line, char** save);

    void assignVal(T* id1, T* id2);

    void loadCache(std::string fileName);
    void saveCache(std::string fileName);

    struct compareIndexLengthPairById {
        bool operator()(const Index &lhs, const Index &rhs) const {
            return (lhs.id < rhs.id);
        }
    };

    void sortIndex();
};

#endif

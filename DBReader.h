#ifndef DBREADER_H
#define DBREADER_H

// php_dbreader Written by Milot Mirdita milot@mirdita.de
// php_dbreader is a low latency version of the mmseqs db access library
// Original Implementation from MMseqs
// Written by Martin Steinegger
// & Maria Hauser mhauser@genzentrum.lmu.de
//

#include <cstddef>
#include <string>

#include <phpcpp.h>

template<typename T>
class DBReader : public Php::Base {
public:
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
    Php::Value getId(Php::Parameters &params);

    Php::Value getData(Php::Parameters &params);

    Php::Value getDbKey(Php::Parameters &params);

    Php::Value getLength(Php::Parameters &params);

    Php::Value getOffset(Php::Parameters &params);


private:
    std::string dataFileName;
    std::string indexFileName;
    int dataMode;

    // size of all data stored in ffindex
    ssize_t dataSize;
    char *data;
    FILE *dataFile;

    struct Index {
        T id;
        size_t length;
        size_t offset;
    };

    // number of entries in the index
    ssize_t size;
    Index *index;
    bool loadedFromCache;

    static bool compareById(const Index &x, const Index &y) {
        return (x.id <= y.id);
    }

    void readIndex();
    void sortIndex();

    struct compareIndexLengthPairById {
        bool operator()(const Index &lhs, const Index &rhs) const {
            return (lhs.id < rhs.id);
        }
    };

    void loadCache(std::string fileName);
    void saveCache(std::string fileName);
};

#endif

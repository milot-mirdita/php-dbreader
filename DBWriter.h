#ifndef DBWRITER_H
#define DBWRITER_H

// Written by Martin Steinegger & Maria Hauser mhauser@genzentrum.lmu.de
// 
// Manages ffindex DB write access.
// For parallel write access, one ffindex DB per thread is generated.
// After the parallel calculation is done, all ffindexes are merged into one.
//

#include <string>
#include <vector>
#include "DBReader.h"
#include <phpcpp.h>

class DBWriter {
    public:
        static const size_t ASCII_MODE = 0;
        static const size_t BINARY_MODE = 1;

        DBWriter(const std::string& dataFileName, const std::string& indexFileName, int32_t mode = ASCII_MODE);

        ~DBWriter();

        void write(int32_t key, const std::string& data);

private:
    std::string dataFileName;
    std::string indexFileName;

    FILE* dataFile;
    FILE* indexFile;

    size_t offset;
    size_t entries;

    std::string datafileMode;

    std::vector<DBReader<int32_t>::Index> index;
};

class PhpDBWriter : public Php::Base {
public:
    void __construct(Php::Parameters &params) {
        if (params.size() < 2) {
            throw Php::Exception("Not enough parameters");
        }

        int32_t dataMode = DBWriter::ASCII_MODE;
        if (params.size() > 2) {
            dataMode = (int32_t) params[3];
        }


        dbWriter = new DBWriter((const std::string&) params[0], (const std::string&) params[1], dataMode);
    }

    void __destruct() {
        delete dbWriter;
    };

    void write(Php::Parameters &params) {
        if (params.size() < 2) {
            throw Php::Exception("Not enough parameters");
        }

        dbWriter->write((int32_t) params[0], (const std::string&) params[1]);
    }
private:
    DBWriter* dbWriter;
};

#endif

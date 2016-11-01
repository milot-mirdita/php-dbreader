#include <cstdlib>
#include <cstdio>
#include <sstream>
#include <fstream>
#include <sys/stat.h>

#include "DBWriter.h"
#include "itoa.h"

void errorIfFileExist(const std::string& file){
    struct stat st;
    if(stat(file.c_str(), &st) == 0) {
        std::ostringstream message;
        message << sys_errlist[EEXIST];
        throw Php::Exception(message.str());
    }
}

DBWriter::DBWriter(const std::string& dataFileName,
                   const std::string& indexFileName,
                   int32_t mode) : dataFileName(dataFileName), indexFileName(indexFileName) {

    if (mode == ASCII_MODE) {
        datafileMode = "w";
    } else if (mode == BINARY_MODE) {
        datafileMode = "wb";
    } else {
        std::ostringstream message;
        message << "No right mode for DBWriter " << indexFileName;
        throw Php::Exception(message.str());
    }

    errorIfFileExist(dataFileName);
    errorIfFileExist(indexFileName);

    dataFile = fopen(dataFileName.c_str(), datafileMode.c_str());
    indexFile = fopen(indexFileName.c_str(), "w");

    if (dataFile == NULL) {
        std::ostringstream message;
        message << sys_errlist[errno];
        throw Php::Exception(message.str());
    }

    if (indexFile == NULL) {
        std::ostringstream message;
        message << sys_errlist[errno];
        throw Php::Exception(message.str());
    }
}

void writeIndex(FILE *outFile, DBReader<int32_t>::Index *index, size_t indexSize){
    char buff1[1024];
    for(size_t id = 0; id < indexSize; id++){
        char * tmpBuff = u32toa_sse2((uint32_t)index[id].id, buff1);
        *(tmpBuff-1) = '\t';
        size_t currOffset = index[id].offset;
        tmpBuff = u64toa_sse2(currOffset, tmpBuff);
        *(tmpBuff-1) = '\t';
        uint32_t sLen = index[id].length;
        tmpBuff = u32toa_sse2(sLen,tmpBuff);
        *(tmpBuff-1) = '\n';
        *(tmpBuff) = '\0';
        fwrite(buff1, sizeof(char), strlen(buff1), outFile);
    }
}

DBWriter::~DBWriter() {
    std::stable_sort(index.begin(), index.end(), DBReader<int32_t>::compareIndexLengthPairById());
    writeIndex(indexFile, index.data(), entries);

    fclose(dataFile);
    fclose(indexFile);
}

void DBWriter::write(int32_t key, const std::string& data) {
    size_t offsetStart = offset;
    size_t dataSize = data.length();
    size_t written = fwrite(data.c_str(), sizeof(char), dataSize, dataFile);
    if (written != dataSize) {
        std::ostringstream message;
        message << "Could not write to data file " << dataFileName;
        throw Php::Exception(message.str());
    }
    offset += written;

    // entries are always separated by a null byte
    char nullByte = '\0';
    written = fwrite(&nullByte, sizeof(char), 1, dataFile);
    if (written != 1) {
        std::ostringstream message;
        message << "Could not write to data file " << dataFileName;
        throw Php::Exception(message.str());
    }
    offset += 1;

    size_t length = offset - offsetStart;
    DBReader<int32_t>::Index entry;
    entry.id = key;
    entry.length = length;
    entry.offset = offset;
    index.push_back(entry);

    entries++;
}

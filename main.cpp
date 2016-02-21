#include <phpcpp.h>
#include <iostream>

#include "DBReader.h"

/**
 *  tell the compiler that the get_module is a pure C function
 */
extern "C" {
    
    /**
     *  Function that is called by PHP right after the PHP process
     *  has started, and that returns an address of an internal PHP
     *  strucure with all the details and features of your extension
     *
     *  @return void*   a pointer to an address that is understood by PHP
     */
    PHPCPP_EXPORT void *get_module() 
    {
        static Php::Extension extension("dbreader", "0.1");

        Php::Class<DBReader<int32_t>> intDB("IntDBReader");
        intDB.method("__construct", &DBReader<int32_t>::__construct);
        intDB.method("__destruct", &DBReader<int32_t>::__destruct);
        intDB.method("getDataSize", &DBReader<int32_t>::getDataSize);
        intDB.method("getSize", &DBReader<int32_t>::getSize);
        intDB.method("getData", &DBReader<int32_t>::getData);
        intDB.method("getDbKey", &DBReader<int32_t>::getDbKey);
        intDB.method("getLength", &DBReader<int32_t>::getLength);
        intDB.method("getOffset", &DBReader<int32_t>::getOffset);
        intDB.method("getId", &DBReader<int32_t>::getId);

        intDB.property("USE_DATA", "1", Php::Public | Php::Static);
        intDB.property("USE_WRITABLE", "2", Php::Public | Php::Static);

        extension.add(std::move(intDB));

        Php::Class<DBReader<char[32]>> stringDB("StringDBReader");
        stringDB.method("__construct", &DBReader<char[32]>::__construct);
        stringDB.method("__destruct", &DBReader<char[32]>::__destruct);
        stringDB.method("getDataSize", &DBReader<char[32]>::getDataSize);
        stringDB.method("getSize", &DBReader<char[32]>::getSize);
        stringDB.method("getData", &DBReader<char[32]>::getData);
        stringDB.method("getDbKey", &DBReader<char[32]>::getDbKey);
        stringDB.method("getLength", &DBReader<char[32]>::getLength);
        stringDB.method("getOffset", &DBReader<char[32]>::getOffset);
        stringDB.method("getId", &DBReader<char[32]>::getId);

        stringDB.property("USE_DATA", "1", Php::Public | Php::Static);
        stringDB.property("USE_WRITABLE", "2", Php::Public | Php::Static);

        extension.add(std::move(stringDB));

        return extension;
    }
}

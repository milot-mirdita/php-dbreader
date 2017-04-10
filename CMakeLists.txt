cmake_minimum_required(VERSION 3.0 FATAL_ERROR)
project(php_dbreader CXX)

set(CMAKE_CXX_STANDARD 11)
set(CMAKE_POSITION_INDEPENDENT_CODE TRUE)

set(php_dbreader_source_files
        DBReader.h
        DBReader.cpp
        itoa.h
        DBWriter.h
        DBWriter.cpp
        main.cpp)

add_library(dbreader SHARED ${php_dbreader_source_files})
target_link_libraries(dbreader phpcpp)
set_target_properties(dbreader
        PROPERTIES
        PREFIX ""
        SUFFIX ".so")

execute_process(COMMAND php-config --extension-dir
        OUTPUT_VARIABLE PHP_EXTENSION_DIR
        OUTPUT_STRIP_TRAILING_WHITESPACE)

install(TARGETS dbreader LIBRARY DESTINATION ${PHP_EXTENSION_DIR})

#pragma once

#include <string>
#include "status.h"

using namespace std;

class WAL{
public:
    virtual ~WAL() = default;

    virtual Status appendPut(const string &key ,const string & value) = 0;
    virtual Status appendDel(const string &key) = 0;
    virtual Status sync() = 0;
};
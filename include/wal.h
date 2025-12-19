#pragma once

#include <string>
#include <functional>
#include "status.h"

using namespace std;

enum class WalOpType {
    PUT,
    DEL
};
class WAL{
    public:
        virtual ~WAL() = default;

        virtual Status appendPut(const string &key ,const string & value) = 0;
        virtual Status appendDel(const string &key) = 0;
        virtual Status sync() = 0;
        virtual Status replay(
            const function<void(WalOpType,const string& ,const string&) >& fn
        ) = 0;
};

// Factory method to create a WAL instance
WAL* CreateWAL(const string &path);
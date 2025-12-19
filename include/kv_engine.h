#pragma once

#include <string>
#include "status.h"

using namespace std;

class KVEngine {
    public:
        virtual ~KVEngine() = default;

        virtual Status put(const string &key, const string &value) = 0;
        virtual Status get(const string &key, string* value) = 0;
        virtual Status del(const string &key) = 0;
};
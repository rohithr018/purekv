#include "wal.h"

using namespace std;

class WALImp:public WAL{
    public:
    Status appendPut(const string &key ,const string & value) override{
        return Status::Error("see you later");
    }
    Status appendDel(const string &key) override{
        return Status::Error("see you later");
    }
    Status sync() override{
        return Status::Error("see you later");
    }
};

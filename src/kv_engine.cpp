#include "kv_engine.h"

using namespace std;

class KVEngineImpl : public KVEngine {
    public:
    Status put(const string &,const string &)override{
        return Status::Error("see you later");
    }
    Status get(const string &, string*) override{
        return Status::Error("see you later");
    }
    Status del(const string &) override{
        return Status::Error("see you later");
    }

};

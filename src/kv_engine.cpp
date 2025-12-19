#include "kv_engine.h"
#include <unordered_map>
#include <mutex>
#include "wal.h"

using namespace std;

class KVEngineImpl : public KVEngine {

    private:
        unordered_map<string, string> store_;
        mutex mu_;    
        WAL* wal_;

    public:
        KVEngineImpl() :wal_(CreateWAL("wal/kv.wal")){
            wal_->replay(
                [this](WalOpType type, const string &key, const string &value){
                    if(type==WalOpType::PUT){
                        store_[key]=value;
                    } else if(type==WalOpType::DEL){
                        store_.erase(key);
                    }
                }
            );
        }

        ~KVEngineImpl(){
            delete wal_;
        }

        Status put(const string & key,const string & value) override{

            lock_guard<mutex> lock(mu_);
            wal_->appendPut(key, value);
            store_[key]=value;

            return Status::OK();
        }

        Status get(const string & key, string* value) override{

            lock_guard<mutex> lock(mu_);
            auto it=store_.find(key);
            if(it==store_.end()){
                return Status::Error("KEY_NOT_FOUND");
            }
            *value=it->second;
            return Status::OK();
        }

        Status del(const string & key) override{

            lock_guard<mutex> lock(mu_);
            auto it=store_.find(key);
            if(it==store_.end()){
                return Status::Error("KEY_NOT_FOUND");
            }   
            wal_->appendDel(key);
            store_.erase(key);
            return Status::OK();
        }

};

// Factory implementation
KVEngine* CreateKVEngine() {
    return new KVEngineImpl();
}
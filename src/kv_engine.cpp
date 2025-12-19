#include "kv_engine.h"
#include <unordered_map>
#include <mutex>
#include "wal.h"
#include "segment.h"
#include <sstream>
#include <unistd.h>

using namespace std;

class KVEngineImpl : public KVEngine {

    private:
        unordered_map<string, string> store_;
        vector<string> segments_;
        size_t mem_limit = 5;
        size_t compaction_threshold = 3;
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
            if(store_.size()>=mem_limit){
                flush_memtable();
            }

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

        void flush_memtable(){

            ostringstream name;
            name << "segments/seg_"<<segments_.size()<<".sst";
            write_segment(name.str(), store_);
            segments_.push_back(name.str());
            store_.clear();

            if(segments_.size()>=compaction_threshold){
                compact_segments();
            }

        }
        
        void  compact_segments(){
            unordered_map<string, string> merged;
            for(const auto &seg: segments_){
                read_segment(seg,merged);
            }

            ostringstream name;
            name << "segments/seg_"<<segments_.size()<<".sst";
            write_segment(name.str(), merged);

            vector<string> old=segments_;
            segments_.clear();
            segments_.push_back(name.str());

            for(const auto &seg: old){
                unlink(seg.c_str());
            }
        }

};

// Factory implementation
KVEngine* CreateKVEngine() {
    return new KVEngineImpl();
}
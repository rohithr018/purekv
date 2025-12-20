#include "kv_engine.h"
#include <unordered_map>
#include <mutex>
#include "wal.h"
#include "segment.h"
#include <sstream>
#include <unistd.h>
#include <shared_mutex>
#include <fcntl.h>
#include <filesystem>
#include <cstring>
#include <zlib.h>

using namespace std;

class KVEngineImpl : public KVEngine {

    private:
        unordered_map<string, string> store_;
        vector<string> segments_;
        size_t mem_limit = 5;
        size_t compaction_threshold = 3;   
        WAL* wal_;

        mutable shared_mutex mem_mu_;
        mutex wal_mu_;
        mutex seg_mu_;

    public:
        KVEngineImpl() :wal_(CreateWAL("wal/kv.wal")){
            
            wal_->replay(
                [this](WalOpType type, const string &key, const string &value){
                    unique_lock<shared_mutex>lock(mem_mu_);
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
            {
                lock_guard<mutex> wlock(wal_mu_);
                wal_->appendPut(key, value);
            }
            {
                unique_lock<shared_mutex>mlock(mem_mu_);
                store_[key]=value;
            }

            bool flush_needed=false;

            {
                shared_lock<shared_mutex> rlock(mem_mu_);   
                if(store_.size()>=mem_limit){
                    flush_needed=true;
                }
            }

            if(flush_needed){
                flush_memtable();
            }

            return Status::OK();
        }

        Status get(const string & key, string* value) override{
            {

                shared_lock<shared_mutex> rlock(mem_mu_);
                auto it=store_.find(key);
                if(it!=store_.end()){
                    *value=it->second;
                    return Status::OK();
                }
            }
            {
                lock_guard<mutex>slock(seg_mu_);
                for(auto it=segments_.rbegin();it!=segments_.rend();++it){
                    if(read_from_segment(*it,key,value))return Status::OK();
                }
                
            }
            return Status::Error("KEY_NOT_FOUND");
        }

        Status del(const string & key) override{
            {

                lock_guard<mutex> wlock(wal_mu_);
                wal_->appendDel(key);
            }
            {
                unique_lock<shared_mutex>lock(mem_mu_);
                auto it=store_.find(key);
                if(it==store_.end()){
                    return Status::Error("KEY_NOT_FOUND");
                }   
                store_.erase(key);
            }

            return Status::OK();
        }

        void flush_memtable(){

            unordered_map<string,string>snapshot;

            {
                unique_lock<shared_mutex>lock(mem_mu_);
                snapshot.swap(store_);   
            }


            ostringstream name;
            name << "segments/seg_"<<segments_.size()<<".sst";
            write_segment(name.str(), snapshot);

            {
                lock_guard<mutex> lock(seg_mu_);
                segments_.push_back(name.str());
            }

            if(segments_.size()>=compaction_threshold){
                compact_segments();
            }

        }
        
        void  compact_segments(){
            vector<string>local_segments;
            {
                lock_guard<mutex> lock(seg_mu_);
                local_segments.swap(segments_);
            }
            unordered_map<string, string> merged;
            for(const auto &seg: local_segments){
                read_segment(seg,merged);
            }
            ostringstream name;
            name << "segments/seg_"<<segments_.size()<<".sst";
            write_segment(name.str(), merged);

            {
                lock_guard<mutex> lock(seg_mu_);
                for(const auto &seg: local_segments){
                    unlink(seg.c_str());
                }
                segments_.clear();
                segments_.push_back(name.str());
            }
        }

        static bool read_from_segment(
            const string & path,
            const string & key,
            string* value
        ){
            int fd=open(path.c_str(),O_RDONLY);
            if(fd<0){
                return false;
            }
            while(true){
                uint32_t stored_crc;
                if(read(fd, &stored_crc, sizeof(stored_crc)) != sizeof(stored_crc)) break;

                uint32_t klen,vlen;
                
                if(read(fd,&klen,sizeof(klen))!=sizeof(klen))break;
                if (read(fd,&vlen,sizeof(vlen))!=sizeof(vlen))break;


                vector<char>buf(sizeof(klen)+sizeof(vlen)+klen+vlen);
                size_t off=0;

                memcpy(buf.data()+off,&klen,sizeof(klen));off+=sizeof(klen);
                memcpy(buf.data()+off,&vlen,sizeof(vlen));off+=sizeof(vlen);

                if(read(fd,buf.data()+off,klen+vlen)!=(ssize_t)(klen+vlen))break;

                uint32_t crc=crc32(
                    0,
                    reinterpret_cast<const Bytef*>(buf.data()),
                    buf.size()
                );

                if(crc!=stored_crc) break;

                string k(buf.data() + sizeof(klen) + sizeof(vlen), klen);
                string v(buf.data() + sizeof(klen) + sizeof(vlen) + klen, vlen);

                if(k==key){
                    *value=v;
                    close(fd);
                    return true;
                }
            }
            close(fd);
            return false;
        }

};

// Factory implementation
KVEngine* CreateKVEngine() {
    return new KVEngineImpl();
}
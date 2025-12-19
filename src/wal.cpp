#include "wal.h"
#include <fstream>
#include <mutex>
#include <vector>
#include <cstring>
#include <unistd.h>
#include <fcntl.h>
#include <functional>
#include <sys/stat.h>
#include <zlib.h>

using namespace std;

/*
    | uint32 checksum |
    | uint8  type     |   (1 = PUT, 2 = DEL)
    | uint32 key_len  |
    | uint32 val_len  |
    | key bytes       |
    | value bytes     |  (only for PUT)
*/

static const uint8_t REC_PUT = 1;
static const uint8_t REC_DEL = 2;

class WALImpl:public WAL{

    private:
        string path_;
        int fd_;
        mutex mu_;

        Status append(uint8_t type, const string &key, const string &value){
            lock_guard<mutex> lock(mu_);

            uint32_t klen = key.size();
            uint32_t vlen = value.size();

            vector<char>buf;
            buf.resize(1 + 4 + 4 + klen + vlen);
            
            size_t off = 0;
            buf[off++] = type;

            memcpy(&buf[off], &klen, 4);off +=4;
            memcpy(&buf[off], &vlen, 4);off +=4;
            memcpy(&buf[off], key.data(), klen);off +=klen;
            if(type == REC_PUT){
                memcpy(&buf[off], value.data(), vlen);
            }

            uint32_t crc = crc32(0,
                reinterpret_cast<const Bytef *>(buf.data()),
                buf.size() 
            );

            write(fd_, &crc, 4);
            write(fd_, buf.data(), buf.size());

            fsync(fd_);
            return Status::OK();
        }


    public:

        WALImpl(const string &path):path_(path), fd_(-1){
            fd_ = open(path.c_str(), O_CREAT | O_APPEND| O_WRONLY, 0644);

        }

        Status appendPut(const string &key ,const string & value) override{
            return append(REC_PUT, key, value);
        }

        Status appendDel(const string &key) override{
            return append(REC_DEL, key, "");
        }

        Status sync() override{
            if(fd_<0){
                return Status::Error("WAL_NOT_OPEN");
            }
            fsync(fd_);
            return Status::OK();
        }

        Status replay(const function<void(WalOpType, const string&, const string&)>& fn) override {
            
            int rfd = open(path_.c_str(), O_RDONLY);
            if (rfd < 0) return Status::OK();
            while(true){
                uint32_t stored_crc =0;
                ssize_t n= read(rfd, &stored_crc, 4);
                if(n==0)break; //EOF
                if(n!=4)break; //partial


                uint8_t type;
                uint32_t klen, vlen;

                if(read(rfd, &type,1)!=1)break;
                if(read(rfd, &klen,4)!=4)break;
                if(read(rfd, &vlen,4)!=4)break;

                vector<char>buf;
                buf.resize(1 + 4 + 4 + klen + vlen);

                size_t off =0;
                buf[off++] = type;

                memcpy(&buf[off], &klen, 4);off +=4;
                memcpy(&buf[off], &vlen, 4);off +=4;

                if(read(rfd, buf.data()+off, klen + vlen)!=(ssize_t)(klen + vlen))break;

                uint32_t crc = crc32(0,
                    reinterpret_cast<const Bytef *>(buf.data()),
                    buf.size() 
                );

                if(crc != stored_crc){
                    break; //corrupted record
                }

                string key(buf.data() + 9, klen);
                string value;
                if(type == REC_PUT){
                    value.assign(buf.data() + 9 + klen, vlen);
                    fn(WalOpType::PUT, key, value);
                }else if(type == REC_DEL){
                    fn(WalOpType::DEL, key, "");
                }
            }
            close(rfd);
            return Status::OK();
        }

        ~WALImpl(){
            if(fd_ != -1)close(fd_);
        }
};

// Factory Implementation
WAL* CreateWAL(const string &path){
    return new WALImpl(path);
}

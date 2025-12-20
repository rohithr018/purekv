#include "segment.h"
#include <fcntl.h>
#include <unistd.h>
#include <cstring>
#include <cstdint>
#include <zlib.h>
#include <vector>


using namespace std;

/*
    | uint32 crc     |
    | uint32 key_len |
    | uint32 val_len |
    | key bytes      |
    | value bytes    |

*/

static Status write_all(
    int fd,
    const void*buf,size_t len
){
    const char * p=static_cast<const char*>(buf);
    while(len>0){
        ssize_t n = write(fd,p,len);

        if(n<=0){
            return Status::Error("SEGMENT_WRITE_FAILED");
        }
        p+=n;
        len-=n;
    }
    return Status::OK();
}

Status write_segment(
    const string &path,
    const unordered_map<string, string> &data
){
    int fd=open (path.c_str(),O_WRONLY|O_CREAT|O_TRUNC,0644);
    if(fd<0)return Status::Error("SEGMENT_OPEN_FAILED");

    for(const auto&[key,value]:data){
        uint32_t klen=key.size();
        uint32_t vlen=value.size();

        vector<char>buf(sizeof(klen)+sizeof(vlen)+klen+vlen);
        size_t off=0;

        memcpy(buf.data()+off,&klen,sizeof(klen));off+=sizeof(klen);
        memcpy(buf.data()+off,&vlen,sizeof(vlen));off+=sizeof(vlen);
        memcpy(buf.data()+off,key.data(),klen);off+=klen;
        memcpy(buf.data()+off,value.data(),vlen);

        uint32_t crc=crc32(
            0,
            reinterpret_cast<const Bytef*>(buf.data()),
            buf.size()
        );
        if(
            write(fd,&crc,sizeof(crc))!=sizeof(crc) ||
            write(fd,buf.data(),buf.size())!= (ssize_t)(buf.size())

        ){
            close(fd);
            return Status::Error("SEGMENT_WRITE_FAILED");
        }
    }
    fsync(fd);
    close(fd);
    return Status::OK();
}

Status read_segment(
    const string &path,
    unordered_map<string, string> &out
){
    int fd=open(path.c_str(),O_RDONLY);
    if(fd<0)return Status::Error("SEGMENT_OPEN_FAILED");

    while(true){
        uint32_t stored_crc;
        if(read(fd, &stored_crc, sizeof(stored_crc)) != sizeof(stored_crc))break;

        uint32_t klen,vlen;

        if(read(fd,&klen,sizeof(klen))!=sizeof(klen))break;
        if(read(fd,&vlen,sizeof(vlen))!=sizeof(vlen))break;

        vector<char>buf(sizeof(klen)+sizeof(vlen)+klen+vlen);
        size_t off=0;
        memcpy(buf.data()+off,&klen,sizeof(klen));off+=sizeof(klen);
        memcpy(buf.data()+off,&vlen,sizeof(vlen));off+=sizeof(vlen);

        if(read(fd,buf.data()+off,klen+vlen)!=(ssize_t)(klen+vlen))break;

        uint32_t calc_crc=crc32(
            0,
            reinterpret_cast<const Bytef*>(buf.data()),
            buf.size()
        );

        if(calc_crc!=stored_crc){
            break;
        }

        string key(buf.data()+sizeof(klen)+sizeof(vlen),klen);
        string val(buf.data()+sizeof(klen)+sizeof(vlen)+klen,vlen);

        out[key]=val;
    }
    close(fd);
    return Status::OK();

}
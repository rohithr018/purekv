#include "segment.h"
#include <fcntl.h>
#include <unistd.h>
#include <cstring>
#include <cstdint>


using namespace std;

/*
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

        if(
            !write_all(fd,&klen,sizeof(klen)).ok() ||
            !write_all(fd,key.data(),klen).ok() ||
            !write_all(fd,&vlen,sizeof(vlen)).ok() ||
            !write_all(fd,value.data(),vlen).ok()

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
        uint32_t klen,vlen;

        if(read(fd,&klen,sizeof(klen))!=sizeof(klen))break;
        if(read(fd,&vlen,sizeof(vlen))!=sizeof(vlen))break;

        string key(klen,'\0');
        string val(vlen,'\0');

        if(read(fd,key.data(),klen) != (ssize_t)(klen))break;
        if(read(fd,val.data(),vlen) != (ssize_t)(vlen))break;

        out[key]=val;
    }
    close(fd);
    return Status::OK();

}
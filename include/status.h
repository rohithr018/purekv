#pragma once

#include <string>
using namespace std;

class Status{
    private:
        bool ok_;
        string msg_;

    public:
        Status(bool ok = true, string msg = "") : ok_(ok), msg_(msg) {}
        static Status OK(){
            return Status(true);
        }
        static Status Error(const string &msg){
            return Status(false, msg);
        }
        bool ok() const {
            return ok_;
        }
        string msg() const { 
            return msg_; 
        }
};

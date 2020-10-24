#ifndef ORIGINAL_INTERFACE_H_
#define ORIGINAL_INTERFACE_H_

#ifndef STH_CONST_H_
#include "sth_const.h"
#endif

#include <iostream>
#include <mutex>
#include <memory>
#include <string.h>

#define pr_info_and_exit(str) do { \
    std::cout << str << ", FILE: " << __FILE__ << \
        ", LINE: " << __LINE__ << std::endl; \
    exit(-1); \
}while(0);

// global logical clock
volatile unsigned long long global_clock = 1;
/*
 * All ptm objects should inherit this abstract class.
 */
class PtmObjectInterface {
public:
    unsigned long long ts_;  // timestamp.
public:
    // refers: https://www.cnblogs.com/albizzia/p/8979078.html
    PtmObjectInterface() {
        ts_ = 0;
    }
    virtual ~PtmObjectInterface() {};
    virtual PtmObjectInterface *Clone() = 0;
    virtual void Copy(PtmObjectInterface *object) = 0;
};

class PtmObjectWrapperInterface {
public:
    virtual bool Validate(unsigned long long ts) = 0;
    virtual void CommitWrite(unsigned long long ts, PtmObjectInterface *write_object) = 0;
    virtual bool Lock() = 0;
    virtual void Unlock() = 0;
    virtual ~PtmObjectWrapperInterface() {};
};

class SthTxInterface {
public:
    unsigned long long read_ts_;
public:
    SthTxInterface() {
        read_ts_ = 0;
    }
    ~SthTxInterface() { };
public:
    // if object's timestamp is less or equal to end_[object's tid]
    // return true; else return false. 
    bool CmpClocks(unsigned long long ts) {
        if (ts <= read_ts_) {
            return true;
        } else {
            return false;
        }
    }
};

#endif
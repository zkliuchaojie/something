#ifndef ORDO_INTERFACE_H_
#define ORDO_INTERFACE_H_

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
const unsigned long long ordo_boundary = 40;
static inline unsigned long ReadTSC(void) {
    unsigned long var;
    unsigned int hi, lo;
    asm volatile("rdtsc":"=a"(lo),"=d"(hi));
    var = ((unsigned long long int) hi << 32) | lo;
    return var;
}

unsigned long long get_time() {
    return ReadTSC();
}

bool cmp_time(unsigned long long t1, unsigned long long t2) {
    if (t1 > t2 + ordo_boundary)
        return true;
    return false;
}

unsigned long long new_time(unsigned long long t) {
    unsigned long long new_time;
    while(cmp_time(new_time=get_time(), t) == false)
        ;
    return new_time;
}

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
        if (cmp_time(read_ts_, ts)) {
            return true;
        } else {
            return false;
        }
    }
};

#endif
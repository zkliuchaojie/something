#ifndef INTSET_H_
#define INTSET_H_

#include <pthread.h>

typedef unsigned long long Value_t;

class AbstractIntset {
public:
    // if val exists, return true, or false
    virtual bool Search(Value_t val) = 0;
    virtual void Insert(Value_t val) = 0;
    // if val exists and is deleted successfully, return ture
    virtual bool Delete(Value_t val) = 0;
    virtual unsigned long long Size() = 0;
    virtual ~AbstractIntset() {};
};


#endif
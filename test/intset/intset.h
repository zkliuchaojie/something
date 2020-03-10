#ifndef INTSET_H_
#define INTSET_H_

#include <pthread.h>
#include <stdlib.h>

typedef unsigned long long Key_t;
typedef unsigned long long Value_t;

const Key_t SENTINEL = -2; // 11111...110
const Key_t INVALID = -1; // 11111...111

const Value_t NONE = 0x0;

struct Pair {
  Key_t key;
  Value_t value;

  Pair(void)
  : key{INVALID} { }

  Pair(Key_t _key, Value_t _value)
  : key{_key}, value{_value} { }

  // Pair& operator=(const Pair& other) {
  //   key = other.key;
  //   value = other.value;
  // }

  void* operator new(size_t size) {
    void *ret;
    posix_memalign(&ret, 64, size);
    return ret;
  }

  void* operator new[](size_t size) {
    void *ret;
    posix_memalign(&ret, 64, size);
    return ret;
  }
};

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
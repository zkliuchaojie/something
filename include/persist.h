#ifndef PERSIST_H_
#define PERSIST_H_

#include <cstdlib>
#include <stdint.h>

#ifndef STH_CONST_H_
#include "sth_const.h"
#endif

inline void mfence(void) {
    asm volatile("mfence":::"memory");
}

inline void sfence(void) {
    asm volatile("sfence":::"memory");
}

inline void lfence(void) {
    asm volatile("lfence":::"memory");
}

inline void clflush(char* data, size_t len) {
    volatile char *ptr = (char*)((unsigned long)data & (~(kCacheLineSize-1)));
    mfence();
    for (; ptr < data+len; ptr += kCacheLineSize) {
        asm volatile("clflush %0" : "+m" (*(volatile char*)ptr));
    }
    mfence();
}

#endif  // PERSIST_H_

#ifndef PERSIST_H_
#define PERSIST_H_

#include <cstdlib>
#include <stdint.h>

inline void mfence(void) {
    asm volatile("mfence":::"memory");
}

inline void sfence(void) {
    asm volatile("sfence":::"memory");
}

inline void lfence(void) {
    asm volatile("lfence":::"memory");
}

#endif  // PERSIST_H_

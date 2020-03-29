#ifndef STH_ATOMIC_H_
#define STH_ATOMIC_H_

// refers: https://gcc.gnu.org/onlinedocs/gcc/_005f_005fatomic-Builtins.html
#define ATOMIC_LOAD(ptr)            __atomic_load_n (ptr, __ATOMIC_RELAXED)
#define ATOMIC_STORE(ptr, val)      __atomic_store_n (ptr, val, __ATOMIC_RELAXED)
#define ATOMIC_FETCH_ADD(ptr, val)  __atomic_fetch_add (ptr, val, __ATOMIC_RELAXED)
#define ATOMIC_SUB_FETCH(ptr, val)  __atomic_sub_fetch (ptr, val, __ATOMIC_RELAXED)
#define CAS(_p, _u, _v)  (__atomic_compare_exchange_n \
    (_p, _u, _v, false, __ATOMIC_ACQUIRE, __ATOMIC_ACQUIRE))

#endif
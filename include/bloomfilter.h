#ifndef BLOOM_FILTER_H_
#define BLOOM_FILTER_H_

#ifndef STH_CONST_H_
#include "sth_const.h"
#endif

#ifndef HASH_H_
#include "hash.h"
#endif

#include <string.h>

class alignas(kCacheLineSize) BloomFilter {
public:
    BloomFilter() {memset(bloom_, 0, kBloomFilterSize);};
    ~BloomFilter() {};
    void Clear() {
        memset(bloom_, 0, kBloomFilterSize);
    }
    bool MightContain(void *addr) {
        size_t pos1 = h1(addr, sizeof(addr)) % (kBloomFilterSize * 8);
        if (!(bloom_[pos1/8] & (1<<(pos1%8))))
            return false;
        size_t pos2 = h2(addr, sizeof(addr)) % (kBloomFilterSize * 8);
        if (!(bloom_[pos2/8] & (1<<(pos2%8))))
            return false;
        return true;
    }
    void Put(void *addr) {
        size_t pos1 = h1(addr, sizeof(addr)) % (kBloomFilterSize * 8);
        bloom_[pos1/8] |= 1<<(pos1%8);
        size_t pos2 = h2(addr, sizeof(addr)) % (kBloomFilterSize * 8);
        bloom_[pos2/8] |= 1<<(pos2%8);
    }
private:
    char bloom_[kBloomFilterSize];
};

#endif
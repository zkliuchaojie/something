#ifndef STH_CONST_H_
#define STH_CONST_H_

const int kMaxThreadNum = 8;
// so the max kkMaxThreadNum is 256, but now we use 64
const int kThreadIdBitSize = 8;
const unsigned long long kRwSetDefaultSize = 4096;
const int kCacheLineSize = 64;
const int kOldVersionsDefaultSize = 2;
const int kMaxThreadCounter = 16;
const int kBloomFilterSize = 64;

#endif
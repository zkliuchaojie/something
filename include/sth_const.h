#ifndef STH_CONST_H_
#define STH_CONST_H_

#include <climits>

const int kMaxThreadNum = 128;
// so the max kMaxThreadNum is 256, but now we use 64
const int kThreadIdBitSize = 8;
const unsigned long long kRwSetDefaultSize = 4096;
const int kCacheLineSize = 64;
const int kMaxThreadCounter = 16;
const int kBloomFilterSize = 64;
const int kVersionSize = 3;
const unsigned long long kInvalidTiAndTs = ULLONG_MAX;

#endif
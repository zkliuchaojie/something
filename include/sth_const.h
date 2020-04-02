#ifndef STH_CONST_H_
#define STH_CONST_H_

#include <climits>

const int kMaxThreadNum = 16;
// so the max kkMaxThreadNum is 256, but now we use 64
const int kThreadIdBitSize = 8;
const unsigned long long kRwSetDefaultSize = 4096;
const int kCacheLineSize = 64;
const int kMaxThreadCounter = 16;
const int kBloomFilterSize = 64;
const int kVersionSize = 3;
const unsigned long long kInvalidTiAndTs = ULLONG_MAX;
// the number of threads in a Cluster.
const int kClusterSize = 2;
// the number of Cluster.
const int kClusterNum = 16;

#endif
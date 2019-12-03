#include <limits.h>
#include "gtest/gtest.h"

#ifndef BLOOM_FILTER_H_
#include "bloomfilter.h"
#endif

#include <thread>

TEST(BloomFilter, Simple_Test) {
    BloomFilter bloom_filter;
    unsigned long int a[2] = {140737066581888, 140737066543744}; 
    bloom_filter.Put(a+0);
    ASSERT_EQ(bloom_filter.MightContain(a+0), true);
    ASSERT_EQ(bloom_filter.MightContain(a+1), false);
}
#ifndef STH_INTERFACE_H_
#define STH_INTERFACE_H_

#ifndef STH_CONST_H_
#include "sth_const.h"
#endif

#include <iostream>
#include <mutex>
#include <memory>
#include <string.h>

#define pr_info_and_exit(str) do { \
    std::cout << str << ", FILE: " << __FILE__ << \
        ", LINE: " << __LINE__ << std::endl; \
    exit(-1); \
}while(0);

// thread id and timestamp things
// NOTE: we call the clock when modifying an object as timestamp.
#define TI_AND_TS(ti, ts)   (((unsigned long long)ti<<(64-kThreadIdBitSize)) | (ts))
#define TS(ti_and_ts)       ((ti_and_ts << kThreadIdBitSize) >> kThreadIdBitSize)
#define TI(ti_and_ts)       (ti_and_ts >> (64-kThreadIdBitSize))
#define MAX(v1, v2)         (v1>v2 ? v1 : v2)
#define MIN(v1, v2)         (v1>v2 ? v2 : v1)

struct ThreadIdMapEntry {
    bool                is_busy_;
    unsigned long long  thread_clock_;
};

class ThreadIdAllocator {
public:
    // 0 means we do not find a suitable thread id.
    unsigned long long GetThreadIdAndClock() {
        std::lock_guard<std::mutex> l(mutex);
        for (int i=0; i<kMaxThreadNum; i++) {
            if (thread_id_map_[i].is_busy_ == false) {
                thread_id_map_[i].is_busy_ = true;
                return TI_AND_TS(i, thread_id_map_[i].thread_clock_);
            }
        }
        return 0;
    }
    void PutThreadIdAndClock(unsigned long long ti_and_ts) {
        std::lock_guard<std::mutex> l(mutex);
        int thread_id = TI(ti_and_ts);
        if (thread_id >=0 && thread_id < kMaxThreadNum && \
            thread_id_map_[thread_id].is_busy_ == true) {
            thread_id_map_[thread_id].is_busy_ = false;
            thread_id_map_[thread_id].thread_clock_ = TS(ti_and_ts);
        } else {
            pr_info_and_exit("thread id is not legical");
        }
    }

public:
    static std::shared_ptr<ThreadIdAllocator> NewThreadIdAllocatorInstance() {
        std::lock_guard<std::mutex> l(mutex);
        if (thread_id_allocator == nullptr) {
            thread_id_allocator = std::shared_ptr<ThreadIdAllocator>(new ThreadIdAllocator());
        }
        return thread_id_allocator;
    }

private:
    ThreadIdAllocator() {
        for(int i=0; i<kMaxThreadNum; i++) {
           thread_id_map_[i].is_busy_ = false;   // false means the thread id i is not used.
           thread_id_map_[i].thread_clock_ = 1;
        }
    }
    ThreadIdAllocator(ThreadIdAllocator&) = delete;
    ThreadIdAllocator& operator=(const ThreadIdAllocator)=delete;
    static std::shared_ptr<ThreadIdAllocator> thread_id_allocator;
    static std::mutex mutex;
    ThreadIdMapEntry thread_id_map_[kMaxThreadNum];
};

std::shared_ptr<ThreadIdAllocator> ThreadIdAllocator::thread_id_allocator = nullptr;
std::mutex ThreadIdAllocator::mutex;

/*
 * All ptm objects should inherit this abstract class.
 */
class PtmObjectInterface {
public:
    unsigned long long ti_and_ts_;  // thread id and timestamp.
public:
    // refers: https://www.cnblogs.com/albizzia/p/8979078.html
    PtmObjectInterface() {
        ti_and_ts_ = TI_AND_TS(0, 0);
    }
    virtual ~PtmObjectInterface() {};
    virtual PtmObjectInterface *Clone() = 0;
    virtual void Copy(PtmObjectInterface *object) = 0;
};

class PtmObjectWrapperInterface {
public:
    virtual bool Validate(unsigned long long ti_and_ts) = 0;
    virtual void CommitWrite(unsigned long long ti_and_ts, PtmObjectInterface *write_object) = 0;
    virtual bool Lock() = 0;
    virtual void Unlock() = 0;
    virtual ~PtmObjectWrapperInterface() {};
};

class SthTxInterface {
public:
    int                 thread_id_;
    unsigned long long  thread_clock_;
    unsigned long long  ends_[kMaxThreadNum];
    unsigned long long  latest_[kMaxThreadNum];

    SthTxInterface() {
        unsigned long long ti_and_ts = ThreadIdAllocator::NewThreadIdAllocatorInstance()->GetThreadIdAndClock();
        if (ti_and_ts == 0) {
            pr_info_and_exit("can not get thread id");
        }
        thread_id_ = TI(ti_and_ts);
        thread_clock_ = TS(ti_and_ts);
        memset(ends_, 0, sizeof(unsigned long long)*kMaxThreadNum);
        memset(latest_, 0, sizeof(unsigned long long)*kMaxThreadNum);
    }
    ~SthTxInterface() {
        if (thread_id_ != -1 )
            ThreadIdAllocator::NewThreadIdAllocatorInstance()->PutThreadIdAndClock(TI_AND_TS(thread_id_, thread_clock_));
    }
public:
    // if object's timestamp is less or equal to end_[object's tid]
    // return true; else return false. 
    bool CmpClocks(unsigned long long ti_and_ts) {
        if (TS(ti_and_ts) <= ends_[TI(ti_and_ts)]) {
            return true;
        } else {
            if (TS(ti_and_ts) > latest_[TI(ti_and_ts)])
                latest_[TI(ti_and_ts)] = TS(ti_and_ts);
            return false;
        }
    }
    void UpdateEnds(unsigned long long ti_and_ts) {
        ends_[TI(ti_and_ts)] = TS(ti_and_ts);
    }
};

#endif
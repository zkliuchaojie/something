/*
 * We do not support nested transaction for now.
 */

#ifndef STH_REDO_H_
#define STH_REDO_H_ 

#ifndef STH_ATOMIC_H_
#include "sth_atomic.h"
#endif

#ifndef PERSIST_H_
#include "persist.h"
#endif

#ifndef STH_CONST_H_
#include "sth_const.h"
#endif

#define USE_AEP

#ifdef USE_AEP
#include "aep.h"
#endif

#include <vector>
#include <iostream>
#include <setjmp.h>
#include <mutex>
#include <cstring>
#include <algorithm>
#include <shared_mutex>

#define PTM_THREAD_CLEAN do { \
    ; \
}while(0);

// timestamp things
#define TI_AND_TS(ti, ts)   (((unsigned long long)ti<<(sizeof(unsigned long long)*8-kThreadIdBitSize)) | (ts))
#define TS(ti_and_ts)       ((ti_and_ts << kThreadIdBitSize) >> kThreadIdBitSize)
#define TI(ti_and_ts)       (ti_and_ts >> (sizeof(unsigned long long)*8-kThreadIdBitSize))
#define MAX(v1, v2)         (v1>v2 ? v1 : v2)
#define MIN(v1, v2)         (v1>v2 ? v2 : v1)

#define pr_info_and_exit(str) do { \
    std::cout << str << ", FILE: " << __FILE__ << \
        ", LINE: " << __LINE__ << std::endl; \
    exit(-1); \
}while(0);

#define PTM_START(tx_mode) do { \
    sigjmp_buf *_e = sth_ptm_start(tx_mode); \
    if (_e != NULL) { \
        /* save the current signal mask */ \
        sigsetjmp(*_e, 1); \
    } \
}while(0);

#define PTM_COMMIT do { \
    sth_ptm_commit(); \
}while(0);

class Transaction;

enum TransactionStatus {
    ACTIVE = 0,
    COMMITTED, 
    ABORTED,
    UNDETERMINED,
};

enum TransactionMode {
    RDONLY = 0x1,
    RDWR   = 0x2,
};

enum OpenMode {
    READ = 0,
    WRITE,
};

template <typename T>
class PtmObjectWrapper;
class Transaction;
static void sth_ptm_abort();
class RwSet;

#define INF ((unsigned long long)(-1L))

/*
 * All ptm objects should inherit this abstract class.
 */
class AbstractPtmObject {
public:
    // refers: https://www.cnblogs.com/albizzia/p/8979078.html
    virtual ~AbstractPtmObject() {};
    virtual AbstractPtmObject *Clone() = 0;
    // copy ptm_object to this
    virtual void Copy(AbstractPtmObject *ptm_object) = 0;
};

class AbstractPtmObjectWrapper {
public: 
    virtual bool Validate(unsigned long long version_num) = 0;
    virtual void CommitWrite(unsigned long long commit_timestamp, AbstractPtmObject *object) = 0;
    virtual void Unlock() = 0;
    virtual ~AbstractPtmObjectWrapper() {};
};

struct PtmObjectWrapperWithVersionNum {
    AbstractPtmObjectWrapper *ptm_object_wrapper_;
    AbstractPtmObject *object_;
    unsigned long long ti_and_ts_;
};

class RwSet {
public:
    PtmObjectWrapperWithVersionNum *set_;
    unsigned long long entries_num_;
    unsigned long long capacity_;
    RwSet() {
        entries_num_ = 0;
        capacity_ = kRwSetDefaultSize;
        set_ = (PtmObjectWrapperWithVersionNum *)malloc( \
            sizeof(PtmObjectWrapperWithVersionNum) * capacity_);
    }
    ~RwSet() {
        delete set_;
    }
    unsigned long long GetEntriesNum() {
        return entries_num_;
    }
    void Clear() {
        entries_num_ = 0;
    }
    void Push(AbstractPtmObjectWrapper *ptm_object_wrapper, unsigned long long ti_and_ts, AbstractPtmObject *object) {
        // TODO: realloc
        set_[entries_num_].ptm_object_wrapper_ = ptm_object_wrapper;
        set_[entries_num_].ti_and_ts_ = ti_and_ts;
        set_[entries_num_].object_ = object;

        entries_num_++;
        if(entries_num_ > capacity_)
            std::cout << "overflow" << std::endl;
    }
    AbstractPtmObject* GetWrtieObjectBy(AbstractPtmObjectWrapper *ptm_object_wrapper) {
        for(int i=0; i<entries_num_; i++) {
            if(set_[i].ptm_object_wrapper_ == ptm_object_wrapper)
                return set_[i].object_;
        }
        return nullptr;
    }
    bool Validate() {
        for (int i=0; i<entries_num_; i++) {
            if (set_[i].ptm_object_wrapper_->Validate(set_[i].ti_and_ts_) == false)
                return false;
        }
        return true;
    }
    void CommitWrites(unsigned long long ti_and_ts) {
        for (int i=0; i<entries_num_; i++) {
            set_[i].ptm_object_wrapper_->CommitWrite(ti_and_ts, set_[i].object_);
        }
    }
    void Persist() {
	//std::cout << entries_num_ << std::endl;
	for (int i=0; i<entries_num_; i++) {
        char *persist_region = (char *)vmem_malloc(64);
        memcpy(persist_region, set_[i].object_, 64);
	    clflush(persist_region, 64);
        }
    }
    void Unlock() {
        for (int i=0; i<entries_num_; i++) {
            set_[i].ptm_object_wrapper_->Unlock(); 
        }
    }
};

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

class Transaction  {
public:
    /*
     * ends_ are used to record all threads' current time, and all
     * objects' commit time should be less or equal it.
     */
    int                 thread_id_;
    unsigned long long  thread_clock_;
    // persisted_thread_clock_ is used to recovery
    unsigned long long  persisted_thread_clock_;
    unsigned long long  ends_[kMaxThreadNum];
    unsigned long long  latest_[kMaxThreadNum];
    RwSet *r_set_;
    RwSet *w_set_;
    sigjmp_buf *env_;
    TransactionMode mode_;

public:
    Transaction() {
        unsigned long long ti_and_ts = ThreadIdAllocator::NewThreadIdAllocatorInstance()->GetThreadIdAndClock();
        if (ti_and_ts == 0) {
            pr_info_and_exit("can not get thread id");
        }
        thread_id_ = TI(ti_and_ts);
        thread_clock_ = TS(ti_and_ts);

        memset(ends_, 0, sizeof(unsigned long long)*kMaxThreadNum);
        memset(latest_, 0, sizeof(unsigned long long)*kMaxThreadNum);

        r_set_ = new RwSet();
        w_set_ = new RwSet();
        env_ = (sigjmp_buf *)malloc(sizeof(sigjmp_buf));
    }
 
    ~Transaction() {
        if (thread_id_ != -1 )
            ThreadIdAllocator::NewThreadIdAllocatorInstance()->PutThreadIdAndClock(TI_AND_TS(thread_id_, thread_clock_));
        if (r_set_ != nullptr)
            delete r_set_;
        if (w_set_ != nullptr)
            delete w_set_;
        if (env_ != nullptr)
            free(env_);
    }
public:
    // if object's timestamp is less or equal to end_[object's tid]
    // return true; else return false.
    bool CmpClocks(unsigned long long ti_and_ts, bool update_latest) {
        if (TS(ti_and_ts) <= ends_[TI(ti_and_ts)]) {
            return true;
        } else {
            if ((update_latest == true) && TS(ti_and_ts) > latest_[TI(ti_and_ts)]) {
                // std::cout << "update: " << TS(ti_and_ts) << std::endl;
                latest_[TI(ti_and_ts)] = TS(ti_and_ts);
            }
            return false;
        }
    }
    void UpdateEnds(unsigned long long ti_and_ts) {
        ends_[TI(ti_and_ts)] = TS(ti_and_ts);
    }

};

// thread things
thread_local Transaction thread_tx;
thread_local unsigned long long thread_read_abort_counter = 0;
thread_local unsigned long long thread_abort_counter = 0;

/*
 * ptm object wrapper
 */
template <typename T>
class alignas(kCacheLineSize) PtmObjectWrapper : public AbstractPtmObjectWrapper {
public:
    PtmObjectWrapper() {
        ti_and_ts_ = kInvalidTiAndTs;
        lock_ = -1;
    }

    T *Open(OpenMode mode) {
        if (mode == READ) {
            return OpenWithRead();
        } else if (mode == WRITE) {
            return OpenWithWrite();
        }
    }

    bool Validate(unsigned long long ti_and_ts) {
        // we do not need to check lock != -1, since in OpenWithRead func,
        // we have ensured that the object is consistent.
        if (ti_and_ts_ != ti_and_ts)
            return false;
        return true;
    }
    void CommitWrite(unsigned long long ti_and_ts, AbstractPtmObject *object) {
        object_.Copy(object);
        ti_and_ts_ = ti_and_ts;
        clflush((char *)this, sizeof(*this));
    }
    void Unlock() {
        // lock_ = -1 is enough
        lock_ = -1;
    }
private:
    volatile int lock_;  // -1 means not locked, 0~kVersionSize-1 means locked
    volatile unsigned long long ti_and_ts_;
    T object_;
    // std::mutex mutex_;s
    /*
     * when OpenWithRead, it may be a Update Tx, but we don't know
     * until OpenWithWrite. So we will use old objects, and there is
     * a bug. For example, for a linkedlist, when deleting an object,
     * you search it first. In this process, if you use old objects,
     * you may can't find it, then the delete operation failed. So we
     * need users tell us whether it is a update transaction.
     */
    T* OpenWithRead() {
retry:
        unsigned long long curr_ti_and_ts = ti_and_ts_;
        if (curr_ti_and_ts == kInvalidTiAndTs) {
            std::cout << "should not go here" << std::endl;
        }
        if (thread_tx.CmpClocks(curr_ti_and_ts, lock_ == -1) == true) {
            T *ret;
            ret = (T *)thread_tx.w_set_->GetWrtieObjectBy(this);
            if (ret != nullptr)
                return ret;
            ret = (T *)object_.Clone();
            // write_pos != pos_ is to ensure that ret is the latest version.
            if (lock_ != -1 || ti_and_ts_ != curr_ti_and_ts) {
                goto retry;
            }
            if (thread_tx.mode_ == RDWR) {
                thread_tx.r_set_->Push(this, curr_ti_and_ts, ret);
            }
            return ret;
        } else {
            // RDONLY transaction can't directly use extending.
            // For example, tx first reads A''', then when reads B,
            // it extends its ends. Even through A is not modified,
            // tx may read B'', which is updated with A'' in the same
            // transaction.
            if (lock_ == -1 && thread_tx.r_set_->Validate()) {
                thread_tx.UpdateEnds(curr_ti_and_ts);
                goto retry;
            }
            thread_read_abort_counter++;
            sth_ptm_abort();
        }
    }

    T *OpenWithWrite() {
retry:
        unsigned long long curr_ti_and_ts = ti_and_ts_;
        // NOTE: CmpClocks will modify latest_, which is always right.
        // Because for a timestamp, there are two situation: the corresponding
        // tx is committed or not. If committed, it is ok. If not, it means
        // the tx will succeed, since we have validated read set and got locks,
        // and we can only access objects modified in the tx until the tx release
        // its locks. If abort, we will start and initialize latest with zero, so
        // it is ok.

        // first condition is a special case: PtmObject is not initialized.
        if (curr_ti_and_ts == kInvalidTiAndTs ||
            thread_tx.CmpClocks(curr_ti_and_ts, lock_ == -1) == true) {
            T *ret;
            ret = (T *)thread_tx.w_set_->GetWrtieObjectBy(this);
            if (ret != nullptr) {
                return ret;
            }
            int tmp = -1;
            if (CAS(&lock_, &tmp, 1) == true) {
                // the first condition is a must, since other tx may modify it and commit.
                // the second condition is a must, since there may be a circle.
                if (curr_ti_and_ts != ti_and_ts_) {
                    lock_ = -1;
                    goto retry;
                } else {
                    ret = (T *)object_.Clone();
                    thread_tx.w_set_->Push(this, curr_ti_and_ts, ret);
                    return ret;
                }
            } else {
                sth_ptm_abort();
            }
        } else {
            // The condition of lock_ == -1 is a must, because
            // even though RDWR tx won't read unconsistent data,
            // next RDONLY tx may use those timestamps, leading
            // to error.
            if (lock_ == -1 && thread_tx.r_set_->Validate()) {
                thread_tx.UpdateEnds(curr_ti_and_ts);
                goto retry;
            } else {
                sth_ptm_abort();
            }
        }
    }
};

void InitTransaction() {
    thread_tx.r_set_->Clear();
    thread_tx.w_set_->Clear();
     for(int i=0; i<kMaxThreadNum; i++) {
        thread_tx.ends_[i] = thread_tx.latest_[i];
    }
}

static jmp_buf *sth_ptm_start(TransactionMode mode) {
    thread_tx.mode_ = mode;
    InitTransaction();
    return thread_tx.env_;
}

static void sth_ptm_commit() {
    if (thread_tx.mode_ == RDWR) {
        if (thread_tx.r_set_->Validate() == false) {
            sth_ptm_abort();
        }
        unsigned long long commit_ts = thread_tx.thread_clock_ + 1;
        // Why shouldn't we release locks when commit writes?  
        // If so, another tx may read the timestamp from the committed
        // objects, but there are also some objects that are not committed.
        // Then it may read unconsistent data. For example, it can read the
        // committed objects, but can not read uncommitted objects. And if
        // we release locks after committing writes, other tx can not get
        // the latest timestamp until committing writes are finished.
	thread_tx.w_set_->Persist();
	mfence();
        thread_tx.w_set_->CommitWrites(TI_AND_TS(thread_tx.thread_id_, commit_ts));
        mfence();
        thread_tx.w_set_->Unlock();
        // we need a mfence here.
        mfence();
        thread_tx.persisted_thread_clock_ = commit_ts;
        clflush((char *)(&(thread_tx.persisted_thread_clock_)), sizeof(unsigned long long));
        mfence();
        thread_tx.thread_clock_ = commit_ts;
    }
}

void sth_ptm_abort() {
    thread_tx.w_set_->Unlock();
    InitTransaction();
    // std::cout << "abort" << std::endl;
    thread_abort_counter++;
    siglongjmp(*(thread_tx.env_), 1);
}

#endif

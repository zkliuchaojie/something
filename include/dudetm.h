/*
 * dudetm is implemented with PL2 not tinySTM.
 */

#ifndef DUDETM_H_
#define DUDETM_H_

#ifndef STH_ATOMIC_H_
#include "sth_atomic.h"
#endif

#ifndef PERSIST_H_
#include "persist.h"
#endif

#ifndef STH_CONST_H_
#include "sth_const.h"
#endif

#ifndef THREAD_SAFE_QUEUE_H_
#include "thread_safe_queue.h"
#endif

#define USE_AEP

#ifdef USE_AEP
#include "aep.h"
#endif

// #define ASYNC_DUDETM

#include <vector>
#include <iostream>
#include <setjmp.h>
#include <mutex>
#include <cstring>
#include <algorithm>
#include <shared_mutex>
#include <mutex>
#include <condition_variable>
#include <queue>
#include <thread>

#define PTM_THREAD_CLEAN do { \
    ; \
}while(0);

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

#define INF ((unsigned long long)(-1L))

/*
 * All ptm objects should inherit this abstract class.
 */
class AbstractPtmObject {
public:
    unsigned long long ts_;  // timestamp.
    AbstractPtmObject() { ts_ = 0; }
    // refers: https://www.cnblogs.com/albizzia/p/8979078.html
    virtual ~AbstractPtmObject() {};
    virtual AbstractPtmObject *Clone() = 0;
    // copy ptm_object to this
    virtual void Copy(AbstractPtmObject *ptm_object) = 0;
};

class AbstractPtmObjectWrapper {
public: 
    virtual bool Validate(unsigned long long ts) = 0;
    virtual void CommitWrite(unsigned long long ts, AbstractPtmObject *write_object) = 0;
    virtual bool Lock() = 0;
    virtual void Unlock() = 0;
    virtual ~AbstractPtmObjectWrapper() {};
};

struct ReadSetEntry {
    AbstractPtmObjectWrapper *ptm_object_wrapper_;
    unsigned long long ts_;
};

struct WriteSetEntry {
    AbstractPtmObjectWrapper *ptm_object_wrapper_;
    unsigned long long ts_;
    AbstractPtmObject *write_object_;
};

class ReadSet {
public:
    ReadSetEntry *set_;
    unsigned long long entries_num_;
    unsigned long long capacity_;
    ReadSet() {
        entries_num_ = 0;
        capacity_ = kRwSetDefaultSize;
        set_ = (ReadSetEntry *)malloc(sizeof(ReadSetEntry) * capacity_);
        memset(set_, 0, sizeof(ReadSetEntry) * capacity_);
    }
    ~ReadSet() {
        Clear();
        if (set_ != nullptr)
            delete set_;
    }
    void Clear() {
        entries_num_ = 0;
    }
    void Push(AbstractPtmObjectWrapper *ptm_object_wrapper, unsigned long long ts) {
        // TODO: realloc
        set_[entries_num_].ptm_object_wrapper_ = ptm_object_wrapper;
        set_[entries_num_].ts_ = ts;

        entries_num_++;
        if(entries_num_ > capacity_)
            pr_info_and_exit("overflow")
    }
    bool Validate() {
        for (int i=0; i<entries_num_; i++) {
            if (set_[i].ptm_object_wrapper_->Validate(set_[i].ts_) == false)
                return false;
        }
        return true;
    }
};

class WriteSet {
public:
    WriteSetEntry *set_;
    unsigned long long entries_num_;
    unsigned long long capacity_;
    WriteSet() {
        entries_num_ = 0;
        capacity_ = kRwSetDefaultSize;
        set_ = (WriteSetEntry *)malloc(sizeof(WriteSetEntry) * capacity_);
        memset(set_, 0, sizeof(WriteSetEntry) * capacity_);
    }
    ~WriteSet() {
        Clear();
        if (set_ != nullptr)
            delete set_;
    }
    void Clear() {
        for(int i=0; i<entries_num_; i++) {
            if (set_[i].write_object_ != nullptr)
                delete set_[i].write_object_;
        }
        entries_num_ = 0;
    }
    void Push(AbstractPtmObjectWrapper *ptm_object_wrapper, unsigned long long ts, 
        AbstractPtmObject *write_object) {
        set_[entries_num_].ptm_object_wrapper_ = ptm_object_wrapper;
        set_[entries_num_].ts_ = ts;
        set_[entries_num_].write_object_ = write_object;
        entries_num_++;
        if(entries_num_ > capacity_)
            pr_info_and_exit("overflow");
    }
    AbstractPtmObject* GetWrtieObjectBy(AbstractPtmObjectWrapper *ptm_object_wrapper) {
        for(int i=0; i<entries_num_; i++) {
            if(set_[i].ptm_object_wrapper_ == ptm_object_wrapper)
                return set_[i].write_object_;
        }
        return nullptr;
    }
    bool Validate() {
        for (int i=0; i<entries_num_; i++) {
            if (set_[i].ptm_object_wrapper_->Validate(set_[i].ts_) == false)
                return false;
        }
        return true;
    }
    void CommitWrites(unsigned long long ts) {
        for (int i=0; i<entries_num_; i++) {
            set_[i].ptm_object_wrapper_->CommitWrite(ts, set_[i].write_object_);
        }
    }
    void Lock() {
        for (int i=0; i<entries_num_; i++) {
            if (set_[i].ptm_object_wrapper_->Lock() == false) {
                for(i=i-1; i>=0; i--) {
                    set_[i].ptm_object_wrapper_->Unlock();
                }
                sth_ptm_abort();
            }
        }
    }
    void Unlock() {
        for (int i=0; i<entries_num_; i++) {
            set_[i].ptm_object_wrapper_->Unlock(); 
        }
    }
};

class RedoLog {
public:
    RedoLog () {
        commit_ts_ = -1;
        is_persisted_ = false;
    }
    ~RedoLog () {
        for (int i=0; i<entries_.size(); i++)
            free(entries_[i]);
    }
    void Append(void *addr, int len, char *content) {
        LogEntry *log = (LogEntry *)malloc(sizeof(LogEntry) + len);
        log->addr_ = addr;
        log->len_ = len;
        memcpy(log->content, content, len);
        entries_.push_back(log);
    }
    void Copy(RedoLog *redolog) {
        for (int i=0; i<redolog->entries_.size(); i++) {
            LogEntry *log = (LogEntry *)malloc(sizeof(LogEntry) + redolog->entries_[i]->len_);
            log->addr_ = redolog->entries_[i]->addr_;
            log->len_ = redolog->entries_[i]->len_;
            memcpy(log->content, redolog->entries_[i]->content, redolog->entries_[i]->len_);
            entries_.push_back(log);
        }
    }
    void ApplyLogEntries() {
        while (is_persisted_ == false) {;}
        for (int i=0; i<entries_.size(); i++) {
            memcpy(entries_[i]->addr_, entries_[i]->content, entries_[i]->len_);
            clflush((char *)entries_[i]->addr_, entries_[i]->len_);
        }
    }
public:
    struct LogEntry {
        void *addr_;
        int len_;
        char content[0];
    };
public:
    std::vector<LogEntry *> entries_;
    long long int commit_ts_;
    volatile bool is_persisted_;
};

class CompareRedolog
{
public:
    bool operator () (RedoLog* &a, RedoLog* &b) const
    {
        return a->commit_ts_ > b->commit_ts_;
    }
};

class LogRegion {
public:
    // the defualt size of LogSegment is 16MB
    struct LogSegment {
        unsigned long long pos_;
        char *segment_;
    };
public:
    LogRegion() {
        curr_log_segment_ = new LogSegment();
        curr_log_segment_->segment_ = (char *)vmem_malloc(16*1024*1024);
        curr_log_segment_->pos_ = 0;
        log_segment_vec_.push_back(curr_log_segment_);
    }
    void AllocateNewSegment() {
        curr_log_segment_ = new LogSegment();
        curr_log_segment_->segment_ = (char *)vmem_malloc(16*1024*1024);
        if (!curr_log_segment_->segment_) {
            std::cout << "can not alloc memory" << std::endl;
            exit(-1);
        }
        curr_log_segment_->pos_ = 0;
        log_segment_vec_.push_back(curr_log_segment_);
    }
    void PersistRedolog(RedoLog *redolog) {
retry:
        int start = curr_log_segment_->pos_;
        if (curr_log_segment_->pos_ + sizeof(redolog->commit_ts_) >= 16*1024*1024) {
                AllocateNewSegment();
                goto retry;
        }
        memcpy(curr_log_segment_->segment_+curr_log_segment_->pos_, &redolog->commit_ts_, sizeof(redolog->commit_ts_));
        curr_log_segment_->pos_ += sizeof(redolog->commit_ts_);
        for (int i=0; i<redolog->entries_.size(); i++) {
            if (curr_log_segment_->pos_ + sizeof(void *) + sizeof(int) + redolog->entries_[i]->len_ >= 16*1024*1024) {
                AllocateNewSegment();
                goto retry;
            }
            memcpy(curr_log_segment_->segment_+curr_log_segment_->pos_, redolog->entries_[i], \
                sizeof(void *) + sizeof(int) + redolog->entries_[i]->len_);
            curr_log_segment_->pos_ += sizeof(void *) + sizeof(int) + redolog->entries_[i]->len_;
        }
        clflush(curr_log_segment_->segment_+start, curr_log_segment_->pos_-start);
        redolog->is_persisted_ = true;
    }
    std::vector<LogSegment* > log_segment_vec_;
    LogSegment *curr_log_segment_;
};

class Transaction {
public:
    unsigned long long  read_ts_;
    TransactionMode     mode_;
    ReadSet             *r_set_;
    WriteSet            *w_set_;
    sigjmp_buf          *env_;
    RedoLog             *vlog_;

public:
    Transaction() {
        read_ts_ = 0;
        r_set_ = new ReadSet();
        w_set_ = new WriteSet();
        env_ = (sigjmp_buf *)malloc(sizeof(sigjmp_buf));
        vlog_ = nullptr;
    }
 
    ~Transaction() {
        if (r_set_ != nullptr)
            delete r_set_;
        if (w_set_ != nullptr)
            delete w_set_;
        if (env_ != nullptr)
            free(env_);
    }
public:
    bool CmpClocks(unsigned long long ts) {
        if (ts <= read_ts_) {
            return true;
        } else {
            return false;
        }
    }
};

// thread things
thread_local Transaction thread_tx;
thread_local unsigned long long thread_read_abort_counter = 0;
thread_local unsigned long long thread_abort_counter = 0;
// log region
thread_local LogRegion log_region;

// global things
// global logical clock
volatile unsigned long long global_clock = 1;
// transaction redo logs, tx_logs is used by persist thread
ThreadSafeQueue<RedoLog *> tx_logs;
std::mutex pq_mutex;
// reproduce timestamp
unsigned long long current_reproduce_timestamp = global_clock;

#ifdef ASYNC_DUDETM
// persist thread stop
volatile bool persist_thread_stop;
#endif
// reproduce thread stop
volatile bool reproduce_thread_stop;
// used by reproduce thread
std::priority_queue<RedoLog*, std::vector<RedoLog*>, CompareRedolog> persisted_redologs;

RedoLog *persisted_redologs_pop() {
    std::unique_lock<std::mutex> mlock(pq_mutex);
    if (persisted_redologs.empty()) {
        return nullptr;
    }
    RedoLog *item = persisted_redologs.top();
    if (item->commit_ts_ == current_reproduce_timestamp) {
        persisted_redologs.pop();
        current_reproduce_timestamp++;
        return item;
    } else {
        return nullptr;
    }
}
void persisted_redologs_push(RedoLog *item) {
    std::unique_lock<std::mutex> mlock(pq_mutex);
    persisted_redologs.push(item);
    mlock.unlock();
}

#ifdef ASYNC_DUDETM
// Persist thread
void persist_thread() {
    persist_thread_stop = false;
    while (true) {
        if (persist_thread_stop == true)
            return;
        RedoLog *redolog = tx_logs.pop();
        if (redolog == nullptr)
            continue;
        log_region.PersistRedolog(redolog);
        persisted_redologs_push(redolog);
    }
}
#endif

// Reproduce thread
void reproduce_thread() {
    reproduce_thread_stop = false;
    while (true) {
        if (reproduce_thread_stop == true)
            return;
        RedoLog *redolog = persisted_redologs_pop();
        if (redolog == nullptr)
            continue;
        redolog->ApplyLogEntries();
        // std::cout << redolog->commit_ts_ << std::endl;
        delete redolog;
    }
}

/*
 * ptm object wrapper
 */
template <typename T>
class alignas(kCacheLineSize) PtmObjectWrapper : public AbstractPtmObjectWrapper {
public:
    PtmObjectWrapper() {
        lock_ = 0;
        dram_object_ = nullptr;
    }

    T *Open(OpenMode mode) {
        if (mode == READ) {
            return OpenWithRead();
        } else if (mode == WRITE) {
            return OpenWithWrite();
        }
        return nullptr;
    }

    bool Validate(unsigned long long ts) {
        if (dram_object_->ts_ != ts)
            return false;
        return true;
    }
    void CommitWrite(unsigned long long ts, AbstractPtmObject *write_object) {
        dram_object_->Copy(write_object);
        dram_object_->ts_ = ts;
        thread_tx.vlog_->Append(&pm_object_, sizeof(T), (char *)write_object);
    }
    bool Lock() {
        int tmp = 0;
        return CAS(&lock_, &tmp, 1);
    }
    void Unlock() {
        // lock_ = 0 is enough
        lock_ = 0;
    }
private:
    int lock_;
    // pm_object_ is in PM
    T pm_object_;
    // dram_object_ is in DRAM
    T *dram_object_;

    // for now, we didn't consider release T*
    T* OpenWithRead() {
retry:
        if (dram_object_ == nullptr) {
            dram_object_ = (T *)pm_object_.Clone();
        }
        unsigned long long curr_ts = dram_object_->ts_;
        if (lock_ != 0)
            sth_ptm_abort();
        if (thread_tx.CmpClocks(curr_ts) == true) {
            T *ret = (T *)dram_object_->Clone();
            if (lock_ != 0 || dram_object_->ts_ != curr_ts)
                goto retry;
            if (thread_tx.mode_ == RDWR) {
                thread_tx.r_set_->Push(this, curr_ts);
            }
            return ret;
        } else {
            sth_ptm_abort(); 
        }
    }

    T *OpenWithWrite() {
retry:
        if (dram_object_ == nullptr) {
            dram_object_ = (T *)pm_object_.Clone();
        }
        unsigned long long curr_ts = dram_object_->ts_;
        if (lock_ != 0)
            sth_ptm_abort();
        if (thread_tx.CmpClocks(curr_ts) == true) {
            T *ret;
            ret = (T *)thread_tx.w_set_->GetWrtieObjectBy(this);
            if (ret != nullptr) {
                return ret;
            }
            ret = (T *)dram_object_->Clone();
            if (lock_ != 0 || dram_object_->ts_ != curr_ts)
                goto retry;
            thread_tx.w_set_->Push(this, curr_ts, ret);
            return ret;
        } else {
            sth_ptm_abort();
        }
    }
};

void InitTransaction() {
    thread_tx.r_set_->Clear();
    thread_tx.w_set_->Clear();
    thread_tx.read_ts_ = global_clock;
    thread_tx.vlog_ = new RedoLog();
}

static jmp_buf *sth_ptm_start(TransactionMode mode) {
#ifdef ASYNC_DUDETM
    static bool have_start_persist_thread = false;
    if (have_start_persist_thread == false) {
        new std::thread(persist_thread);
        have_start_persist_thread = true;
    }
#endif
    static bool have_start_reproduce_thread = false;
    if (have_start_reproduce_thread == false) {
        new std::thread(reproduce_thread);
        have_start_reproduce_thread = true;
    }
    thread_tx.mode_ = mode;
    InitTransaction();
    return thread_tx.env_;
}

static void sth_ptm_commit() {
    if (thread_tx.mode_ == RDWR) {
        thread_tx.w_set_->Lock();
        if (thread_tx.r_set_->Validate() == false ||
            thread_tx.w_set_->Validate() == false) {
            thread_tx.w_set_->Unlock();
            sth_ptm_abort();
        }
        unsigned long long commit_ts = ATOMIC_FETCH_ADD(&global_clock, 1);
        thread_tx.w_set_->CommitWrites(commit_ts);
        thread_tx.vlog_->commit_ts_ = commit_ts;
#ifdef ASYNC_DUDETM
        tx_logs.push(thread_tx.vlog_);
#else
        log_region.PersistRedolog(thread_tx.vlog_);
        persisted_redologs_push(thread_tx.vlog_);
#endif
        thread_tx.w_set_->Unlock();
    }
}

void sth_ptm_abort() {
    thread_abort_counter++;
    InitTransaction();
    siglongjmp(*(thread_tx.env_), 1);
}

#endif
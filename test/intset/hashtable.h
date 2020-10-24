#ifndef INTSET_HASH_TABLE_H_
#define INTSET_HASH_TABLE_H_

// hashtable is implemented with linked lists

#ifndef STH_INTERFACE_TL2_H_
#include "sth_interface_TL2.h"
#endif

// #ifndef ORIGINAL_INTERFACE_TL2_H_
// #include "original_interface_TL2.h"
// #endif

// #ifndef ORDO_INTERFACE_TL2_H_
// #include "ordo_interface_TL2.h"
// #endif

#ifndef INTSET_H_
#include "intset.h"
#endif

#ifndef HASH_H_
#include "hash.h"
#endif

#include <iostream>
/*
 * hash table entry
 */
class Entry: public PtmObjectInterface {
public:
    Key_t key_;
    Value_t val_;
    PtmObjectWrapper<Entry> *next_;
public:
    Entry() : key_(INVALID), val_(0), next_(nullptr) {};
    Entry(Key_t key, Value_t val) : key_(key), val_(val), next_(nullptr) {};
    ~Entry() {};
    PtmObjectInterface *Clone() {
        Entry *entry = new Entry();
        entry->val_ = val_;
        entry->key_ = key_;
        entry->next_ = next_;
        return entry;
    }
    void Copy(PtmObjectInterface *ptm_object) {
        Entry *entry = (Entry *)ptm_object;
        key_ = entry->key_;
        val_ = entry->val_;
        next_ = entry->next_;
    }
};

/* 
 * test with the following command:
 * ./intset-ht -i 100000 -r 1000000 -u 0 -n 1 
 */

class HashTable : public AbstractIntset {
public:
    HashTable() {
        capacity_ = 1000000;     // 10k
        dict_ = new PtmObjectWrapper<Entry>*[capacity_];
        for(int i=0; i<capacity_; i++) {
            PTM_START(RDWR);
            PtmObjectWrapper<Entry> *tmp = new PtmObjectWrapper<Entry>();
            Entry *entry = tmp->Open(WRITE);
            entry->key_ = INVALID;
            entry->next_ = nullptr;
            dict_[i] = tmp;
            PTM_COMMIT;
        }
    }
    ~HashTable() {
        // do not care
    }
    Value_t Get(Key_t key);
    bool Update(Key_t key, Value_t val);
    int Insert(Key_t key, Value_t val);
    int Delete(Key_t key);
    unsigned long long Size();

  private:
    size_t capacity_;
    PtmObjectWrapper<Entry> **dict_;
};

Value_t HashTable::Get(Key_t key) {
    // the default: doesn't contain
    PTM_START(RDONLY);
    Value_t retval=0;
    // std::cout << "get" << std::endl;
    auto key_hash = h(&key, sizeof(key));
    auto loc = key_hash % capacity_;
    // std::cout << loc << std::endl;
    PtmObjectWrapper<Entry> *curr = dict_[loc];
    while (curr != nullptr) {
        Entry *entry = curr->Open(READ);
        if(entry->key_ == key) {
            retval = 1;
            break;
        }else {
            curr = entry->next_;
        }
    }
    PTM_COMMIT;
    return retval;
}

bool HashTable::Update(Key_t key, Value_t val) {
    PTM_START(RDWR);
    bool ret = false;
    auto key_hash = h(&key, sizeof(key));
    auto loc = key_hash % capacity_;
    PtmObjectWrapper<Entry> *curr = dict_[loc];
    Entry *entry = curr->Open(READ);
    if (entry->key_ == key) { 
        Entry *entry = curr->Open(WRITE);
        entry->key_ = val;
        ret = true;
    } else {
        // search linked list
        PtmObjectWrapper<Entry> *prev = curr;
        curr = entry->next_;
        while (curr != nullptr) {
            Entry *entry = curr->Open(READ);
            if(entry->key_ == key) {
                entry = curr->Open(WRITE);
                entry->val_ = val;
                ret = true;
                break;
            }else {
                prev = curr;
                curr = entry->next_;
            }
        }
    }
    PTM_COMMIT;
    return ret;
}

int HashTable::Insert(Key_t key, Value_t val) {
    PTM_START(RDWR);
    // std::cout << "insert" << std::endl;
    auto key_hash = h(&key, sizeof(key));
    auto loc = key_hash % capacity_;
    // std::cout << loc << std::endl;
    PtmObjectWrapper<Entry> *curr = dict_[loc];
    Entry *entry = curr->Open(WRITE);
    if (entry->key_ == INVALID) {
        entry->key_ = key;
        entry->val_ = val;
    }else {
        // we need alloc a new entry
        PtmObjectWrapper<Entry> *new_entry_wrapper = new PtmObjectWrapper<Entry>();
        Entry *new_entry = new_entry_wrapper->Open(WRITE);
        new_entry->key_ = key;
        new_entry->val_ = val;
        new_entry->next_ = entry->next_;
        entry->next_ = new_entry_wrapper;
        // std::cout << "not first" << std::endl;
    }
    PTM_COMMIT;
    return 1;
}

int HashTable::Delete(Key_t key) {
    PTM_START(RDWR);
    bool ret = 0;
    // std::cout << "delete" << std::endl;
    auto key_hash = h(&key, sizeof(key));
    auto loc = key_hash % capacity_;
    PtmObjectWrapper<Entry> *curr = dict_[loc];
    Entry *entry = curr->Open(READ);
    if (entry->key_ == key) { 
        Entry *entry = curr->Open(WRITE);
        entry->key_ = INVALID;
        ret = 1;
    } else {
        // search linked list
        PtmObjectWrapper<Entry> *prev = curr;
        curr = entry->next_;
        while (curr != nullptr) {
            Entry *entry = curr->Open(READ);
            if(entry->key_ == key) {
                Entry *prev_entry = prev->Open(WRITE);
                entry = curr->Open(WRITE);
                // this is a must when adding Update
                if (entry->next_ != nullptr)
                    entry->next_->Open(WRITE);
                prev_entry->next_ = entry->next_;
                ret = 1;
                break;
            }else {
                prev = curr;
                curr = entry->next_;
            }
        }
    }
    PTM_COMMIT;
    return ret;
}

unsigned long long HashTable::Size() {
    PTM_START(RDONLY);
    unsigned long long ret = 0;
    // std::cout << "size" << std::endl;
    for(int i=0; i<capacity_; i++) {
        PtmObjectWrapper<Entry> *curr = dict_[i];
        while(curr != nullptr) {
            Entry *entry = curr->Open(READ);
            if (entry->key_ != INVALID)
                ret++;
            curr = entry->next_;
        }
    }
    PTM_COMMIT;
    return ret;
}

#endif  // LINEAR_HASH_H_

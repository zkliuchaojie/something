#ifndef INTSET_LINKEDLIST_H_
#define INTSET_LINKEDLIST_H_

#ifndef STH_H_
#include "sth.h"
#endif

#ifndef MM_PARTITION_H_
#include "mm_partition.h"
#endif

#ifndef MM_POOL_H_
#include "mm_pool.h"
#endif

#ifndef INTSET_H_
#include "intset.h"
#endif

/*
 * PO means Ptm Object.
 */
class PONode : public AbstractPtmObject {
public:
    Value_t val_;
    PtmObjectWrapper *next_;

public:
    PONode() : val_(0), next_(nullptr) {};
    PONode(Value_t val) : val_(val), next_(nullptr) {};
    PONode(Value_t val, PtmObjectWrapper *next) : val_(val), next_(next) {};
    ~PONode() {};
    AbstractPtmObject *Clone() {
        PONode *po_node = po_pool_->Alloc();
        po_node->val_ = val_;
        po_node->next_ = next_;
        return po_node;
    }
};

class LinkedList : public AbstractIntset {
public:
    // sentinel 哨兵
    PtmObjectWrapper *sentinel_;

public:
    Pool<PONode> *po_node_pool_;

public:
    LinkedList() {
        po_node_pool_ = new Pool<PONode>();
        PTM_START;
        PONode *po_node = po_node_pool_->Alloc();
        sentinel_ = new PtmObjectWrapper(po_node);
        po_node->next_ = sentinel_;
        PTM_COMMIT;
    };
    ~LinkedList() {
        delete po_node_pool_;
    }
    // if val exists, return true, or false
    bool Search(Value_t val);
    void Insert(Value_t val);
    // if val exists and is deleted successfully, return ture
    bool Delete(Value_t val);
    unsigned long long Size();
};

bool LinkedList::Search(Value_t val) {
    PTM_START;
    bool retval = false;
    PONode * po_node = (PONode *)sentinel_->Open(READ);
    // skip the sentinel
    PtmObjectWrapper *po_node_wrapper = po_node->next_;
    while(po_node_wrapper != sentinel_) {
        po_node = (PONode *)po_node_wrapper->Open(READ);
        if(po_node->val_ == val) {
            retval = true;
            break;
        }else {
            po_node_wrapper = po_node->next_;
        }
    }
    PTM_COMMIT;
    return retval;
}

void LinkedList::Insert(Value_t val) {
    PTM_START;
    //std::cout <<"insert: " << val << std::endl;
    PtmObjectWrapper *prev = sentinel_;
    PONode *po_node = (PONode *)sentinel_->Open(READ);
    PtmObjectWrapper *curr = po_node->next_;

    po_node = (PONode *)curr->Open(READ);
    while(curr != sentinel_ && val > po_node->val_) {
        prev = curr;
        curr = po_node->next_;
        po_node = (PONode *)curr->Open(READ);
    }
    curr->Open(WRITE);
    po_node = (PONode *)prev->Open(WRITE);
    PONode *new_po_node = po_node_pool_->Alloc();
    new_po_node->val_ = val;
    new_po_node->next_ = curr;
    po_node->next_ = new PtmObjectWrapper(new_po_node);
    std::cout << "insert finished" << std::endl;
    PTM_COMMIT;
}

bool LinkedList::Delete(Value_t val) {
    PTM_START;
    PONode *tmp;
    bool retval = true;
    PtmObjectWrapper *prev = sentinel_;
    PONode *po_node = (PONode *)sentinel_->Open(READ);
    PtmObjectWrapper *curr = po_node->next_;

    po_node = (PONode *)curr->Open(READ);
    while(curr != sentinel_ && val > po_node->val_) {
        prev = curr;
        curr = po_node->next_;
        po_node = (PONode *)curr->Open(READ);
    }
    if(val == po_node->val_) {
        // NOTE: we do not consider releasing the deleted node.
        curr->Open(WRITE);
        po_node->next_->Open(WRITE);
        tmp = (PONode *)prev->Open(WRITE);
        tmp->next_ = po_node->next_;
        retval = true;
    }else {
        retval = false;
    }
    PTM_COMMIT;
    return retval;
}

unsigned long long LinkedList::Size() {
    PTM_START;
    unsigned long long retval = 0;
    // skip the sentinel
    PtmObjectWrapper *po_node_wrapper = ((PONode *)sentinel_->Open(READ))->next_;
    while(po_node_wrapper != sentinel_) {
        retval++;
        po_node_wrapper = ((PONode *)po_node_wrapper->Open(READ))->next_;
    }
    PTM_COMMIT;
    return retval;
}

#endif
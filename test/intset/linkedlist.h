#ifndef INTSET_LINKEDLIST_H_
#define INTSET_LINKEDLIST_H_

#ifndef STH_H_
#include "sth.h"
#endif

typedef unsigned long long Value_t;

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
        PONode *po_node = new PONode();
        po_node->val_ = val_;
        po_node->next_ = next_;
        return po_node;
    }
};

class LinkedList {
public:
    // sentinel 哨兵
    PtmObjectWrapper *sentinel_;

public:
    LinkedList() {
        PTM_START;
        PONode *po_node = new PONode(0);
        sentinel_ = new PtmObjectWrapper(po_node);
        po_node->next_ = sentinel_;
        PTM_COMMIT;
    };
    ~LinkedList() {};
    // if val exists, return true, or false
    bool Search(Value_t val);
    void Insert(Value_t val);
    // if val exists and is deleted successfully, return ture
    bool Delete(Value_t val);
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
    
    PONode *po_node = (PONode *)sentinel_->Open(READ);
    // skip the sentinel
    PtmObjectWrapper *po_node_wrapper = po_node->next_;
    PtmObjectWrapper *prev, *curr;
    prev = curr = po_node_wrapper;
    po_node = (PONode *)curr->Open(READ);
    while(curr != sentinel_ && val < po_node->val_) {
        prev = curr;
        curr = po_node->next_;
        po_node = (PONode *)curr->Open(READ);
    }
    curr->Open(WRITE);
    po_node = (PONode *)prev->Open(WRITE);
    po_node->next_ = new PtmObjectWrapper(new PONode(val, curr));

    PTM_COMMIT;
}

bool LinkedList::Delete(Value_t val) {
    PTM_START;

    bool retval = true;
    PONode *tmp;
    PONode *po_node = (PONode *)sentinel_->Open(READ);
    // skip the sentinel
    PtmObjectWrapper *po_node_wrapper = po_node->next_;
    PtmObjectWrapper *prev, *curr;
    prev = curr = po_node_wrapper;
    po_node = (PONode *)curr->Open(READ);
    while(curr != sentinel_ && val < po_node->val_) {
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

#endif
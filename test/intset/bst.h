#ifndef INTSET_BST_H_
#define INTSET_BST_H_

// #ifndef STH_STH_H_
// #include "sth.h"
// #endif

// #ifndef DUDETM_H_
// #include "dudetm.h"
// #endif

#ifndef PMDKTX_H_
#include "pmdktx.h"
#endif

#ifndef INTSET_H_
#include "intset.h"
#endif

#include <queue>

class PONode : public AbstractPtmObject {
public:
    Value_t val_;
    PtmObjectWrapper<PONode> *left_;
    PtmObjectWrapper<PONode> *right_;
    PtmObjectWrapper<PONode> *parent_;

public:
    PONode() : val_(0), left_(nullptr), right_(nullptr),
               parent_(nullptr) {};
    PONode(Value_t val) : val_(val), left_(nullptr),
               right_(nullptr), parent_(nullptr) {};
    ~PONode() {};
#ifndef PMDKTX_H_
    AbstractPtmObject *Clone() {
        PONode *po_node = new PONode();
        po_node->val_ = val_;
        po_node->left_ = left_;
        po_node->right_ = right_;
        po_node->parent_ = parent_;
        return po_node;
    }
    void Copy(AbstractPtmObject *ptm_object) {
        PONode *po_node = (PONode *)ptm_object;
        val_ = po_node->val_;
        left_ = po_node->left_;
        right_ = po_node->right_;
        parent_ = po_node->parent_;
    }
#endif
};

class Bst : public AbstractIntset {
public:
    // sentinel_ is pointing to root with left_;
    PtmObjectWrapper<PONode> *sentinel_;

public:
    Bst() {
        PTM_START(RDWR);
#ifdef USE_AEP
        sentinel_ = (PtmObjectWrapper<PONode> *)vmem_malloc(sizeof(PtmObjectWrapper<PONode>));
        new (sentinel_) PtmObjectWrapper<PONode>();
#else
        sentinel_ = new PtmObjectWrapper<PONode>();
#endif
        PONode *po_node = sentinel_->Open(WRITE);
        po_node->left_ = nullptr;
        po_node->right_ = nullptr;
        po_node->parent_ = nullptr;
        PTM_COMMIT;
    }
    ~Bst() {
        ;
    }
    // if exists, return 1, else return 0
    Value_t Get(Key_t key);
    // do not support update operation
    bool Update(Key_t key, Value_t val) {return false;}
    // key is equal to val
    int Insert(Key_t key, Value_t val);
    int Delete(Key_t key);
    unsigned long long Size();

private:
    void Transplant(PONode *old_parent,
                PtmObjectWrapper<PONode> *old_wrapper,
                PONode *old, 
                PtmObjectWrapper<PONode> *new_wrapper);
#ifdef PMDKTX_H_
private:
    std::shared_mutex rw_lock_;
#endif

};

Value_t Bst::Get(Key_t key) {
#ifdef PMDKTX_H_
    std::unique_lock<std::shared_mutex> lock(rw_lock_);
#endif
    PTM_START(RDONLY);
    Value_t retval = 0;
    PONode *sentinel_node = sentinel_->Open(READ);
    PtmObjectWrapper<PONode> *curr_wrapper = sentinel_node->left_;
    PONode *curr;
    while (curr_wrapper != nullptr) {
        curr = curr_wrapper->Open(READ);
        if (key == curr->val_) {
            retval = 1;
            break;
        } else if (key > curr->val_) {
            curr_wrapper = curr->right_;
        } else {
            curr_wrapper = curr->left_;
        }
    }
    PTM_COMMIT;
    return retval;
}

int Bst::Insert(Key_t key, Value_t val) {
#ifdef PMDKTX_H_
    std::unique_lock<std::shared_mutex> lock(rw_lock_);
#endif
    PTM_START(RDWR);
    PtmObjectWrapper<PONode> *curr_wrapper = sentinel_;
    PtmObjectWrapper<PONode> *saved_curr_wrapper = curr_wrapper;
    PONode *curr;
    while (curr_wrapper != nullptr) {
        curr = curr_wrapper->Open(READ);
        saved_curr_wrapper = curr_wrapper;
        if (saved_curr_wrapper == sentinel_) {
            curr_wrapper = curr->left_;
        } else {
            if (key > curr->val_) {
                curr_wrapper = curr->right_;
            } else {
                curr_wrapper = curr->left_;
            }
        }
    }
#ifdef USE_AEP
        PtmObjectWrapper<PONode> *new_po_node_wrapper = (PtmObjectWrapper<PONode> *)vmem_malloc(sizeof(PtmObjectWrapper<PONode>));
        new (new_po_node_wrapper) PtmObjectWrapper<PONode>();
#else
        PtmObjectWrapper<PONode> *new_po_node_wrapper = new PtmObjectWrapper<PONode>();
#endif
    PONode *new_po_node = new_po_node_wrapper->Open(WRITE);
    new_po_node->val_ = val;
    new_po_node->left_ = nullptr;
    new_po_node->right_ = nullptr;
    new_po_node->parent_ = saved_curr_wrapper;
    curr = saved_curr_wrapper->Open(WRITE);
    if (saved_curr_wrapper == sentinel_) {
        curr->left_ = new_po_node_wrapper;
    } else {
        if (key > curr->val_) {
            curr->right_ = new_po_node_wrapper;
        } else {
            curr->left_ = new_po_node_wrapper;
        }
    }
    PTM_COMMIT;
    return 1;
}

/*
 * old_parent and old should be opened with WRITE before calling this func.
 */
void Bst::Transplant(PONode *old_parent,
                PtmObjectWrapper<PONode> *old_wrapper,
                PONode *old, 
                PtmObjectWrapper<PONode> *new_wrapper) {
    if (old_parent->left_ == old_wrapper) {
        old_parent->left_ = new_wrapper;
    } else {
        old_parent->right_ = new_wrapper;
    }
    if (new_wrapper != nullptr) {
        PONode *new_ = new_wrapper->Open(WRITE);
        new_->parent_ = old->parent_;
    }
}

int Bst::Delete(Key_t key) {
#ifdef PMDKTX_H_
    std::unique_lock<std::shared_mutex> lock(rw_lock_);
#endif
    PTM_START(RDWR);
    bool retval = 0;
    // std::cout <<"delete: " << key << std::endl;
    PONode *sentinel_node = sentinel_->Open(READ);
    PtmObjectWrapper<PONode> *curr_wrapper = sentinel_node->left_;
    PONode *curr;
    while (curr_wrapper != nullptr) {
        curr = curr_wrapper->Open(READ);
        if (key == curr->val_) {
            break;
        } else if (key > curr->val_) {
            curr_wrapper = curr->right_;
        } else {
            curr_wrapper = curr->left_;
        }
    }
    if (curr_wrapper != nullptr) {
        curr = curr_wrapper->Open(WRITE);
        PONode *parent_node = curr->parent_->Open(WRITE);
        if (curr->left_ == nullptr) {
            Transplant(parent_node, curr_wrapper, curr, curr->right_);
        } else if (curr->right_ == nullptr) {
            Transplant(parent_node, curr_wrapper, curr, curr->left_);
        } else {
            // find the mininum from curr->right_
            PtmObjectWrapper<PONode> *successor_wrapper = curr->right_;
            PONode *successor = successor_wrapper->Open(READ);
            while (successor->left_ != nullptr) {
                successor_wrapper = successor->left_;
                successor = successor_wrapper->Open(READ);
            }
            successor = successor_wrapper->Open(WRITE);
            if (curr->right_ != successor_wrapper) {
                PONode *successor_parent = successor->parent_->Open(WRITE);
                Transplant(successor_parent, successor_wrapper, successor, successor->right_);
                successor->right_ = curr->right_;
                PONode *curr_right = curr->right_->Open(WRITE);
                curr_right->parent_ = successor_wrapper;
            }
            Transplant(parent_node, curr_wrapper, curr, successor_wrapper);
            successor->left_ = curr->left_;
            PONode *curr_left = curr->left_->Open(WRITE);
            curr_left->parent_ = successor_wrapper;
        }
        retval = 1;
    }
    PTM_COMMIT;
    return retval;
}

unsigned long long Bst::Size() {
#ifdef PMDKTX_H_
    std::shared_lock<std::shared_mutex> lock(rw_lock_);
#endif
    PTM_START(RDONLY);
    unsigned long long retval = 0;
    PONode *sentinel_node = sentinel_->Open(READ);
    PtmObjectWrapper<PONode> *wrapper = sentinel_node->left_;
    std::queue<PtmObjectWrapper<PONode> *> q;
    q.push(wrapper);
    while(!q.empty()) {
        wrapper = q.front();
        q.pop();
        if (wrapper != nullptr) {
            retval++;
            PONode *node = wrapper->Open(READ);
            q.push(node->left_);
            q.push(node->right_);
        }
    }
    PTM_COMMIT;
    return retval;
}

#endif
#ifndef INTSET_RBT_H_
#define INTSET_RBT_H_

// #ifndef STH_INTERFACE_TL2_H_
// #include "sth_interface_TL2.h"
// #endif

// #ifndef ORIGINAL_INTERFACE_TL2_H_
// #include "original_interface_TL2.h"
// #endif

#ifndef ORDO_INTERFACE_TL2_H_
#include "ordo_interface_TL2.h"
#endif

#ifndef INTSET_H_
#include "intset.h"
#endif

#include <queue>

enum Color {
    BLACK = 0,
    RED = 1,
};

class PONode : public PtmObjectInterface {
public:
    Value_t val_;
    PtmObjectWrapper<PONode> *left_;
    PtmObjectWrapper<PONode> *right_;
    PtmObjectWrapper<PONode> *parent_;
    Color color_;

public:
    PONode() : val_(0), left_(nullptr), right_(nullptr),
               parent_(nullptr), color_(BLACK) {};
    ~PONode() {};
    PtmObjectInterface *Clone() {
        PONode *po_node = new PONode();
        po_node->val_ = val_;
        po_node->left_ = left_;
        po_node->right_ = right_;
        po_node->parent_ = parent_;
        po_node->color_ = color_;
        return po_node;
    }
    void Copy(PtmObjectInterface *ptm_object) {
        PONode *po_node = (PONode *)ptm_object;
        val_ = po_node->val_;
        left_ = po_node->left_;
        right_ = po_node->right_;
        parent_ = po_node->parent_;
        color_ = po_node->color_;
    }
};

class Rbt : public AbstractIntset {
public:
    // sentinel_ is pointing to root_ with left_;
    PtmObjectWrapper<PONode> *sentinel_;
    // nil_
    PtmObjectWrapper<PONode> *nil_;

public:
    Rbt() {
        PTM_START(RDWR);
        // init nil_
        nil_ = new PtmObjectWrapper<PONode>();
        PONode *nil_node = nil_->Open(WRITE);
        nil_node->left_ = nullptr;
        nil_node->right_ = nullptr;
        nil_node->parent_ = nullptr;
        nil_node->color_ = BLACK;

        // init sentinel_
        sentinel_ = new PtmObjectWrapper<PONode>();
        PONode *sentinel_node = sentinel_->Open(WRITE);
        sentinel_node->left_ = nil_;    // thsi is root
        sentinel_node->right_ = nullptr;
        sentinel_node->parent_ = nullptr;
        sentinel_node->color_ = BLACK;
        
        PTM_COMMIT;
    }
    ~Rbt() {
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
};

Value_t Rbt::Get(Key_t key) {
    PTM_START(RDONLY);
    Value_t retval = 0;
    PONode *sentinel_node = sentinel_->Open(READ);
    PtmObjectWrapper<PONode> *curr_wrapper = sentinel_node->left_;
    PONode *curr;
    while (curr_wrapper != nil_) {
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

int Rbt::Insert(Key_t key, Value_t val) {
    PTM_START(RDWR);
    PtmObjectWrapper<PONode> *curr_wrapper = sentinel_;
    PtmObjectWrapper<PONode> *saved_curr_wrapper = curr_wrapper;
    PONode *curr;
    while (curr_wrapper != nil_) {
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
    PtmObjectWrapper<PONode> *new_po_node_wrapper = new PtmObjectWrapper<PONode>();
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
void Rbt::Transplant(PONode *old_parent,
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

int Rbt::Delete(Key_t key) {
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

unsigned long long Rbt::Size() {
    PTM_START(RDONLY);
    unsigned long long retval = 0;
    PONode *sentinel_node = sentinel_->Open(READ);
    PtmObjectWrapper<PONode> *wrapper = sentinel_node->left_;
    std::queue<PtmObjectWrapper<PONode> *> q;
    q.push(wrapper);
    while(!q.empty()) {
        wrapper = q.front();
        q.pop();
        if (wrapper != nil_) {
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
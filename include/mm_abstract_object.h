#ifndef MM_ABSTRACT_OBJECT_H_
#define MM_ABSTRACT_OBJECT_H_

class Partition;

/*
 * All object that want to use mm, 
 * should inherite this abstract object.
 */
class MMAbstractObject {
public:
    Partition *__owner_partition_;
    MMAbstractObject *__next_;
    MMAbstractObject() : __owner_partition_(nullptr), __next_(nullptr) {};
    virtual ~MMAbstractObject() {};
};

#endif
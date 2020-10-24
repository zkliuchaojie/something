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
    void *__mm_pool_;   // recording which pool it belongs to.
    MMAbstractObject() : __owner_partition_(nullptr), \
    __next_(nullptr), __mm_pool_(nullptr) {};
    virtual ~MMAbstractObject() {};
    //virtual void Destroy(void *object) = 0;
};

#endif
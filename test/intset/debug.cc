#ifndef INTSET_H_
#include "intset.h"
#endif

// #ifndef INTSET_HASH_TABLE_H_
// #include "hashtable.h"
// #endif

// #ifndef INTSET_LINKEDLIST_H_
// #include "linkedlist.h"
// #endif

#ifndef INTSET_BST_H_
#include "bst.h"
#endif

#include <iostream>
#include <thread>
#include <sys/sysinfo.h>
#include <sys/time.h>
#include <fstream>
#include <time.h>
#include <unistd.h>

#define NR_OPERATIONS   2048
#define INPUT_FILE      "data"      // using input_gen.cpp to generate data file

#define NR_THREADS      1           // must be equal or less nr_cpus
#define NR_RECOVERY_THREADS     1   //the default value

bool stop = 0;
double *times;

double mysecond() {
    struct timeval tp;
    int i;
    i = gettimeofday(&tp, NULL);
    return ( (double)tp.tv_sec + (double)tp.tv_usec*1.e-6);
}

void insert(AbstractIntset *set, Value_t *values, int cur_nr_threads) {
    for(int i=0; !stop && i<NR_OPERATIONS/cur_nr_threads; i++) {
        set->Insert(values[i], values[i]);
    }
}

void search(AbstractIntset *set, Value_t *values, int cur_nr_threads) {
    bool tmp_value;
    for(int i=0; !stop && i<NR_OPERATIONS/cur_nr_threads; i++) {
        tmp_value = set->Get(values[i]);
        if(__glibc_unlikely(tmp_value != true)){
            std::cout << "search failed, value: " << values[i] << std::endl;
        }
    }
}

void _delete(AbstractIntset *set, Value_t *values, int cur_nr_threads) {
    bool is_delete_success;
    for(int i=0; !stop && i<NR_OPERATIONS/cur_nr_threads; i++) {
        is_delete_success = set->Delete(values[i]);
        if(__glibc_unlikely(is_delete_success == false)){
            std::cout << "delele failed, key: " << (unsigned long)values[i] << std::endl;
        }
    }
}

void print_result(int thread_num, AbstractIntset *intset) {
    double total_time = times[1] - times[0];
    std::cout << "thread num: " << thread_num+1;
    std::cout <<", total time: " << total_time << \
    ", OPS: " << NR_OPERATIONS*1.0/(total_time) << \
    ", size: " << intset->Size() << std::endl;
}

void clear_cache() {
    const size_t size = 1024*1024*512;
    int* dummy = new int[size];
    for (int i=100; i<size; i++) {
        dummy[i] = i;
    }
    for (int i=100;i<size-(1024*1024);i++) {
        dummy[i] = dummy[i-rand()%100] + dummy[i+rand()%100];
    }
    delete[] dummy;
}

AbstractIntset *set_new() {
#ifdef INTSET_HASH_TABLE_H_
    return new HashTable();
#else
#ifdef INTSET_LINKEDLIST_H_
  return new LinkedList();
#else
#ifdef INTSET_BST_H_
  return new Bst();
#endif
#endif
#endif
}

int main() {
    // register_printcs_with_signal(SIGSEGV);
    // get keys and values from file
    Value_t *values = new Value_t[NR_OPERATIONS];
    std::ifstream ifs;
    ifs.open(INPUT_FILE);
    unsigned long tmp;
    for(long long int i=0; i<NR_OPERATIONS; ++i) {
        ifs >> values[i];
        // forget the values
        ifs >> tmp;
    }

    int nr_cpus = get_nprocs_conf();
    std::thread *threads[nr_cpus];
    times = new double [2];
    AbstractIntset *set;
    //test insert
    std::cout << "test insert: " << std::endl;
    for(int thread_idx=0; thread_idx<NR_THREADS; thread_idx++) {
        set = set_new();
        times[0] = mysecond();
        for(long long int i=0; i<=thread_idx; i++)
            threads[i] = new std::thread(insert, set, values+i*NR_OPERATIONS/(thread_idx+1),
                 thread_idx+1);
        for(int i=0; i<=thread_idx; i++)
            threads[i]->join();
        times[1] = mysecond();
        print_result(thread_idx, set);
        delete set;
    }
    // test get
    std::cout << "test get: " << std::endl;
    for(int thread_idx=0; thread_idx<NR_THREADS; thread_idx++) {
        set = set_new();
        // insert data first
        for(long long int i=0; i<=thread_idx; i++)
            threads[i] = new std::thread(insert, set, values+i*NR_OPERATIONS/(thread_idx+1),
                thread_idx+1);
        for(int i=0; i<=thread_idx; i++)
            threads[i]->join();

        times[0] = mysecond();
        for(long long int i=0; i<=thread_idx; i++)
            threads[i] = new std::thread(search, set, values+i*NR_OPERATIONS/(thread_idx+1),
                thread_idx+1);
        for(int i=0; i<=thread_idx; i++)
            threads[i]->join();
        times[1] = mysecond();
        print_result(thread_idx, set);
        delete set;
    }
    // test _delete
    std::cout << "test _delete: " << std::endl;
    for(int thread_idx=0; thread_idx<NR_THREADS; thread_idx++) {
        set = set_new();
        // insert data first
        for(long long int i=0; i<=thread_idx; i++)
            threads[i] = new std::thread(insert, set, values+i*NR_OPERATIONS/(thread_idx+1),
                 thread_idx+1);
        for(int i=0; i<=thread_idx; i++)
            threads[i]->join();

        times[0] = mysecond();
        for(long long int i=0; i<=thread_idx; i++)
            threads[i] = new std::thread(_delete, set, values+i*NR_OPERATIONS/(thread_idx+1),
               thread_idx+1);
        for(int i=0; i<=thread_idx; i++)
            threads[i]->join();
        times[1] = mysecond();
        print_result(thread_idx, set);
        delete set;
    }
    return 0;
}

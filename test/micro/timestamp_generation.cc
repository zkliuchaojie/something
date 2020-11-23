#include <iostream>
#include <thread>
#include <sys/sysinfo.h>
#include <sys/time.h>
#include <fstream>
#include <time.h>
#include <unistd.h>

// int threads_array_size = 10;
// int threads_num[10] = {1, 8, 16, 24, 32, 40, 48, 56, 64, 72};
int threads_array_size = 4;
int threads_num[4] = {1, 2, 3, 4};
unsigned long long counters[72];
volatile bool stop = false;

// a global logical clock
#define ATOMIC_ADD_FETCH(ptr, val)  __atomic_add_fetch (ptr, val, __ATOMIC_RELAXED)
volatile unsigned long long __clock = 1;
void global_clock(int tid) {
    unsigned long long counter = 0;
    while (!stop) {
        ATOMIC_ADD_FETCH(&__clock, 1);
        counter++;
    }
    counters[tid] = counter;
}

// ORDO
const unsigned long long ordo_boundary = 340;
static inline unsigned long ReadTSC(void) {
    unsigned long var;
    unsigned int hi, lo;
    asm volatile("rdtsc":"=a"(lo),"=d"(hi));
    var = ((unsigned long long int) hi << 32) | lo;
    return var;
}

unsigned long long get_time() {
    return ReadTSC();
}

bool cmp_time(unsigned long long t1, unsigned long long t2) {
    if (t1 > t2 + ordo_boundary)
        return true;
    return false;
}
unsigned long long new_time(unsigned long long t) {
    unsigned long long new_time;
    while(cmp_time(new_time=get_time(), t) == false)
        ;
    return new_time;
}

void ORDO(int tid) {
    unsigned long long counter = 0;
    unsigned long long start = 0;
    while (!stop) {
        start = new_time(start);
        counter++;
    }
    counters[tid] = counter;
}

// Sclock
#define TI_AND_TS(ti, ts) (((unsigned long long)ti<<(64-8)) | (ts))
unsigned long long Sclock_new_time(int tid, unsigned long long &thread_clock) {
    thread_clock += 1;
    return TI_AND_TS(tid, thread_clock);
}
void Sclock(int tid) {
    unsigned long long counter = 0;
    unsigned long long start = 0;
    while (!stop) {
        start = Sclock_new_time(tid, start);
        counter++;
    }
    counters[tid] = counter;
}

int main() {
    std::cout << "start global logical clock test" << std::endl;
    std::thread *threads[72];
    for(int i=0; i<threads_array_size; i++) {
        unsigned long long throughput = 0;
        for(int j=0; j<5; j++) {
            stop = false;
            __clock = 1;
            for(int pos=0; pos<72; pos++)
                counters[pos] = 0;
            for(int tid=0; tid<threads_num[i]; tid++)
                threads[i] = new std::thread(global_clock, tid);
            // 10s
            struct timespec timeout;
            timeout.tv_sec = 10;
            timeout.tv_nsec = 0;
            nanosleep(&timeout, NULL);
            stop = 1;
            for(int tid=0; tid<threads_num[i]; tid++) {
                while (counters[tid] == 0)
                    sleep(1);
                throughput += counters[tid];
            }
        }
        std::cout << throughput*1.0/5/1000000 << std::endl;
    }

    std::cout << "start ORDO test" << std::endl;
    for(int i=0; i<threads_array_size; i++) {
        unsigned long long throughput = 0;
        for(int j=0; j<5; j++) {
            stop = false;
            __clock = 1;
            for(int pos=0; pos<72; pos++)
                counters[pos] = 0;
            for(int tid=0; tid<threads_num[i]; tid++)
                threads[i] = new std::thread(ORDO, tid);
            // 10s
            struct timespec timeout;
            timeout.tv_sec = 10;
            timeout.tv_nsec = 0;
            nanosleep(&timeout, NULL);
            stop = 1;
            for(int tid=0; tid<threads_num[i]; tid++) {
                while (counters[tid] == 0)
                    sleep(1);
                throughput += counters[tid];
            }
        }
        std::cout << throughput*1.0/5/1000000 << std::endl;
    }

    std::cout << "start Sclock test" << std::endl;
    for(int i=0; i<threads_array_size; i++) {
        unsigned long long throughput = 0;
        for(int j=0; j<5; j++) {
            stop = false;
            __clock = 1;
            for(int pos=0; pos<72; pos++)
                counters[pos] = 0;
            for(int tid=0; tid<threads_num[i]; tid++)
                threads[i] = new std::thread(Sclock, tid);
            // 10s
            struct timespec timeout;
            timeout.tv_sec = 10;
            timeout.tv_nsec = 0;
            nanosleep(&timeout, NULL);
            stop = 1;
            for(int tid=0; tid<threads_num[i]; tid++) {
                while (counters[tid] == 0)
                    sleep(1);
                throughput += counters[tid];
            }
        }
        std::cout << throughput*1.0/5/1000000 << std::endl;
    }
    return 0;
}

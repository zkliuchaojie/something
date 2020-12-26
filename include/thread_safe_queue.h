#ifndef THREAD_SAFE_QUEUE_H_
#define THREAD_SAFE_QUEUE_H_

#include <queue>
#include <thread>
#include <mutex>
#include <condition_variable>

template <typename T>
class ThreadSafeQueue
{
public:
    T pop() {
        std::unique_lock<std::mutex> mlock(mutex_);
        if (queue_.empty()) {
            return nullptr;
        }
        auto item = queue_.front();
        queue_.pop();
        return item;
    }

    void push(const T& item) {
        std::unique_lock<std::mutex> mlock(mutex_);
        queue_.push(item);
        mlock.unlock();
    }

private:
    std::queue<T> queue_;
    std::mutex mutex_;
};

#endif
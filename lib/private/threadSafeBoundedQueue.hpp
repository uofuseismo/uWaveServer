#ifndef UWAVE_SERVER_PRIVATE_THREAD_SAFE_BOUNDED_QUEUE_HPP
#define UWAVE_SERVER_PRIVATE_THREAD_SAFE_BOUNDED_QUEUE_HPP
#include <queue>
#include <memory>
#include <mutex>
#include <condition_variable>
namespace
{
/// @brief This is a thread-safe bounded queue based on listing 4.5 of C++
///        Concurrency in Action, 2nd Edition. 
template<typename T>
class ThreadSafeBoundedQueue
{
public:
    /// @name Constructors
    /// @{

    /// @brief Constructor.
    ThreadSafeBoundedQueue() = default;
    /// @brief Constructor with a given size.
    explicit ThreadSafeBoundedQueue(const int capacity)
    {
        setCapacity(capacity);
    }
    /// @brief Copy constructor.
    /// @param[in] queue  The queue from which to initialize this class.
    ThreadSafeBoundedQueue(const ThreadSafeBoundedQueue &queue)
    {
        *this = queue;
    }
    /// @brief Move constructor.
    /// @param[in,out] queue  The queue from which to initialize this class.
    ///                       On exit, queue's behavior is undefined.
    ThreadSafeBoundedQueue(ThreadSafeBoundedQueue &&queue) noexcept
    {
        *this = std::move(queue);
    }
    /// @}

    /// @name Operators
    /// @{
    /// @brief Copy assignment.
    /// @param[in] queue  The queue to copy to this.
    /// @result A deep copy of the input queue.
    ThreadSafeBoundedQueue& operator=(const ThreadSafeBoundedQueue &queue)
    {
        if (&queue == this){return *this;}
        std::lock_guard<std::mutex> lockGuard(queue.mMutex);
        mDataQueue = queue.mDataQueue;
    }
    /// @brief Move assignment.
    /// @param[in,out] queue  The queue whose memory will be moved to this.
    ///                       On exit, queue's behavior is undefined. 
    /// @result The memory from queue moved to this.
    ThreadSafeBoundedQueue& operator=(ThreadSafeBoundedQueue &&queue) noexcept
    {
        if (&queue == this){return *this;}
        std::lock_guard<std::mutex> lockGuard(queue.mMutex);
        mDataQueue = std::move(queue.mDataQueue);
    }
    /// @}

    /// @brief Sets the capacity.
    void setCapacity(const int capacity)
    {
        if (capacity < 1)
        {
            throw std::invalid_argument("Bounded queue capacity must be positive");
        }
        std::lock_guard<std::mutex> lockGuard(mMutex);
        // May have to make the queue smaller
        auto queueSize = static_cast<int> (mDataQueue.size());
        if (queueSize > capacity)
        {
            while (static_cast<int> (mDataQueue.size()) > capacity)
            {
                mDataQueue.pop();
            }
        }
        mCapacity = capacity;
    }
    /// @brief Adds a value to the back of the bounded queue.
    /// @param[in] value  The value to add to the bounded queue.
    void push(const T &value)
    {
        std::lock_guard<std::mutex> lockGuard(mMutex);
        if (mCapacity > 0)
        {
            auto queueSize = static_cast<int> (mDataQueue.size()); 
            if (queueSize >= mCapacity)
            {
                mDataQueue.pop(); // Pop the first (oldest) element
            } 
        }
        mDataQueue.push(value);
        mConditionVariable.notify_one(); // Let waiting thread know 
    }
    /// @brief Adds a value to the back of the bounded queue.
    /// @param[in,out] value  The value to add to the bounded queue.
    ///                       On exit, value's behavior is undefined.
    void push(T &&value)
    {
        std::lock_guard<std::mutex> lockGuard(mMutex);
        if (mCapacity > 0)
        {
            auto queueSize = static_cast<int> (mDataQueue.size()); 
            if (queueSize >= mCapacity)
            {
                mDataQueue.pop(); // Pop the first (oldest) element
            }
        }
        mDataQueue.push(std::move(value));
        mConditionVariable.notify_one(); // Let waiting thread know 
    }
    /// @brief Copies the value from the front of the bounded queue and removes
    ///        that value from the front of the bounded queue.
    /// @param[out] value  A copy of the value at the front of the queue.
    void wait_and_pop(T *value)
    {
        // Waiting thread needs to unlock mutex while waiting and lock again
        // afterward.  unique_lock does this while lock_guard does not.
        std::unique_lock<std::mutex> lockGuard(mMutex);
        mConditionVariable.wait(lockGuard, [this]
                                {
                                    return !mDataQueue.empty();
                                });
        *value = std::move(mDataQueue.front());
        mDataQueue.pop();
    }
    /// @brief Copies the value from the front of the bounded queue and
    ///        removes that value from the front of the bounded queue.
    /// @param[out] value  The value from the front of the queue.  Or, if the
    ///                    function times out, then a NULL pointer.
    /// @result True indicates that the value was set whereas false indicate.
    ///         the class timed out.
    /// @note This variant can time out and return NULL.
    [[nodiscard]]
    bool wait_until_and_pop(T *value,
                            const std::chrono::milliseconds &waitFor
                               = static_cast<std::chrono::milliseconds> (10))
    {
        auto now = std::chrono::system_clock::now();
        std::unique_lock<std::mutex> lockGuard(mMutex);
        if (mConditionVariable.wait_until(lockGuard, now + waitFor, [this] 
                                         {
                                             return !mDataQueue.empty();
                                         }))
        {
            *value = std::move(mDataQueue.front());
            mDataQueue.pop();
            return true;
        }
        return false;
    }
    /// @brief Pops the front of the bounded queue.  This returns nothing.
    /// @note This is useful when enforcing a maximum queue size.
    void pop()
    {
        std::lock_guard<std::mutex> lockGuard(mMutex);
        mDataQueue.pop();
    }
    /// @result A container with the value from the front of the bounded queue.
    ///         The value at front of the bounded queue is removed.
    /// @result The value at the front of the queue.
    [[nodiscard]] std::shared_ptr<T> wait_and_pop()
    {
        std::unique_lock<std::mutex> lockGuard(mMutex);
        mConditionVariable.wait(lockGuard, [this]
                                {
                                    return !mDataQueue.empty();
                                });
        std::shared_ptr<T> result{std::make_shared<T> (std::move(mDataQueue.front()))};
        mDataQueue.pop();
        return result;
    }
    /// @brief Attempts to copy the value of the value at the front of 
    ///        the bounded queue and remove it from the bounded queue.
    /// @param[out] value  If not a nullptr, then this is the value at the front
    ///                    at the front of the queue.
    /// @retval True indicates that the queue was not empty and value 
    ///         corresponds to the value that was at the front of the queue.
    /// @retval False indicates that the queue was empty. 
    [[nodiscard]] bool try_pop(T *value)
    {
        *value = nullptr;
        std::lock_guard<std::mutex> lockGuard(mMutex);
        if (mDataQueue.empty()){return false;}
        *value = std::move(mDataQueue.front());
        mDataQueue.pop();
        return true;
    }
    /// @brief A container with the value at the front of the bounded queue
    ///        provided that the queue is not empty.  If the bounded queue is
    ///        not empty then then this value is removed from the front. 
    /// @result The value at the front of the queue or a nullptr if the queue
    ///         was empty. 
    [[nodiscard]] std::shared_ptr<T> try_pop()
    {
        std::lock_guard<std::mutex> lockGuard(mMutex);
        std::shared_ptr<T> result;
        if (mDataQueue.empty())
        {
            result = nullptr;
            return result;
        }
        result = std::make_shared<T> (std::move(mDataQueue.front()));
        mDataQueue.pop();
        return result;
    }
    /// @result True indicates that the bounded queue is empty.
    [[nodiscard]] bool empty() const
    {
        std::lock_guard<std::mutex> lockGuard(mMutex);
        return mDataQueue.empty();
    }
    /// @result The number of elements in the bounded queue.
    [[nodiscard]] size_t size() const
    {
        std::lock_guard<std::mutex> lockGuard(mMutex);
        return mDataQueue.size();
    }
    [[nodiscard]] size_t capacity() const
    {
        std::lock_guard<std::mutex> lockGuard(mMutex);
        return static_cast<size_t> (mCapacity);
    }
    /// @result The maximum number of elements in the bounded queue.
    /// @name Constructors
    /// @{

    /// @brief Empties the bounded queue.
    //void clear() noexcept
    //{
    //    std::lock_guard<std::mutex> lockGuard(mMutex);
    //    mDataQueue.clear();
    //}
    /// @brief Destructor.
    ~ThreadSafeBoundedQueue() = default;
    /// @}
private:
    mutable std::mutex mMutex;
    std::queue<T> mDataQueue;
    std::condition_variable mConditionVariable;
    int mCapacity{-1}; // By default treat this like an unbounded queue
};
}
#endif

#ifndef __DCSR__RING_BUFFER_H__
#define __DCSR__RING_BUFFER_H__

#include <cassert>
#include <csignal>
#include <memory>
#include <span>
#include <queue>
#include <boost/container/static_vector.hpp>
#include "numa.h"

#include "env.h"

/**
 * @brief Simplified version of std::circular_buffer, no allocator, no iterator, no reallocation
 *        Use std components instead of boost components.
 *        Add free_space_one() and free_space_two() to get free space in buffer.
 *        Add advance_back() to advance back pointer.
 *        Combine free_space_* and advance_back(), can prepare elements inplace.
 * @tparam T 
 */
template<typename T>
class circular_buffer {
public:
    using this_type = circular_buffer<T>;
    using value_type = T;
    using pointer = T*;
    using const_pointer = const T*;
    using reference = T&;
    using const_reference = const T&;
    using param_value_type = const value_type&;
    using rvalue_type = value_type&&;
    using difference_type = std::ptrdiff_t;
    using size_type = std::size_t;
    using capacity_type = std::size_t;

    using array_range = std::span<T>;
    using const_array_range = std::span<const T>;

    using allocator_type = std::allocator<T>;
    using Alloc = allocator_type;   // template argument of boost version

private:
    pointer m_buff;
    pointer m_end;
    pointer m_first;
    pointer m_last;
    size_type m_size;

public:
    reference operator [] (size_type index) {
        assert(index < size()); // check for invalid index
        return *add(m_first, index);
    }

    const_reference operator [] (size_type index) const {
        assert(index < size()); // check for invalid index
        return *add(m_first, index);
    }

    reference at(size_type index) {
        check_position(index);
        return (*this)[index];
    }

    const_reference at(size_type index) const {
        check_position(index);
        return (*this)[index];
    }

    reference front() {
        assert(!empty()); // check for empty buffer (front element not available)
        return *m_first;
    }

    reference back() {
        assert(!empty()); // check for empty buffer (back element not available)
        return *((m_last == m_buff ? m_end : m_last) - 1);
    }

    const_reference front() const {
        assert(!empty()); // check for empty buffer (front element not available)
        return *m_first;
    }

    const_reference back() const {
        assert(!empty()); // check for empty buffer (back element not available)
        return *((m_last == m_buff ? m_end : m_last) - 1);
    }

    array_range array_one() {
        return array_range(m_first, (m_last <= m_first && !empty() ? m_end : m_last) - m_first);
    }

    array_range array_two() {
        return array_range(m_buff, m_last <= m_first && !empty() ? m_last - m_buff : 0);
    }

    const_array_range array_one() const {
        return const_array_range(m_first, (m_last <= m_first && !empty() ? m_end : m_last) - m_first);
    }

    const_array_range array_two() const {
        return const_array_range(m_buff, m_last <= m_first && !empty() ? m_last - m_buff : 0);
    }

    array_range free_space_one() {
        return array_range(m_last, (m_last <= m_first && !empty() ? m_first : m_end) - m_last);
    }

    array_range free_space_two() {
        return array_range(m_buff, m_last <= m_first && !empty() ? 0 : m_first - m_buff);
    }

    bool is_linearized() const noexcept { return m_first < m_last || m_last == m_buff; }

// Size and capacity

    size_type size() const noexcept { return m_size; }

    size_type max_size() const noexcept {
        return std::numeric_limits<difference_type>::max();
    }

    bool empty() const noexcept { return size() == 0; }

    bool full() const noexcept { return capacity() == size(); }

    size_type reserve() const noexcept { return capacity() - size(); }

    capacity_type capacity() const noexcept { return m_end - m_buff; }

// Construction/Destruction

    explicit circular_buffer() noexcept
    : m_buff(0), m_end(0), m_first(0), m_last(0), m_size(0) {}

    explicit circular_buffer(capacity_type buffer_capacity)
    : m_size(0) {
        initialize_buffer(buffer_capacity);
        m_first = m_last = m_buff;
    }

    circular_buffer(size_type n, param_value_type item)
    : m_size(n) {
        initialize_buffer(n, item);
        m_first = m_last = m_buff;
    }

    circular_buffer(capacity_type buffer_capacity, size_type n, param_value_type item)
    : m_size(n) {
        assert(buffer_capacity >= size()); // check for capacity lower than size
        initialize_buffer(buffer_capacity, item);
        m_first = m_buff;
        m_last = buffer_capacity == n ? m_buff : m_buff + n;
    }

    circular_buffer(circular_buffer<T>&& cb) noexcept
    : m_buff(0), m_end(0), m_first(0), m_last(0), m_size(0) {
        cb.swap(*this);
    }

    ~circular_buffer() noexcept {
        destroy();
    }

// Push and pop
private:
    /*! INTERNAL ONLY */
    template <class ValT>
    void push_back_impl(ValT item) {
        if (full()) {
            if (empty())
                return;
            // std::raise(SIGINT);
            // fmt::println("warning: circular_buffer is full, pop_front() is called");
            replace(m_last, static_cast<ValT>(item));
            increment(m_last);
            m_first = m_last;
        } else {
            std::construct_at(m_last, static_cast<ValT>(item));
            increment(m_last);
            ++m_size;
        }
    }

public:

    void push_back(param_value_type item) {
        push_back_impl<param_value_type>(item);
    }

    void push_back(rvalue_type item) {
        push_back_impl<rvalue_type>(std::move(item));
    }

    void push_back() {
        value_type temp;
        push_back(std::move(temp));
    }

    void advance_back(size_t n) {
        if(n <= reserve()) {
            m_last = add(m_last, n);    
            m_size += n;
        } else {
            if(capacity() == 0) {
                return;
            }
            m_last = add(m_last, n % capacity());
            m_size = capacity();
            m_first = m_last;
        }
    }

    void pop_back() {
        assert(!empty()); // check for empty buffer (back element not available)
        decrement(m_last);
        destroy_item(m_last);
        --m_size;
    }

    void pop_front() {
        assert(!empty()); // check for empty buffer (front element not available)
        destroy_item(m_first);
        increment(m_first);
        --m_size;
    }

// Erase

    void erase_begin(size_type n) {
        assert(n <= size()); // check for n greater than size
        erase_begin(n, std::is_trivially_destructible<value_type>());
    }

    void erase_end(size_type n) {
        assert(n <= size()); // check for n greater than size
        erase_end(n, std::is_trivially_destructible<value_type>());
    }

    void clear() noexcept {
        destroy_content();
        m_size = 0;
    }

private:
// Helper methods

    /*! INTERNAL ONLY */
    void check_position(size_type index) const {
        if (index >= size())
            throw std::out_of_range("circular_buffer");
    }

    /*! INTERNAL ONLY */
    template <class Pointer>
    void increment(Pointer& p) const {
        if (++p == m_end)
            p = m_buff;
    }

    /*! INTERNAL ONLY */
    template <class Pointer>
    void decrement(Pointer& p) const {
        if (p == m_buff)
            p = m_end;
        --p;
    }

    /*! INTERNAL ONLY */
    template <class Pointer>
    Pointer add(Pointer p, difference_type n) const {
        return p + (n < (m_end - p) ? n : n - (m_end - m_buff));
    }

    /*! INTERNAL ONLY */
    template <class Pointer>
    Pointer sub(Pointer p, difference_type n) const {
        return p - (n > (p - m_buff) ? n - (m_end - m_buff) : n);
    }

    /*! INTERNAL ONLY */
    pointer map_pointer(pointer p) const { return p == 0 ? m_last : p; }

    /*! INTERNAL ONLY */
    const Alloc& alloc() const {
        return std::allocator<value_type>();
    }

    /*! INTERNAL ONLY */
    Alloc& alloc() {
        static Alloc a;
        return a;
    }

    /*! INTERNAL ONLY */
    pointer allocate(size_type n) {
        if (n > max_size())
            throw std::length_error("circular_buffer");
        return (n == 0) ? 0 : alloc().allocate(n);
    }

    /*! INTERNAL ONLY */
    void deallocate(pointer p, size_type n) {
        if (p != 0)
            alloc().deallocate(p, n);
    }

    /*! INTERNAL ONLY */
    bool is_uninitialized(const_pointer p) const noexcept {
        return (m_first < m_last)
            ? (p >= m_last || p < m_first)
            : (p >= m_last && p < m_first);
    }

    /*! INTERNAL ONLY */
    void replace(pointer pos, param_value_type item) {
        *pos = item;
    }

    /*! INTERNAL ONLY */
    void replace(pointer pos, rvalue_type item) {
        *pos = std::move(item);
    }

    /*! INTERNAL ONLY */
    void construct_or_replace(bool construct, pointer pos, param_value_type item) {
        if (construct)
            std::construct_at(pos, item);
        else
            replace(pos, item);
    }

    /*! INTERNAL ONLY */
    void construct_or_replace(bool construct, pointer pos, rvalue_type item) {
        if (construct)
            std::construct_at(pos, std::move(item));
        else
            replace(pos, std::move(item));
    }

    /*! INTERNAL ONLY */
    void destroy_item(pointer p) {
        std::destroy_at(p);
    }

    /*! INTERNAL ONLY */
    void destroy_if_constructed(pointer pos) {
        if (is_uninitialized(pos))
            destroy_item(pos);
    }

    /*! INTERNAL ONLY */
    void destroy_content() {
        destroy_content(std::is_trivially_destructible<value_type>());
    }

    /*! INTERNAL ONLY */
    void destroy_content(const std::true_type&) {
        m_first = add(m_first, size());
    }

    /*! INTERNAL ONLY */
    void destroy_content(const std::false_type&) {
        for (size_type ii = 0; ii < size(); ++ii, increment(m_first))
            destroy_item(m_first);
    }

    /*! INTERNAL ONLY */
    void destroy() noexcept {
        destroy_content();
        deallocate(m_buff, capacity());
    }

    /*! INTERNAL ONLY */
    void initialize_buffer(capacity_type buffer_capacity) {
        m_buff = allocate(buffer_capacity);
        m_end = m_buff + buffer_capacity;
    }

    /*! INTERNAL ONLY */
    void reset(pointer buff, pointer last, capacity_type new_capacity) {
        destroy();
        m_size = last - buff;
        m_first = m_buff = buff;
        m_end = m_buff + new_capacity;
        m_last = last == m_end ? m_buff : last;
    }

    /*! INTERNAL ONLY */
    void erase_begin(size_type n, const std::true_type&) {
        m_first = add(m_first, n);
        m_size -= n;
    }

    /*! INTERNAL ONLY */
    void erase_begin(size_type n, const std::false_type&) {
        // iterator b = begin();
        // rerase(b, b + n);
        throw std::runtime_error("Not implemented");
    }

    /*! INTERNAL ONLY */
    void erase_end(size_type n, const std::true_type&) {
        m_last = sub(m_last, n);
        m_size -= n;
    }

    /*! INTERNAL ONLY */
    void erase_end(size_type n, const std::false_type&) {
        // iterator e = end();
        // erase(e - n, e);
        throw std::runtime_error("Not implemented");
    }

};

namespace dcsr {

/**
 * @brief Single reader, single writer ring buffer, with batch enqueue and dequeue.
 * Elements will prepare after last elements of buffer, and enqueue in batch.
 * 
 * @tparam T 
 */
template<typename T>
class BatchRingBuffer {
private:
    constexpr static size_t MAX_FREE_SPACE_BATCH = 65536;
    using array_range = std::span<T>;
    using semaphore_type = std::counting_semaphore<MAX_FREE_SPACE_BATCH>;
    using mutex_type = SpinMutex;
    using lock_guard = std::lock_guard<mutex_type>;

private:
    circular_buffer<T> buffer_;

    semaphore_type sema_free_space_; // has new free space
    mutex_type mutex_;     // as lightweight lock

    T* free_begin_;                     // [Writer only] free space begin
    // size_t at_least_free_;              // [Writer only] at least free space in buffer, avoid to lock every push
    size_t ready_to_enqueue_;           // [Writer only] ready to enqueue
    size_t had_enqueued_;               // [Writer only] had enqueued

    size_t visible_batch_size_;              // Constant
    size_t process_batch_size_;              // Constant

public:
    BatchRingBuffer() = default;

    BatchRingBuffer(size_t buffer_size, size_t visible_batch_size, size_t process_batch_size)
    : buffer_(buffer_size), sema_free_space_(0), mutex_(),
      free_begin_(nullptr), /*at_least_free_(0),*/ ready_to_enqueue_(0), had_enqueued_(0),
      visible_batch_size_(visible_batch_size), process_batch_size_(process_batch_size) {
        if(buffer_size == 0 || visible_batch_size == 0 || process_batch_size == 0) {
            throw std::runtime_error("BatchRingBuffer: initialize with 0 size");
        }
        if(buffer_size % visible_batch_size != 0 || buffer_size % process_batch_size != 0) {
            throw std::runtime_error("BatchRingBuffer: buffer_size % batch != 0");
        }
        if(buffer_size / process_batch_size > MAX_FREE_SPACE_BATCH) {
            throw std::runtime_error("BatchRingBuffer: buffer_size / process_batch_size > MAX_FREE_SPACE_BATCH");
        }

        //fmt::println("addr: {}", fmt::ptr(buffer_.array_one().data()));
        lock_guard lock(mutex_);
        size_t batch_count = buffer_size / process_batch_size;
        sema_free_space_.release(batch_count - 1);
        free_begin_ = buffer_.free_space_one().data();
        // at_least_free_ = process_batch_size;
    }

    ~BatchRingBuffer() = default;

    /**
     * @brief [Writer call] Enqueue an element, if enqueue_batch elements are ready, 
     * make them visible to the reader.
     * 
     * @param t 
     */
    void PushBack(const T& t) {
        *(free_begin_ + ready_to_enqueue_) = t;
        ready_to_enqueue_ ++;

        if(ready_to_enqueue_ - had_enqueued_ == visible_batch_size_) {
            // fmt::println("Batch enqueue: {}", ready_to_enqueue_);
            lock_guard lock(mutex_);
            buffer_.advance_back(visible_batch_size_);
            had_enqueued_ = ready_to_enqueue_;
            // fmt::println("Batch enqueue done");
        }

        if(ready_to_enqueue_ == process_batch_size_) {
            // fmt::println("Acquire free space");
            // auto t1 = std::chrono::high_resolution_clock::now();

            sema_free_space_.acquire(); // Has free space

            // auto t2 = std::chrono::high_resolution_clock::now();
            // double dur = std::chrono::duration<double>(t2 - t1).count();
            // if(dur > 1) {
            //     fmt::println("Wait for free space: {:.2f}s", dur);
            // }
            lock_guard lock(mutex_);
            free_begin_ = buffer_.free_space_one().data();
            buffer_.advance_back(ready_to_enqueue_ - had_enqueued_);
            ready_to_enqueue_ = 0;
            had_enqueued_ = 0;
            // fmt::println("Acquire free space done");
        }
        
        RUN_EXPR_IN_DEBUG(dcsr_assert(ready_to_enqueue_ >= had_enqueued_, fmt::format("ready_to_enqueue_={} < had_enqueued_={}", ready_to_enqueue_, had_enqueued_)));
    }

    /**
     * @brief [Writer call] Readonly function, but not thread safe.
     *        Only writer can call, or make sure no data is writing!!!
     *        Return that elements are ready to enqueue but not visible.
     * @return array_range 
     */
    array_range ReadyData() const {
        RUN_EXPR_IN_DEBUG(dcsr_assert(ready_to_enqueue_ >= had_enqueued_, fmt::format("ready_to_enqueue_={} < had_enqueued_={}", ready_to_enqueue_, had_enqueued_)));
        return array_range(free_begin_ + had_enqueued_, ready_to_enqueue_ - had_enqueued_);
    }

    array_range VisibleBatchUnsafe() {
        return array_range(buffer_.array_one().data(), std::min(buffer_.size(), process_batch_size_));
    }

    /**
     * @brief [Reader call] Return the current visible batch to the reader.
     * @return array_range 
     */
    array_range VisibleBatch() {
        lock_guard lock(mutex_);
        return VisibleBatchUnsafe();
    }

    T* VisibleBatchPointer() {
        lock_guard lock(mutex_);
        return VisibleBatchUnsafe().data();
    }

    /**
     * @brief [Reader call] Visible batch elements cout.
     * If ReleaseBatch is not called, the visible range address will not be changed.
     * Size <= process_batch_size (ensure data is in contiguous memory).
     */
    size_t VisibleBatchSize() {
        lock_guard lock(mutex_);
        return std::min(buffer_.size(), process_batch_size_);
    }

    /**
     * @brief [Reader call] Release the visible elements, make them free, and can be overwritten.
     * Reader should ensure the visible elements are enough (> process_batch_size).
     * @return array_range new visible range
     */
    array_range ReleaseBatch() {
        // fmt::println("ReleaseBatch");
        lock_guard lock(mutex_);
        if(VisibleBatchUnsafe().size() < process_batch_size_) {
            throw std::runtime_error("ReleaseBatch: visible_size_ < process_batch_size_");
        }
        buffer_.erase_begin(process_batch_size_);
        sema_free_space_.release();
        // fmt::println("ReleaseBatch done");
        return VisibleBatchUnsafe();
    }
};

/**
 * @brief Batch numa buffer, single reader - single writer.
 * No release, so it's not a ring buffer, simpler than BatchRingBuffer.
 * Support useful numa control.
 * @tparam T 
 */
template<typename T>
class alignas(CACHE_LINE_SIZE)
BatchNumaBuffer {
public:
    using array_range = std::span<T>;
    using const_array_range = std::span<const T>;
    using pointer = T*;
private:
    // Buffer
    const pointer buffer_;
    const size_t buffer_size_;
    const size_t visible_batch_mask_;
    size_t ready_size_;

    // uint64_t padding_[4];   // Padding to align with cache line

    std::atomic<size_t> visible_size_;

public:
    // BatchNumaBuffer(): buffer_(nullptr) {}

    BatchNumaBuffer(size_t buffer_size, size_t visible_batch_size, int numa_node)
        :   buffer_(NumaAllocArrayOnNode<T>(buffer_size, numa_node)),
            buffer_size_(buffer_size), 
            visible_batch_mask_(std::bit_ceil(visible_batch_size) - 1),
            ready_size_(0), visible_size_(0)
    {
        // memset(buffer_, 0, sizeof(T) * 64*1024*1024);
        if((visible_batch_mask_ + 1) != visible_batch_size) {
            fmt::println("Warning: visible_batch_size={} is not power of 2", visible_batch_size);
        }
    }

    ~BatchNumaBuffer() {
        NumaFreeArray(buffer_, buffer_size_);
    }

    void PushBack(const T& t) {
        buffer_[ready_size_] = t;
        ready_size_ ++;
        // fmt::println("buffer[{}]={}", ready_size_, t);

        // if(t.from == 1) {
        //     fmt::println("buffer[{}]={}", ready_size_, t);
        // }

        if((ready_size_ & visible_batch_mask_) == 0) {
            visible_size_.store(ready_size_, std::memory_order_release);
        }
    }

    pointer VisibleBatchPointer() {
        return buffer_;
    }

    size_t VisibleBatchSize() const {
        return visible_size_.load(std::memory_order_acquire);
    }

    size_t VisibleBatchSizeApprox() const {
        return ready_size_;
    }

    const_array_range ReadyData() const {
        size_t vs = visible_size_.load(std::memory_order_acquire);
        return const_array_range(buffer_ + vs, ready_size_ - vs);
    }

};

template<typename T>
struct alignas(CACHE_LINE_SIZE)
SubBuffer {
    T* buffer;
    uint64_t size;
    uint64_t capacity;
    std::atomic<uint64_t> latest_written_offset;
};

template<typename T, size_t MAX_THREADS=8>
class alignas(CACHE_LINE_SIZE)
MultiWritableBatchNumaBuffer {
public:
    using array_range = std::span<T>;
    using const_array_range = std::span<const T>;
    using pointer = T*;
private:
    // Buffer
    const pointer buffer_;
    std::atomic<uint64_t> allocated_size_;
    const uint64_t buffer_size_;
    const uint64_t visible_batch_size_;
    const uint64_t write_threads_;

    boost::container::static_vector<SubBuffer<T>, MAX_THREADS> sub_buffers_;

    // 主buffer只是atomic的，所有数据先写入sub buffer，写够后由sub buffer的线程请求主buffer的空间并写入。
    // 请求空间似乎可以原子，但记录写到哪似乎只能用锁。否则visible很难确认。
    //     或者每个sub buffer都有一个visible，记录自己写过主buffer的最大值。这样visible就是所有sub buffer的visible的最小值。

private:
    pointer AllocInBuffer(size_t size) {
        size_t off = allocated_size_.fetch_add(size, std::memory_order_seq_cst);
        // fmt::println("AllocInBuffer: off={}, size={}", off, size);
        return buffer_ + off;
    }

public:

    MultiWritableBatchNumaBuffer(size_t buffer_size, size_t visible_batch_size, size_t wthreads, int numa_node)
        :   buffer_(NumaAllocArrayOnNode<T>(buffer_size, numa_node)),
            allocated_size_(0),
            buffer_size_(buffer_size), 
            visible_batch_size_(std::bit_ceil(visible_batch_size)),
            write_threads_(wthreads)
    {
        sub_buffers_.resize(wthreads);
        for(size_t i = 0; i < wthreads; i++) {
            sub_buffers_[i].buffer = AllocInBuffer(visible_batch_size_);
            sub_buffers_[i].size = 0;
            sub_buffers_[i].capacity = visible_batch_size_;
            sub_buffers_[i].latest_written_offset.store(0, std::memory_order_seq_cst);
        }
    }

    ~MultiWritableBatchNumaBuffer() {
        NumaFreeArray(buffer_, buffer_size_);
    }

    void PushBackInto(const T& t, size_t idx) {
        // fmt::println("PushBackInto: buffer={}, idx={}, size={}", sub_buffers_[idx].buffer - buffer_, idx, sub_buffers_[idx].size);
        auto& sb = sub_buffers_[idx];
        sb.buffer[sb.size ++] = t;

        if(sb.size == sb.capacity) {
            size_t written_off = (sb.buffer - buffer_) + sb.size;
            sb.latest_written_offset.store(written_off, std::memory_order_seq_cst);
            sb.buffer = AllocInBuffer(visible_batch_size_);
            sb.size = 0;
            sb.capacity = visible_batch_size_;
        }
    }

    void Collect() {
        std::vector<std::pair<T*, size_t>> not_full;
        for(size_t i = 0; i < write_threads_; i++) {
            // fmt::println("Not full: {} ({})", sub_buffers_[i].buffer - buffer_, sub_buffers_[i].size);
            not_full.push_back({sub_buffers_[i].buffer, sub_buffers_[i].size});
        }
        std::sort(not_full.begin(), not_full.end());

        size_t k = 0;
        T* need_to_fill_buf = not_full[0].first;
        size_t pos = not_full[0].second;

        size_t mk = write_threads_ - 1;
        T* need_to_move_buf = not_full[mk].first;
        size_t mpos = not_full[mk].second;
        while(need_to_fill_buf < need_to_move_buf) {
            if(mpos <= visible_batch_size_ - pos) {  // move all
                // fmt::println("A: Move {} from {} to {}", mpos, need_to_move_buf - buffer_, need_to_fill_buf - buffer_);
                while(mpos > 0) {
                    need_to_fill_buf[pos ++] = need_to_move_buf[mpos - 1];
                    mpos --;
                }

                need_to_move_buf -= visible_batch_size_;
                if(need_to_move_buf == need_to_fill_buf) {
                    mpos = pos;
                    break;
                }

                if(need_to_move_buf == not_full[mk - 1].first) {
                    mpos = not_full[mk - 1].second;
                    mk --;
                } else {
                    mpos = visible_batch_size_;
                }

            } else {
                // fmt::println("B: Move {} from {} to {}", visible_batch_size_ - pos, need_to_move_buf - buffer_, need_to_fill_buf - buffer_);
                while(pos < visible_batch_size_) {
                    need_to_fill_buf[pos] = need_to_move_buf[mpos - 1];
                    pos ++;
                    mpos --;
                }
                // fmt::println("After move: pos={}, mpos={}", pos, mpos);
                if(k == write_threads_ - 1) {
                    break;
                }
                need_to_fill_buf = not_full[++k].first;
                pos = not_full[k].second;
            }
        }

        if(mpos == visible_batch_size_) {
            need_to_move_buf += visible_batch_size_;
            mpos = 0;
        }

        size_t new_visible = need_to_move_buf - buffer_;
        size_t new_allocated = new_visible + visible_batch_size_;
        allocated_size_.store(new_allocated, std::memory_order_seq_cst);
        // fmt::println("New visible: {}", new_visible);
        // fmt::println("New allocated: {}", new_allocated);

        sub_buffers_[0].buffer = need_to_move_buf;
        sub_buffers_[0].size = mpos;
        sub_buffers_[0].capacity = visible_batch_size_;
        sub_buffers_[0].latest_written_offset.store(new_visible, std::memory_order_seq_cst);
        
        for(size_t i = 1; i < write_threads_; i++) {
            sub_buffers_[i].buffer = AllocInBuffer(visible_batch_size_);
            sub_buffers_[i].size = 0;
            sub_buffers_[i].capacity = visible_batch_size_;
            sub_buffers_[i].latest_written_offset.store(new_visible, std::memory_order_seq_cst);
        }
    }

    pointer VisibleBatchPointer() {
        return buffer_;
    }

    size_t VisibleBatchSize() const {
        size_t latest = std::numeric_limits<size_t>::max();
        for(size_t i = 0; i < write_threads_; i++) {
            latest = std::min(latest, sub_buffers_[i].latest_written_offset.load(std::memory_order_acquire));
        }
        return latest;
    }

    const_array_range ReadyData() const {
        auto& sb0 = sub_buffers_[0];
        return const_array_range(sb0.buffer, sb0.size);
    }

};

} // namespace dcsr

#endif // __DCSR__RING_BUFFER_H__
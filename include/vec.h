#ifndef DARRAY_H
#define DARRAY_H

#include <memory>

namespace dcsr {

/**
 * @brief Unbounded vector (with unique_ptr<T[]>, with only capacity specified at initialization)
 * 
 * @tparam Item
*/
template<typename Item>
class uvec {
private:
    std::unique_ptr<Item[]> data_;
    size_t size_;
public:
    using value_type = Item;
    using size_type = size_t;
    using diffrence_type = std::ptrdiff_t;
    using reference = Item&;
    using const_reference = const Item&;
    using pointer = Item*;
    using const_pointer = const Item*;
    using iterator = Item*;
    using const_iterator = const Item*;
    using reverse_iterator = std::reverse_iterator<iterator>;
    using const_reverse_iterator = std::reverse_iterator<const_iterator>;

    uvec() noexcept: data_(nullptr), size_(0) {}
    uvec(size_t capacity): data_(std::make_unique_for_overwrite<Item[]>(capacity)), size_(0) {}
    uvec(uvec&& other) noexcept: data_(std::move(other.data_)), size_(other.size_) {}
    uvec(const uvec&) = delete;

    uvec& operator=(uvec&& other) {
        data_ = std::move(other.data_);
        size_ = other.size_;
        return *this;
    }
    uvec& operator=(const uvec&) = delete;

    // Element access
    Item&       operator[] (size_t idx)         { return data_[idx]; }
    const Item& operator[] (size_t idx) const   { return data_[idx]; }
    Item&       front()         { return data_[0]; }
    const Item& front() const   { return data_[0]; }
    Item&       back()          { return data_[size_ - 1]; }
    const Item& back() const    { return data_[size_ - 1]; }
    Item*       data()          { return data_.get(); }
    const Item* data() const    { return data_.get();}

    // Iterators
    Item*       begin()         { return data_.get(); }
    Item*       end()           { return data_.get() + size_; }
    const Item* begin() const   { return data_.get(); }
    const Item* end() const     { return data_.get() + size_; }
    const Item* cbegin() const  { return data_.get(); }
    const Item* cend() const    { return data_.get() + size_; }
    std::reverse_iterator<Item*>       rbegin()        { return std::reverse_iterator<Item*>(end()); }
    std::reverse_iterator<Item*>       rend()          { return std::reverse_iterator<Item*>(begin()); }
    std::reverse_iterator<const Item*> rbegin() const  { return std::reverse_iterator<const Item*>(end()); }
    std::reverse_iterator<const Item*> rend() const    { return std::reverse_iterator<const Item*>(begin()); }
    std::reverse_iterator<const Item*> crbegin() const { return std::reverse_iterator<const Item*>(end()); }
    std::reverse_iterator<const Item*> crend() const   { return std::reverse_iterator<const Item*>(begin()); }

    // Capacity
    bool       empty() const   { return size_ == 0; }
    size_t     size() const    { return size_; }
    // void      reserve(size_t new_capacity) { assert(false); }    // implement in dvec
    // size_t    capacity() const { assert(false); }    // implement in dvec

    // Modifiers
    void clear() { size_ = 0; }
    void push_back(const Item& item) { data_[size_++] = item; }
    void pop_back() { --size_; }
    void resize(size_t new_size) { size_ = new_size; }
    void swap(uvec& other) {
        std::swap(data_, other.data_);
        std::swap(size_, other.size_);
    }
    template<typename... Args>
    void emplace_back(Args&&... args) {
        new (data_.get() + size_) Item(std::forward<Args>(args)...);
        size_++;
    }
    std::unique_ptr<Item[]> release() {
        size_ = 0;
        return data_.release();
    }
    std::pair<std::unique_ptr<Item[]>, size_t> refresh(size_t new_capacity) {
        std::unique_ptr<Item[]> new_data = std::make_unique_for_overwrite<Item[]>(new_capacity);
        size_t sz = size_;

        size_ = 0;
        new_data.swap(data_);
        
        return std::make_tuple(std::move(new_data), sz);
    }

};

/**
 * @brief Bounded vector (capacity is stored)
 * 
 * @tparam Item
*/
template<typename Item>
class dvec {
private:
    std::unique_ptr<Item[]> data_;
    size_t size_;
    size_t capacity_;
public:
    using value_type = Item;
    using size_type = size_t;
    using diffrence_type = std::ptrdiff_t;
    using reference = Item&;
    using const_reference = const Item&;
    using pointer = Item*;
    using const_pointer = const Item*;
    using iterator = Item*;
    using const_iterator = const Item*;
    using reverse_iterator = std::reverse_iterator<iterator>;
    using const_reverse_iterator = std::reverse_iterator<const_iterator>;

    dvec() noexcept: data_(nullptr), size_(0), capacity_(0) {}
    dvec(size_t capacity): data_(std::make_unique_for_overwrite<Item[]>(capacity)), size_(0), capacity_(capacity) {}
    dvec(dvec&& other) noexcept: data_(std::move(other.data_)), size_(other.size_), capacity_(other.capacity_) {}
    dvec(const dvec&) = delete;

    dvec& operator=(dvec&& other) {
        data_ = std::move(other.data_);
        size_ = other.size_;
        capacity_ = other.capacity_;
        return *this;
    }
    dvec& operator=(const dvec&) = delete;

    // Element access
    Item&       operator[] (size_t idx)         { return data_[idx]; }
    const Item& operator[] (size_t idx) const   { return data_[idx]; }
    Item&       front()         { return data_[0]; }
    const Item& front() const   { return data_[0]; }
    Item&       back()          { return data_[size_ - 1]; }
    const Item& back() const    { return data_[size_ - 1]; }
    Item*       data()          { return data_.get(); }
    const Item* data() const    { return data_.get();}

    // Iterators
    Item*       begin()         { return data_.get(); }
    Item*       end()           { return data_.get() + size_; }
    const Item* begin() const   { return data_.get(); }
    const Item* end() const     { return data_.get() + size_; }
    const Item* cbegin() const  { return data_.get(); }
    const Item* cend() const    { return data_.get() + size_; }
    std::reverse_iterator<Item*>      rbegin()        { return std::reverse_iterator<Item*>(end()); }
    std::reverse_iterator<Item*>      rend()          { return std::reverse_iterator<Item*>(begin()); }
    std::reverse_iterator<const Item*> rbegin() const  { return std::reverse_iterator<const Item*>(end()); }
    std::reverse_iterator<const Item*> rend() const    { return std::reverse_iterator<const Item*>(begin()); }
    std::reverse_iterator<const Item*> crbegin() const { return std::reverse_iterator<const Item*>(end()); }
    std::reverse_iterator<const Item*> crend() const   { return std::reverse_iterator<const Item*>(begin()); }

    // Capacity
    bool       empty() const   { return size_ == 0; }
    bool       full() const    { return size_ == capacity_; }
    size_t     size() const    { return size_; }
    size_t     capacity() const { return capacity_; }
    void reserve(size_t new_capacity) {
        if (new_capacity > capacity_) {
            std::unique_ptr<Item[]> new_data = std::make_unique_for_overwrite<Item[]>(new_capacity);
            std::copy(data_.get(), data_.get() + size_, new_data.get());
            data_ = std::move(new_data);
            capacity_ = new_capacity;
        }
    }

    // Modifiers
    void clear() { size_ = 0; }
    void push_back(const Item& item) { data_[size_++] = item; }
    void pop_back() { --size_; }
    void resize(size_t new_size) { size_ = new_size; }

    void swap(dvec& other) {
        std::swap(data_, other.data_);
        std::swap(size_, other.size_);
        std::swap(capacity_, other.capacity_);
    }

    template<typename... Args>
    void emplace_back(Args&&... args) {
        new (data_.get() + size_) Item(std::forward<Args>(args)...);
        size_++;
    }

    std::unique_ptr<Item[]> release() {
        size_ = 0;
        capacity_ = 0;
        return data_.release();
    }

    std::tuple<std::unique_ptr<Item[]>, size_t, size_t> refresh(size_t new_capacity) {
        std::unique_ptr<Item[]> new_data = std::make_unique_for_overwrite<Item[]>(new_capacity);
        size_t sz = size_;
        size_t capa = capacity_;

        size_ = 0;
        capacity_ = new_capacity;
        new_data.swap(data_);

        return std::make_tuple(std::move(new_data), sz, capa);
    }

    std::tuple< std::unique_ptr<Item[]>, size_t, size_t> refresh() {
        return refresh(capacity_);
    }
};

template<typename Item>
class dvec_view {
private:
    Item* data_;
    size_t size_;
    size_t capacity_;

public:
    using value_type = Item;
    using size_type = size_t;
    using diffrence_type = std::ptrdiff_t;
    using reference = Item&;
    using const_reference = const Item&;
    using pointer = Item*;
    using const_pointer = const Item*;
    using iterator = Item*;
    using const_iterator = const Item*;
    using reverse_iterator = std::reverse_iterator<iterator>;
    using const_reverse_iterator = std::reverse_iterator<const_iterator>;

    dvec_view() noexcept: data_(nullptr), size_(0), capacity_(0) {}
    dvec_view(Item* data, size_t capacity, size_t size=0): data_(data), size_(size), capacity_(capacity) {}

    dvec_view(dvec_view&& other) = default;
    dvec_view(const dvec_view&) = default;

    dvec_view& operator=(dvec_view&& other) = default;
    dvec_view& operator=(const dvec_view&) = default;

    // Element access
    Item&       operator[] (size_t idx)         { return data_[idx]; }
    const Item& operator[] (size_t idx) const   { return data_[idx]; }
    Item&       front()         { return data_[0]; }
    const Item& front() const   { return data_[0]; }
    Item&       back()          { return data_[size_ - 1]; }
    const Item& back() const    { return data_[size_ - 1]; }
    Item*       data()          { return data_; }
    const Item* data() const    { return data_;}

    // Iterators
    Item*       begin()         { return data_; }
    Item*       end()           { return data_ + size_; }
    const Item* begin() const   { return data_; }
    const Item* end() const     { return data_ + size_; }
    const Item* cbegin() const  { return data_; }
    const Item* cend() const    { return data_ + size_; }
    std::reverse_iterator<Item*>      rbegin()        { return std::reverse_iterator<Item*>(end()); }
    std::reverse_iterator<Item*>      rend()          { return std::reverse_iterator<Item*>(begin()); }
    std::reverse_iterator<const Item*> rbegin() const  { return std::reverse_iterator<const Item*>(end()); }
    std::reverse_iterator<const Item*> rend() const    { return std::reverse_iterator<const Item*>(begin()); }
    std::reverse_iterator<const Item*> crbegin() const { return std::reverse_iterator<const Item*>(end()); }
    std::reverse_iterator<const Item*> crend() const   { return std::reverse_iterator<const Item*>(begin()); }

    // Capacity
    bool       empty() const   { return size_ == 0; }
    bool       full() const    { return size_ == capacity_; }
    size_t     size() const    { return size_; }
    size_t     capacity() const { return capacity_; }
    void reserve(size_t new_capacity) {
        if (new_capacity > capacity_) {
            std::unique_ptr<Item[]> new_data = std::make_unique_for_overwrite<Item[]>(new_capacity);
            std::copy(data_.get(), data_.get() + size_, new_data.get());
            data_ = std::move(new_data);
            capacity_ = new_capacity;
        }
    }

    // Modifiers
    void clear() { size_ = 0; }
    void push_back(const Item& item) { data_[size_++] = item; }
    void pop_back() { --size_; }
    void resize(size_t new_size) { size_ = new_size; }

    void swap(dvec_view& other) {
        std::swap(data_, other.data_);
        std::swap(size_, other.size_);
        std::swap(capacity_, other.capacity_);
    }

    template<typename... Args>
    void emplace_back(Args&&... args) {
        new (data_ + size_) Item(std::forward<Args>(args)...);
        size_++;
    }

    std::tuple<Item*, size_t, size_t> refresh(Item* new_data, size_t new_capacity, size_t new_size=0) {
        Item* old = data_;
        size_t sz = size_;
        size_t capa = capacity_;

        size_ = new_size;
        capacity_ = new_capacity;
        data_ = new_data;

        return std::make_tuple(old, sz, capa);
    }
};

}

#endif // DARRAY_H
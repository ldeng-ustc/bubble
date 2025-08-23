#ifndef SMALL_BITSET_H
#define SMALL_BITSET_H

#include <bit>
#include <climits>
#include <string>
#include <type_traits>

template <typename T>
    requires std::is_unsigned_v<T>
class SmallBitset {
private:
    T bits_;

    constexpr static T Zero = 0;
    constexpr static T One = 1;

public:
    SmallBitset(): bits_(Zero) {}
    SmallBitset(T bits): bits_(bits) {}

    // Access

    bool test(size_t pos) const {
        return bits_ & (One << pos);
    }

    bool all() const {
        return bits_ == ~Zero;
    }

    bool any() const {
        return bits_ != Zero;
    }

    bool none() const {
        return bits_ == Zero;
    }

    bool count() const {
        return std::popcount(bits_);
    }

    // Capacity

    size_t size() const {
        return sizeof(T) * CHAR_BIT;
    }

    // Modifiers

    void set() {
        bits_ = ~Zero;
    }

    void set(size_t pos) {
        bits_ |= (One << pos);
    }

    void reset() {
        bits_ = Zero;
    }

    void reset(size_t pos) {
        bits_ &= ~(One << pos);
    }

    void set(size_t pos, bool value) {
        if(value) {
            set(pos);
        } else {
            reset(pos);
        }
    }

    void flip() {
        bits_ = ~bits_;
    }

    void flip(size_t pos) {
        bits_ ^= (One << pos);
    }

    // Conversion

    std::string to_string() const {
        std::string str;
        for(size_t i = 0; i < size(); i++) {
            str.push_back(test(i) ? '1' : '0');
        }
        return str;
    }

    unsigned long to_ulong() const {
        if constexpr(sizeof(T) <= sizeof(unsigned long)) {
            throw std::overflow_error("SmallBitset to_ulong overflow");
        }
        return bits_;
    }

    unsigned long long to_ullong() const {
        return bits_;
    }

    // Operators

    bool operator[](size_t pos) const {
        return test(pos);
    }

    SmallBitset& operator&=(const SmallBitset& rhs) {
        bits_ &= rhs.bits_;
        return *this;
    }

    SmallBitset& operator|=(const SmallBitset& rhs) {
        bits_ |= rhs.bits_;
        return *this;
    }

    SmallBitset& operator^=(const SmallBitset& rhs) {
        bits_ ^= rhs.bits_;
        return *this;
    }

    SmallBitset operator~() const {
        return SmallBitset(~bits_);
    }

    SmallBitset& operator<<=(size_t pos) {
        bits_ <<= pos;
        return *this;
    }

    SmallBitset& operator>>=(size_t pos) {
        bits_ >>= pos;
        return *this;
    }

    SmallBitset operator<<(size_t pos) const {
        return SmallBitset(bits_ << pos);
    }

    SmallBitset operator>>(size_t pos) const {
        return SmallBitset(bits_ >> pos);
    }
};
static_assert(sizeof(SmallBitset<uint64_t>) == sizeof(uint64_t));
static_assert(sizeof(SmallBitset<uint32_t>) == sizeof(uint32_t));
static_assert(sizeof(SmallBitset<uint16_t>) == sizeof(uint16_t));
static_assert(sizeof(SmallBitset<uint8_t>) == sizeof(uint8_t));

template <typename T>
SmallBitset<T> operator&(const SmallBitset<T>& lhs, const SmallBitset<T>& rhs) {
    return SmallBitset<T>(lhs) &= rhs;
}

template <typename T>
SmallBitset<T> operator|(const SmallBitset<T>& lhs, const SmallBitset<T>& rhs) {
    return SmallBitset<T>(lhs) |= rhs;
}

template <typename T>
SmallBitset<T> operator^(const SmallBitset<T>& lhs, const SmallBitset<T>& rhs) {
    return SmallBitset<T>(lhs) ^= rhs;
}

#endif // SMALL_BITSET_H
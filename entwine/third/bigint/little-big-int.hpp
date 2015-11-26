#pragma once

#include <climits>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <ostream>
#include <string>
#include <utility>
#include <vector>

typedef unsigned long long Block;
static const std::size_t bitsPerBlock(CHAR_BIT * sizeof(Block));
static const std::size_t blockMax(std::numeric_limits<Block>::max());

// Adapted from https://howardhinnant.github.io/stack_alloc.html
template <std::size_t N>
class Arena
{
public:
    Arena() noexcept : m_ptr(m_buf) { }
    ~Arena() { m_ptr = nullptr; }

    char* allocate(std::size_t n)
    {
        if (m_buf + N - m_ptr >= n)
        {
            char* r = m_ptr;
            m_ptr += n;
            return r;
        }
        else
        {
            return static_cast<char*>(::operator new(n));
        }
    }

    void deallocate(char* p, std::size_t n) noexcept
    {
        if (inRange(p))
        {
            if (p + n == m_ptr) m_ptr = p;
        }
        else
        {
            ::operator delete(p);
        }
    }

    static constexpr std::size_t size() { return N; }
    std::size_t used() const { return static_cast<std::size_t>(m_ptr - m_buf); }
    void reset() { m_ptr = m_buf; }

private:
    Arena(const Arena&) = delete;
    Arena& operator=(const Arena&) = delete;

    bool inRange(char* p) const noexcept
    {
        return m_buf <= p && p <= m_buf + N;
    }

    static const std::size_t alignment = 16;
    alignas(alignment) char m_buf[N];
    char* m_ptr;
};

template <class T, std::size_t N>
class ShortAlloc
{
public:
    ShortAlloc(Arena<N>& a) noexcept : m_arena(a) { }

    template <class U>
    ShortAlloc(const ShortAlloc<U, N>& a) noexcept : m_arena(a.m_arena) { }
    ShortAlloc(const ShortAlloc&) = default;

    T* allocate(std::size_t n)
    {
        return reinterpret_cast<T*>(m_arena.allocate(n*sizeof(T)));
    }

    void deallocate(T* p, std::size_t n) noexcept
    {
        m_arena.deallocate(reinterpret_cast<char*>(p), n*sizeof(T));
    }

    template <class T1, std::size_t N1, class U, std::size_t M>
    friend bool operator==(
            const ShortAlloc<T1, N1>& x,
            const ShortAlloc<U, M>& y) noexcept;

    template <class U, std::size_t M> friend class ShortAlloc;

    typedef T value_type;
    template <class _Up> struct rebind { typedef ShortAlloc<_Up, N> other; };

private:
    ShortAlloc& operator=(const ShortAlloc&) = delete;

    Arena<N>& m_arena;
};

class BigUint
{
public:
    BigUint();
    BigUint(unsigned long long val);
    explicit BigUint(const std::string& val);

    BigUint(const BigUint& other);
    BigUint& operator=(const BigUint& other);

    // True if this object represents zero.
    bool zero() const { return trivial() && !m_val.front(); }

    // True if this object is of size one, so simple integer math may be used
    // for some operations.
    bool trivial() const { return m_val.size() == 1; }
    std::size_t blockSize() const { return m_val.size(); }

    std::string str() const;    // Get base-10 representation.
    std::string bin() const;    // Get binary representation.

    // Get as an unsigned long long.  Throws std::overflow_error if !trivial().
    unsigned long long getSimple() const;

    // If both the quotient and remainder are required, using the result of
    // this function is more efficient than calling both / and % operators
    // separately.
    //
    // Return value:
    //      result.first - quotient
    //      result.second - remainder
    std::pair<BigUint, BigUint> divMod(const BigUint& denominator) const;

    // Increments the least-significant block without carrying any possible
    // roll-over into subsequent blocks.  Can be used to increment efficiently
    // only if the caller can guarantee that no carry will be produced by this
    // increment.  For example, after a positive bit-shift left.
    void incSimple() { ++m_val.front(); }

    static const unsigned int N = 8;
    typedef ShortAlloc<Block, N> Alloc;
    typedef std::vector<Block, Alloc> Data;

    // Get raw blocks.  For the non-const version, if the result is modified,
    // all future operations may be incorrect.
    Data& val() { return m_val; }
    const Data& val() const { return m_val; }

    // These ones need access to private members.
    friend BigUint& operator*=(BigUint& lhs, const BigUint& rhs);
    friend BigUint& operator<<=(BigUint& lhs, Block rhs);
    friend bool operator<(const BigUint& lhs, const BigUint& rhs);
    friend BigUint operator<<(const BigUint&, Block);

private:
    BigUint(std::vector<Block> blocks);

    // Equivalent to:
    //      *this += (other << shift)
    //
    // ...without the copy overhead of performing the shift in advance.
    void add(const BigUint& other, Block shift);

    Arena<N> m_arena;
    Data m_val;
};

// Assignment.
BigUint& operator+=(BigUint& lhs, const BigUint& rhs);
BigUint& operator-=(BigUint& lhs, const BigUint& rhs);
BigUint& operator*=(BigUint& lhs, const BigUint& rhs);
BigUint& operator/=(BigUint& lhs, const BigUint& rhs);
BigUint& operator%=(BigUint& lhs, const BigUint& rhs);

BigUint& operator&=(BigUint& lhs, const BigUint& rhs);
BigUint& operator|=(BigUint& lhs, const BigUint& rhs);
BigUint& operator<<=(BigUint& lhs, Block rhs);
BigUint& operator>>=(BigUint& lhs, Block rhs);

// Copying.
inline BigUint operator+(const BigUint& lhs, const BigUint& rhs)
{
    BigUint result(lhs); result += rhs; return result;
}

inline BigUint operator-(const BigUint& lhs, const BigUint& rhs)
{
    BigUint result(lhs); result -= rhs; return result;
}

inline BigUint operator*(const BigUint& lhs, const BigUint& rhs)
{
    BigUint result(lhs); result *= rhs; return result;
}

inline BigUint operator/(const BigUint& lhs, const BigUint& rhs)
{
    BigUint result(lhs); result /= rhs; return result;
}

inline BigUint operator%(const BigUint& lhs, const BigUint& rhs)
{
    BigUint result(lhs); result %= rhs; return result;
}

inline BigUint operator|(const BigUint& lhs, const BigUint& rhs)
{
    BigUint result(lhs); result |= rhs; return result;
}

BigUint operator&(const BigUint& lhs, const BigUint& rhs);
BigUint operator<<(const BigUint& lhs, Block rhs);
BigUint operator>>(const BigUint& lhs, Block rhs);

// Pre/post-fixes.  These return *this or a copy, as expected.
BigUint& operator--(BigUint& lhs);      // Pre-decrement.
BigUint  operator--(BigUint& lhs, int); // Post-decrement.
BigUint& operator++(BigUint& lhs);      // Pre-increment.
BigUint  operator++(BigUint& lhs, int); // Post-increment.

// Comparisons.
bool operator==(const BigUint& lhs, const BigUint& rhs);
bool operator!=(const BigUint& lhs, const BigUint& rhs);
bool operator< (const BigUint& lhs, const BigUint& rhs);
bool operator<=(const BigUint& lhs, const BigUint& rhs);
bool operator> (const BigUint& lhs, const BigUint& rhs);
bool operator>=(const BigUint& lhs, const BigUint& rhs);

inline bool operator!(const BigUint& val) { return val.zero(); }
std::ostream& operator<<(std::ostream& out, const BigUint& val);

Block log2(const BigUint& val);
BigUint sqrt(const BigUint& in);

namespace std
{

template<> struct hash<BigUint>
{
    // Based on the public domain Murmur hash, by Austin Appleby.
    // https://sites.google.com/site/murmurhash/
    std::size_t operator()(const BigUint& big) const
    {
        const Block seed(0xc70f6907ULL);
        const Block m(0xc6a4a7935bd1e995ULL);
        const Block r(47);

        const auto& val(big.val());

        Block h(seed ^ (val.size() * sizeof(Block) * m));

        const Block* cur(val.data());
        const Block* end(val.data() + val.size());

        Block k(0);

        while (cur != end)
        {
            k = *cur++;

            k *= m;
            k ^= k >> r;
            k *= m;

            h ^= k;
            h *= m;
        }

        h ^= h >> r;
        h *= m;
        h ^= h >> r;

        return h;
    }
};

} // namespace std

template <class T, std::size_t N, class U, std::size_t M>
inline bool operator==(
        const ShortAlloc<T, N>& x,
        const ShortAlloc<U, M>& y) noexcept
{
    return N == M && &x.m_arena == &y.m_arena;
}

template <class T, std::size_t N, class U, std::size_t M>
inline bool operator!=(
        const ShortAlloc<T, N>& x,
        const ShortAlloc<U, M>& y) noexcept
{
    return !(x == y);
}


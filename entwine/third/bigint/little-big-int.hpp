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

/*
// Adapted from https://howardhinnant.github.io/stack_alloc.html

template<std::size_t N>
class ShortStack
{
public:
    ShortStack() noexcept : m_pos(m_buffer) { }
    ~ShortStack() { m_pos = 0; }

    char* allocate(std::size_t n);
    void deallocate(char* p, std::size_t n) noexcept;

    static constexpr std::size_t size() { return N; }

    std::size_t used() const
    {
        return static_cast<std::size_t>(m_pos - m_buffer);
    }

    void reset()
    {
        m_pos = m_buffer;
    }

private:
    static const std::size_t alignment = 16;
    alignas(alignment) char m_buffer[N];
    char* m_pos;

    bool contained(const char* p) noexcept
    {
        return m_buffer <= p && p <= m_buffer + N;
    }

    ShortStack(const ShortStack&) = delete;
    ShortStack& operator=(const ShortStack&) = delete;
};

template<std::size_t N>
char* ShortStack<N>::allocate(const std::size_t n)
{
    if (m_pos + n <= m_buffer + N)
    {
        char* result = m_pos;
        m_pos += n;
        return result;
    }
    else
    {
        return static_cast<char*>(::operator new(n));
    }
}

template<std::size_t N>
void ShortStack<N>::deallocate(char* p, const std::size_t n) noexcept
{
    if (contained(p))
    {
        if (p + n == m_pos) m_pos = p;
    }
    else
    {
        ::operator delete(p);
    }
}

template<class T, std::size_t N>
class StackAlloc
{
public:
    StackAlloc(ShortStack<N>& ss) noexcept
        : m_ss(ss)
    { }

    template<class U>
    StackAlloc(const StackAlloc<U, N>& other) noexcept
        : m_ss(other.m_ss)
    { }

    T* allocate(std::size_t n)
    {
        return reinterpret_cast<T*>(m_ss.allocate(n * sizeof(T)));
    }

    void deallocate(T* p, std::size_t n) noexcept
    {
        m_ss.deallocate(reinterpret_cast<char*>(p), n * sizeof(T));
    }

    template<class T1, std::size_t N1, class T2, std::size_t N2> friend
    bool operator==(
            const StackAlloc<T1, N1>& lhs,
            const StackAlloc<T2, N2>& rhs) noexcept;

    template<class U> struct rebind { typedef StackAlloc<U, N> other; };
    template<class U, std::size_t M> friend class StackAlloc;

    typedef T value_type;

private:
    ShortStack<N>& m_ss;

    StackAlloc(const StackAlloc&) = delete;
    StackAlloc& operator=(const StackAlloc&) = delete;
};

template<class T1, std::size_t N1, class T2, std::size_t N2>
inline bool operator==(
        const StackAlloc<T1, N1>& lhs,
        const StackAlloc<T2, N2>& rhs) noexcept
{
    return (N1 == N2) && (&lhs.m_ss == &rhs.m_ss);
}

template<class T1, std::size_t N1, class T2, std::size_t N2>
inline bool operator!=(
        const StackAlloc<T1, N1>& lhs,
        const StackAlloc<T2, N2>& rhs) noexcept
{
    return !(lhs == rhs);
}
*/








#include <cassert>

template <std::size_t N>
class arena
{
    static const std::size_t alignment = 16;
    alignas(alignment) char buf_[N];
    char* ptr_;

    bool
    pointer_in_buffer(char* p) noexcept
        {return buf_ <= p && p <= buf_ + N;}

public:
    arena() noexcept : ptr_(buf_) {}
    ~arena() {ptr_ = nullptr;}
    arena(const arena&) = delete;
    arena& operator=(const arena&) = delete;

    char* allocate(std::size_t n);
    void deallocate(char* p, std::size_t n) noexcept;

    static constexpr std::size_t size() {return N;}
    std::size_t used() const {return static_cast<std::size_t>(ptr_ - buf_);}
    void reset() {ptr_ = buf_;}
};

template <std::size_t N>
char*
arena<N>::allocate(std::size_t n)
{
    assert(pointer_in_buffer(ptr_) && "short_alloc has outlived arena");
    if (buf_ + N - ptr_ >= n)
    {
        char* r = ptr_;
        ptr_ += n;
        return r;
    }
    return static_cast<char*>(::operator new(n));
}

template <std::size_t N>
void
arena<N>::deallocate(char* p, std::size_t n) noexcept
{
    assert(pointer_in_buffer(ptr_) && "short_alloc has outlived arena");
    if (pointer_in_buffer(p))
    {
        if (p + n == ptr_)
            ptr_ = p;
    }
    else
        ::operator delete(p);
}

template <class T, std::size_t N>
class short_alloc
{
    arena<N>& a_;
public:
    typedef T value_type;

public:
    template <class _Up> struct rebind {typedef short_alloc<_Up, N> other;};

    short_alloc(arena<N>& a) noexcept : a_(a) {}
    template <class U>
        short_alloc(const short_alloc<U, N>& a) noexcept
            : a_(a.a_) {}
    short_alloc(const short_alloc&) = default;
    short_alloc& operator=(const short_alloc&) = delete;

    T* allocate(std::size_t n)
    {
        return reinterpret_cast<T*>(a_.allocate(n*sizeof(T)));
    }
    void deallocate(T* p, std::size_t n) noexcept
    {
        a_.deallocate(reinterpret_cast<char*>(p), n*sizeof(T));
    }

    template <class T1, std::size_t N1, class U, std::size_t M>
    friend
    bool
    operator==(const short_alloc<T1, N1>& x, const short_alloc<U, M>& y) noexcept;

    template <class U, std::size_t M> friend class short_alloc;
};

template <class T, std::size_t N, class U, std::size_t M>
inline
bool
operator==(const short_alloc<T, N>& x, const short_alloc<U, M>& y) noexcept
{
    return N == M && &x.a_ == &y.a_;
}

template <class T, std::size_t N, class U, std::size_t M>
inline
bool
operator!=(const short_alloc<T, N>& x, const short_alloc<U, M>& y) noexcept
{
    return !(x == y);
}











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
    /*
    typedef StackAlloc<Block, N> Alloc;
    typedef std::vector<Block, Alloc> Data;

    bool test()
    {
        ShortStack<N> arena;
        Data v{Alloc(arena)};
        v.push_back(1);
        return v.empty();
    }
    */

    /*
    const unsigned N = 200;
    typedef short_alloc<int, N> Alloc;
    typedef std::vector<int, Alloc> SmallVector;
    arena<N> a;
    SmallVector v{Alloc(a)};
    */
    typedef short_alloc<Block, N> Alloc;
    typedef std::vector<Block, Alloc> Data;

    bool hin()
    {
        arena<N> a;
        Data v{Alloc(a)};
        v.push_back(1);
        return v.empty();
    }

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

    arena<N> m_shortStack;
    Data m_val;
    /*
    ShortStack<N> m_shortStack;
    Data m_val;
    */
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

}


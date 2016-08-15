/******************************************************************************
* The MIT License (MIT)
*
* Copyright (c) 2015 Connor Manning
*
* Permission is hereby granted, free of charge, to any person obtaining a copy
* of this software and associated documentation files (the "Software"), to deal
* in the Software without restriction, including without limitation the rights
* to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
* copies of the Software, and to permit persons to whom the Software is
* furnished to do so, subject to the following conditions:
*
* The above copyright notice and this permission notice shall be included in all
* copies or substantial portions of the Software.
*
* THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
* IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
* FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
* AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
* LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
* OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
* SOFTWARE.
******************************************************************************/

#pragma once

#include <cassert>
#include <climits>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <ostream>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

/******************************************************************************
* The stack allocator (classes Arena and ShortAlloc) is adapted from
*       https://howardhinnant.github.io/short_alloc.h
*
* These classes are also licensed as MIT, reproduced below:
*
* The MIT License (MIT)
*
* Copyright (c) 2015 Howard Hinnant
*
* Permission is hereby granted, free of charge, to any person obtaining a copy
* of this software and associated documentation files (the "Software"), to deal
* in the Software without restriction, including without limitation the rights
* to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
* copies of the Software, and to permit persons to whom the Software is
* furnished to do so, subject to the following conditions:
*
* The above copyright notice and this permission notice shall be included in all
* copies or substantial portions of the Software.
*
* THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
* IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
* FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
* AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
* LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
* OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
* SOFTWARE.
******************************************************************************/

// Adapted from https://howardhinnant.github.io/stack_alloc.html
template <std::size_t N, std::size_t A = alignof(std::max_align_t)>
class Arena
{
public:
    Arena() noexcept : m_buf(), m_ptr(m_buf)  { }

    Arena(const Arena&) = delete;
    Arena& operator=(const Arena&) = delete;

    char* allocate(std::size_t n)
    {
        n = alignUp(n);

        if (m_ptr + n <= end())
        {
            char* r(m_ptr);
            m_ptr += n;
            return r;
        }
        else
        {
            static_assert(
                    A <= alignof(std::max_align_t),
                    "Operator new cannot guarantee the selected alignment");

            return static_cast<char*>(::operator new(n));
        }
    }

    void deallocate(char* p, std::size_t n) noexcept
    {
        if (stacked(p))
        {
            if (p + n == m_ptr) m_ptr = p;
        }
        else
        {
            ::operator delete(p);
        }
    }

    static constexpr std::size_t size() noexcept { return N; }
    void reset() noexcept { m_ptr = m_buf; }

    std::size_t used() const noexcept
    {
        return static_cast<std::size_t>(m_ptr - m_buf);
    }

private:
    bool stacked(char* p) const noexcept
    {
        return p >= m_buf && p <= end();
    }

    const char* const end() const noexcept
    {
        return m_buf + N;
    }

    static std::size_t alignUp(std::size_t n) noexcept
    {
        return (n + A - 1) & ~(A - 1);
    }

    alignas(A) char m_buf[N];
    char* m_ptr;
};

template <class T, std::size_t N, std::size_t A = alignof(std::max_align_t)>
class ShortAlloc
{
public:
    using value_type = T;
    using ArenaType = Arena<N, A>;

    ShortAlloc(ArenaType& a) noexcept
        : m_arena(a)
    {
        static_assert(N % A == 0, "Invalid size for this alignment");
    }

    template <class U>
    ShortAlloc(const ShortAlloc<U, N, A>& a) noexcept
        : m_arena(a.m_arena)
    { }

    ShortAlloc(const ShortAlloc&) = default;
    ShortAlloc& operator=(const ShortAlloc&) = delete;

    template <class V> struct rebind { using other = ShortAlloc<V, N, A>; };

    T* allocate(std::size_t n)
    {
        return reinterpret_cast<T*>(m_arena.allocate(n * sizeof(T)));
    }

    void deallocate(T* p, std::size_t n) noexcept
    {
        m_arena.deallocate(reinterpret_cast<char*>(p), n * sizeof(T));
    }

    template <class T1, std::size_t N1, class U, std::size_t M>
    friend bool operator==(
            const ShortAlloc<T1, N1>& x,
            const ShortAlloc<U, M>& y) noexcept;

private:
    ArenaType& m_arena;
};

class BigUint
{
public:
    using Block = unsigned long long;
    static constexpr std::size_t bitsPerBlock = CHAR_BIT * sizeof(Block);
    static const std::size_t blockMax;

    BigUint() : m_arena(), m_val(1, 0, Alloc(m_arena)) { }
    BigUint(const Block val) : m_arena(), m_val(1, val, Alloc(m_arena)) { }
    explicit BigUint(const std::string& val);

    BigUint(const Block* begin, const Block* end)
        : m_arena()
        , m_val(begin, end, Alloc(m_arena))
    { }

    BigUint(const BigUint& other)
        : m_arena()
        , m_val(other.m_val, Alloc(m_arena))
    { }

    BigUint& operator=(const BigUint& other)
    {
        m_val.assign(other.m_val.begin(), other.m_val.end());
        return *this;
    }

    ~BigUint() { }

    // True if this object represents zero.
    bool zero() const { return trivial() && !m_val.front(); }
    explicit operator bool() const { return !zero(); }

    // True if this object is of size one, so simple integer math may be used
    // for some operations.
    bool trivial() const { return m_val.size() == 1; }
    std::size_t blockSize() const { return m_val.size(); }

    std::string str() const;    // Get base-10 representation.
    std::string bin() const;    // Get binary representation.

    // Get as an unsigned long long.  Throws std::overflow_error if !trivial().
    unsigned long long getSimple() const
    {
        if (m_val.size() == 1) return m_val.front();
        throw std::overflow_error("This BigUint is too large to get as long.");
    }

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

    static const unsigned int N = sizeof(Block);
    static const unsigned int A = alignof(Block);

    using Alloc = ShortAlloc<Block, N, A>;
    using Data = std::vector<Block, Alloc>;

    // Get raw blocks.  For the non-const version, if the result is modified,
    // all future operations may be incorrect.
    Data& data() { return m_val; }
    const Data& data() const { return m_val; }

    // These ones need access to private members.
    friend BigUint& operator*=(BigUint& lhs, const BigUint& rhs);
    friend BigUint& operator<<=(BigUint& lhs, Block rhs);
    friend bool operator<(const BigUint& lhs, const BigUint& rhs);
    friend BigUint operator<<(const BigUint&, Block);

    static Block log2(const BigUint& val);
    static BigUint sqrt(const BigUint& in);

private:
    BigUint(std::vector<Block> blocks)
        : m_arena()
        , m_val(blocks.begin(), blocks.end(), Alloc(m_arena))
    {
        if (m_val.empty()) m_val.push_back(0);
    }

    // Equivalent to:
    //      *this += (other << shift)
    //
    // ...without the copy overhead of performing the shift in advance.
    void add(const BigUint& other, Block shift);

    Arena<N, A> m_arena;
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
BigUint& operator<<=(BigUint& lhs, BigUint::Block rhs);
BigUint& operator>>=(BigUint& lhs, BigUint::Block rhs);

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
BigUint operator<<(const BigUint& lhs, BigUint::Block rhs);
BigUint operator>>(const BigUint& lhs, BigUint::Block rhs);

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

BigUint::Block log2(const BigUint& val);
BigUint sqrt(const BigUint& in);

namespace std
{

template<> struct hash<BigUint>
{
    // Based on the public domain Murmur hash, by Austin Appleby.
    // https://sites.google.com/site/murmurhash/
    std::size_t operator()(const BigUint& big) const
    {
        const BigUint::Block seed(0xc70f6907ULL);
        const BigUint::Block m(0xc6a4a7935bd1e995ULL);
        const BigUint::Block r(47);

        const auto& val(big.data());

        BigUint::Block h(seed ^ (val.size() * sizeof(BigUint::Block) * m));

        const BigUint::Block* cur(val.data());
        const BigUint::Block* end(val.data() + val.size());

        BigUint::Block k(0);

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


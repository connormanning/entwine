#include "little-big-int.hpp"

#include <algorithm>
#include <bitset>
#include <cassert>
#include <cmath>
#include <deque>
#include <iostream>
#include <sstream>

BigUint::BigUint()
    : m_val{1, 0, Alloc(m_shortStack)}
{ }

BigUint::BigUint(const Block val)
    : m_val{1, val, Alloc(m_shortStack)}
{ }

BigUint::BigUint(std::vector<Block> blocks)
    : m_val(Alloc(m_shortStack))
{
    m_val.insert(m_val.end(), blocks.begin(), blocks.end());
    if (m_val.empty()) m_val.push_back(0);
}

BigUint::BigUint(const std::string& str)
    : m_val(Alloc(m_shortStack))
{
    m_val.push_back(0);
    BigUint factor(1);
    const std::size_t size(str.size());

    for (std::size_t i(size - 8); i < size; i -= 8)
    {
        *this += BigUint(std::stoull(str.substr(i, 8)) * factor);
        factor *= 100000000;
    }

    if (size % 8)
    {
        *this += BigUint(std::stoull(str.substr(0, size % 8)) * factor);
    }
}

BigUint::BigUint(const BigUint& other)
    : m_val(other.m_val, Alloc(m_shortStack))
{ }

BigUint& BigUint::operator=(const BigUint& other)
{
    m_val = other.m_val;
    return *this;
}

std::string BigUint::str() const
{
    if (trivial())
    {
        return std::to_string(m_val.front());
    }
    else
    {
        // Guess the number of digits with an approximation of log10(*this).
        std::deque<char> digits(log2(*this) * 1000 / 3322 + 1, 48);
        BigUint factor(10);
        BigUint lagged(1);

        std::pair<BigUint, BigUint> current;

        std::size_t i(0);

        do
        {
            current = divMod(factor);

            digits.at(i++) = 48 + (current.second / lagged).getSimple();

            lagged = factor;
            factor *= 10;
        }
        while (!current.first.zero() && i < digits.size());

        while (!current.first.zero())
        {
            current = divMod(factor);

            digits.push_back(48 + (current.second / lagged).getSimple());

            lagged = factor;
            factor *= 10;
        }

        while (digits.back() == 48) digits.pop_back();

        return std::string(digits.rbegin(), digits.rend());
    }
}

std::string BigUint::bin() const
{
    std::ostringstream stream;
    const std::size_t size(m_val.size());

    stream << "0b";

    for (std::size_t i(0); i < size; ++i)
    {
        stream << std::bitset<bitsPerBlock>(m_val[size - i - 1]);
    }

    return stream.str();
}

unsigned long long BigUint::getSimple() const
{
    if (m_val.size() == 1)
    {
        return m_val[0];
    }
    else
    {
        throw std::overflow_error("This BigUint is too large to get as long.");
    }
}

void BigUint::add(const BigUint& rhs, const Block shift)
{
    auto& rhsVal(rhs.val());

    const std::size_t shiftBlocks(shift / bitsPerBlock);
    const std::size_t shiftBits(shift % bitsPerBlock);
    const std::size_t shiftBack(shiftBits ? (bitsPerBlock - shiftBits) : 0);

    const std::size_t rhsSize(rhsVal.size());
    const std::size_t lhsSize(m_val.size());

    bool carry(false);
    Block rhsCur(0);

    {
        if (shiftBlocks >= m_val.size()) m_val.resize(shiftBlocks + 1, 0);
        Block& lhsCur(m_val.at(shiftBlocks));

        rhsCur = rhsVal.front() << shiftBits;
        lhsCur += rhsCur;

        carry = lhsCur < rhsCur;
    }

    std::size_t i;
    for (i = 1; i < rhsSize; ++i)
    {
        if (shiftBlocks + i == m_val.size()) m_val.push_back(0);
        Block& lhsCur(m_val.at(shiftBlocks + i));

        rhsCur =
            (rhsVal.at(i) << shiftBits) |
            (shiftBack ? rhsVal[i - 1] >> shiftBack : 0);

        lhsCur += rhsCur + static_cast<Block>(carry);
        carry = lhsCur < rhsCur || (carry && lhsCur == rhsCur);
    }

    {
        rhsCur = (shiftBack ? rhsVal.back() >> shiftBack : 0);

        if (rhsCur || carry)
        {
            if (shiftBlocks + i == m_val.size()) m_val.push_back(0);

            Block& lhsCur(m_val[shiftBlocks + i]);
            lhsCur += rhsCur + static_cast<Block>(carry);
            carry = lhsCur < rhsCur || (carry && lhsCur == rhsCur);
            ++i;
        }
    }

    while (carry && (shiftBlocks + i < lhsSize))
    {
        carry = (++m_val[i] == 0);
        ++i;
    }

    if (carry) m_val.push_back(1);
}

std::pair<BigUint, BigUint> BigUint::divMod(const BigUint& d) const
{
    const auto& dVal(d.val());

    if (d.zero()) throw std::invalid_argument("Cannot divide by zero");

    if (trivial() && d.trivial())
    {
        return std::make_pair(
                BigUint(m_val.front() / dVal.front()),
                BigUint(m_val.front() % dVal.front()));
    }
    else if (*this < d)
    {
        return std::make_pair(BigUint(0), *this);
    }
    else
    {
        std::pair<BigUint, BigUint> result;
        auto& q(result.first);
        auto& r(result.second);

        const std::size_t nValSize(m_val.size());

        // Don't presize the quotient here, we can't have leading zero blocks
        // since it can cause errors in our other operators.
        auto& qVal(q.val());
        Block mask(0);

        for (std::size_t block(nValSize - 1); block < nValSize; --block)
        {
            for (std::size_t bit(bitsPerBlock - 1); bit < bitsPerBlock; --bit)
            {
                r <<= 1;
                mask = Block(1) << bit;

                if (m_val.at(block) & mask) r.incSimple();

                if (r >= d)
                {
                    r -= d;

                    if (block >= qVal.size()) qVal.resize(block + 1, 0);
                    qVal.at(block) |= mask;
                }
            }
        }

        while (!qVal.back()) qVal.pop_back();

        return result;
    }
}

BigUint& operator+=(BigUint& lhs, const BigUint& rhs)
{
    auto& lhsVal(lhs.val());
    auto& rhsVal(rhs.val());

    auto& lhsFront(lhsVal.front());
    const Block rhsFront(rhsVal.front());

    if (lhs.trivial() && rhs.trivial() && lhsFront + rhsFront >= lhsFront)
    {
        lhsFront += rhsFront;
    }
    else
    {
        const std::size_t rhsSize(rhsVal.size());
        lhsVal.resize(std::max(lhsVal.size(), rhsSize), 0);
        const std::size_t lhsSize(lhsVal.size());

        bool carry(false);
        std::size_t i(0);

        while (i < rhsSize)
        {
            Block& lhsCur(lhsVal[i]);
            const Block& rhsCur(rhsVal[i]);

            lhsCur += rhsCur + static_cast<Block>(carry);
            carry = lhsCur < rhsCur || (carry && lhsCur == rhsCur);

            ++i;
        }

        while (carry && i < lhsSize)
        {
            carry = (++lhsVal[i] == 0);
            ++i;
        }

        if (carry) lhsVal.push_back(1);
    }

    return lhs;
}

BigUint& operator-=(BigUint& lhs, const BigUint& rhs)
{
    auto& lhsVal(lhs.val());
    const auto& rhsVal(rhs.val());

    if (lhs.trivial() && rhs.trivial())
    {
        if (lhsVal.front() >= rhsVal.front())
        {
            lhsVal.front() -= rhsVal.front();
            return lhs;
        }

        throw std::underflow_error(
                "Subtraction result was negative (block zero)");
    }

    const std::size_t rhsSize(rhsVal.size());
    const std::size_t lhsSize(lhsVal.size());

    if (lhsSize < rhsSize)
    {
        throw std::underflow_error(
                "Subtraction result was negative (block size)");
    }
    else
    {
        Block old(0);
        bool borrow(false);
        std::size_t i(0);

        while (i < rhsSize)
        {
            Block& lhsCur(lhsVal[i]);
            old = lhsCur;
            const Block& rhsCur(rhsVal[i]);

            lhsCur -= rhsCur + static_cast<Block>(borrow);
            borrow = lhsCur > old || (borrow && lhsCur == old);

            ++i;
        }

        while (borrow && i < lhsSize)
        {
            borrow = (lhsVal[i] == 0);
            --lhsVal[i];
            ++i;
        }

        if (borrow)
        {
            throw std::underflow_error(
                    "Subtraction result was negative (borrow out)");
        }

        while (lhsVal.size() != 1 && lhsVal.back() == 0) lhsVal.pop_back();
    }

    return lhs;
}

BigUint& operator*=(BigUint& lhs, const BigUint& rhs)
{
    if (lhs.zero() || rhs.zero())
    {
        lhs = 0;
    }
    else if (log2(lhs) + log2(rhs) + 1 <= bitsPerBlock)
    {
        lhs = lhs.val().front() * rhs.val().front();
    }
    else
    {
        BigUint out;
        auto& rhsVal(rhs.val());

        for (std::size_t block(0); block < rhsVal.size(); ++block)
        {
            for (std::size_t bit(0); bit < bitsPerBlock; ++bit)
            {
                if ((rhsVal.at(block) >> bit) & 1)
                {
                    out.add(lhs, block * bitsPerBlock + bit);
                }
            }
        }

        lhs = out;
    }

    return lhs;
}

BigUint& operator/=(BigUint& n, const BigUint& d)
{
    const auto div(n.divMod(d));
    n = div.first;
    return n;
}

BigUint& operator%=(BigUint& n, const BigUint& d)
{
    const auto div(n.divMod(d));
    n = div.second;
    return n;
}

BigUint& operator&=(BigUint& lhs, const BigUint& rhs)
{
    auto& lhsVal(lhs.val());
    auto& rhsVal(rhs.val());

    lhsVal.resize(std::min(lhsVal.size(), rhsVal.size()));

    for (std::size_t i(0); i < lhsVal.size(); ++i)
    {
        lhsVal[i] &= rhsVal[i];
    }

    while (!lhs.zero() && !lhsVal.back()) lhsVal.pop_back();

    return lhs;
}

BigUint& operator|=(BigUint& lhs, const BigUint& rhs)
{
    auto& lhsVal(lhs.val());
    auto& rhsVal(rhs.val());

    const std::size_t rhsSize(rhsVal.size());

    lhsVal.resize(std::max(lhsVal.size(), rhsSize), 0);

    for (std::size_t i(0); i < rhsSize; ++i)
    {
        lhsVal[i] |= rhsVal[i];
    }

    return lhs;
}

BigUint& operator<<=(BigUint& lhs, Block rhs)
{
    if (
            lhs.trivial() &&
            (lhs.m_val.front() & (blockMax << (bitsPerBlock - rhs))) == 0)
    {
        lhs.m_val.front() <<= rhs;
        return lhs;
    }

    const std::size_t startBlocks(lhs.blockSize());
    const std::size_t shiftBlocks(rhs / bitsPerBlock);
    const std::size_t shiftBits(rhs % bitsPerBlock);
    const std::size_t shiftBack(shiftBits ? (bitsPerBlock - shiftBits) : 0);

    auto& val(lhs.val());

    {
        Block carry(shiftBack ? val.back() >> shiftBack : 0);
        val.resize(startBlocks + shiftBlocks + (carry ? 1 : 0), 0);
        if (carry) val.back() = carry;
    }

    for (std::size_t i(startBlocks - 1); i < startBlocks; --i)
    {
        val[i + shiftBlocks] =
            (shiftBack && i ? val[i - 1] >> shiftBack : 0) |
            (val[i] << shiftBits);
    }

    for (std::size_t i(0); i < shiftBlocks; ++i)
    {
        val[i] = 0;
    }

    return lhs;
}

BigUint& operator>>=(BigUint& lhs, Block rhs)
{
    const std::size_t startBlocks(lhs.blockSize());
    const std::size_t shiftBlocks(rhs / bitsPerBlock);
    const std::size_t shiftBits(rhs % bitsPerBlock);
    const std::size_t shiftBack(shiftBits ? (bitsPerBlock - shiftBits) : 0);

    auto& val(lhs.val());

    for (std::size_t i(shiftBlocks); i < startBlocks - 1; ++i)
    {
        val[i - shiftBlocks] =
            (shiftBack ? val[i + 1] << shiftBack : 0) |
            (val[i] >> shiftBits);
    }

    Block last(val.back() >> shiftBits);
    val.resize(startBlocks - shiftBlocks - (last ? 0 : 1));
    if (val.empty()) val.push_back(0);
    else if (last) val.back() = last;

    return lhs;
}

BigUint operator&(const BigUint& lhs, const BigUint& rhs)
{
    BigUint result;
    if (lhs.val().size() < rhs.val().size())
    {
        result = lhs;
        result &= rhs;
    }
    else
    {
        result = rhs;
        result &= lhs;
    }
    return result;
}

BigUint operator<<(const BigUint& lhs, const Block rhs)
{
    const std::size_t startBlocks(lhs.blockSize());
    const std::size_t shiftBlocks(rhs / bitsPerBlock);
    const std::size_t shiftBits(rhs % bitsPerBlock);

    BigUint result(std::vector<Block>(startBlocks + shiftBlocks, 0));

    const auto& start(lhs.val());
    auto& val(result.val());

    Block carry(0);

    for (std::size_t i(0); i < startBlocks; ++i)
    {
        val[i + shiftBlocks] = (start[i] << shiftBits) | carry;
        carry = shiftBits ? (start[i] >> (bitsPerBlock - shiftBits)) : 0;
    }

    if (carry) val.push_back(carry);

    return result;
}

BigUint operator>>(const BigUint& lhs, const Block rhs)
{
    // TODO Can be optimized like the above.
    BigUint result(lhs);
    result >>= rhs;
    return result;
}

BigUint& operator--(BigUint& lhs)
{
    lhs -= 1;
    return lhs;
}

BigUint operator--(BigUint& lhs, int)
{
    BigUint copy(lhs);
    lhs -= 1;
    return copy;
}

BigUint& operator++(BigUint& lhs)
{
    lhs += 1;
    return lhs;
}

BigUint operator++(BigUint& lhs, int)
{
    BigUint copy(lhs);
    lhs += 1;
    return copy;
}

bool operator==(const BigUint& lhs, const BigUint& rhs)
{
    const auto& lhsVal(lhs.val());
    const auto& rhsVal(rhs.val());

    const std::size_t size(lhsVal.size());

    if (size != rhsVal.size()) return false;

    for (std::size_t i(0); i < size; ++i)
    {
        if (lhsVal[i] != rhsVal[i]) return false;
    }

    return true;
}

bool operator!=(const BigUint& lhs, const BigUint& rhs)
{
    return !(lhs == rhs);
}

bool operator<(const BigUint& lhs, const BigUint& rhs)
{
    if (lhs.trivial())
    {
        if (rhs.trivial()) return lhs.m_val.front() < rhs.m_val.front();
        else return false;
    }

    const auto& lhsVal(lhs.val());
    const auto& rhsVal(rhs.val());

    const std::size_t lhsSize(lhsVal.size());
    const std::size_t rhsSize(rhsVal.size());

    if      (lhsSize < rhsSize) return true;
    else if (lhsSize > rhsSize) return false;
    else
    {
        for (std::size_t i(lhsSize - 1); i < lhsSize; --i)
        {
            if      (lhsVal[i] < rhsVal[i]) return true;
            else if (lhsVal[i] > rhsVal[i]) return false;
        }

        return false;
    }
}

bool operator<=(const BigUint& lhs, const BigUint& rhs)
{
    return !(rhs < lhs);
}

bool operator>(const BigUint& lhs, const BigUint& rhs)
{
    return rhs < lhs;
}

bool operator>=(const BigUint& lhs, const BigUint& rhs)
{
    return !(lhs < rhs);
}

std::ostream& operator<<(std::ostream& out, const BigUint& val)
{
    out << val.str();
    return out;
}

Block log2(const BigUint& in)
{
    if (in.zero()) throw std::runtime_error("log2(0) is undefined");
    return std::log2(in.val().back()) + (in.blockSize() - 1) * bitsPerBlock;
}


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

#include "little-big-int.hpp"

#include <algorithm>
#include <bitset>
#include <cassert>
#include <cmath>
#include <iostream>
#include <sstream>

const std::size_t BigUint::blockMax = std::numeric_limits<Block>::max();

BigUint::BigUint(const std::string& str)
    : m_arena()
    , m_val(1, 0, Alloc(m_arena))
{
    BigUint factor(1);
    const std::size_t size(str.size());

    for (std::size_t i(size - 8); i < size; i -= 8)
    {
        *this += BigUint(std::stoull(str.substr(i, 8)) * factor);
        factor *= 100000000;
    }

    if (std::size_t mod = size % 8)
    {
        *this += BigUint(std::stoull(str.substr(0, mod)) * factor);
    }
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
        std::string digits;
        digits.reserve(log2(*this) * 1000 / 3322 + 1);

        BigUint factor(10);
        BigUint lagged(1);

        std::pair<BigUint, BigUint> current;

        do
        {
            current = divMod(factor);

            digits.push_back('0' + (current.second / lagged).getSimple());

            lagged = factor;
            factor *= 10;
        }
        while (!current.first.zero());

        while (digits.back() == '0') digits.pop_back();

        return std::string(digits.rbegin(), digits.rend());
    }
}

std::string BigUint::bin() const
{
    std::string result("0b");
    const std::size_t size(m_val.size());

    for (std::size_t i(0); i < size; ++i)
    {
        result += std::bitset<bitsPerBlock>(m_val[size - i - 1]).to_string();
    }

    return result;
}

void BigUint::add(const BigUint& rhs, const Block shift)
{
    auto& rhsVal(rhs.data());

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
    const auto& dVal(d.data());

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
        auto& qVal(q.data());
        Block mask(0);

        for (std::size_t block(nValSize - 1); block < nValSize; --block)
        {
            for (std::size_t bit(bitsPerBlock - 1); bit < bitsPerBlock; --bit)
            {
                r <<= 1;
                mask = Block(1) << bit;

                if (m_val.at(block) & mask) ++r.data().front();

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
    auto& lhsVal(lhs.data());
    auto& rhsVal(rhs.data());

    auto& lhsFront(lhsVal.front());
    const BigUint::Block rhsFront(rhsVal.front());

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
            BigUint::Block& lhsCur(lhsVal[i]);
            const BigUint::Block& rhsCur(rhsVal[i]);

            lhsCur += rhsCur + static_cast<BigUint::Block>(carry);
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
    auto& lhsVal(lhs.data());
    const auto& rhsVal(rhs.data());

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
        BigUint::Block old(0);
        bool borrow(false);
        std::size_t i(0);

        while (i < rhsSize)
        {
            BigUint::Block& lhsCur(lhsVal[i]);
            old = lhsCur;
            const BigUint::Block& rhsCur(rhsVal[i]);

            lhsCur -= rhsCur + static_cast<BigUint::Block>(borrow);
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
    else if (
            BigUint::log2(lhs) + BigUint::log2(rhs) + 1 <=
                BigUint::bitsPerBlock)
    {
        lhs = lhs.data().front() * rhs.data().front();
    }
    else
    {
        BigUint out;
        auto& rhsVal(rhs.data());

        for (std::size_t block(0); block < rhsVal.size(); ++block)
        {
            for (std::size_t bit(0); bit < BigUint::bitsPerBlock; ++bit)
            {
                if ((rhsVal.at(block) >> bit) & 1)
                {
                    out.add(lhs, block * BigUint::bitsPerBlock + bit);
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
    auto& lhsVal(lhs.data());
    auto& rhsVal(rhs.data());

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
    auto& lhsVal(lhs.data());
    auto& rhsVal(rhs.data());

    const std::size_t rhsSize(rhsVal.size());

    lhsVal.resize(std::max(lhsVal.size(), rhsSize), 0);

    for (std::size_t i(0); i < rhsSize; ++i)
    {
        lhsVal[i] |= rhsVal[i];
    }

    return lhs;
}

BigUint& operator<<=(BigUint& lhs, BigUint::Block rhs)
{
    if (
            lhs.zero() || !rhs ||
            (lhs.trivial() && rhs < BigUint::bitsPerBlock &&
            (lhs.data().front() &
                (BigUint::blockMax << (BigUint::bitsPerBlock - rhs))) == 0))
    {
        lhs.data().front() <<= rhs;
        return lhs;
    }

    const std::size_t startBlocks(lhs.blockSize());
    const std::size_t shiftBlocks(rhs / BigUint::bitsPerBlock);
    const std::size_t shiftBits(rhs % BigUint::bitsPerBlock);
    const std::size_t shiftBack(
            shiftBits ? (BigUint::bitsPerBlock - shiftBits) : 0);

    auto& val(lhs.data());

    {
        BigUint::Block carry(shiftBack ? val.back() >> shiftBack : 0);
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

BigUint& operator>>=(BigUint& lhs, BigUint::Block rhs)
{
    const std::size_t startBlocks(lhs.blockSize());
    const std::size_t shiftBlocks(rhs / BigUint::bitsPerBlock);
    const std::size_t shiftBits(rhs % BigUint::bitsPerBlock);
    const std::size_t shiftBack(
            shiftBits ? (BigUint::bitsPerBlock - shiftBits) : 0);

    auto& val(lhs.data());

    for (std::size_t i(shiftBlocks); i < startBlocks - 1; ++i)
    {
        val[i - shiftBlocks] =
            (shiftBack ? val[i + 1] << shiftBack : 0) |
            (val[i] >> shiftBits);
    }

    BigUint::Block last(val.back() >> shiftBits);
    val.resize(startBlocks - shiftBlocks - (last ? 0 : 1));

    if (val.empty()) val.push_back(0);
    else if (last) val.back() = last;

    return lhs;
}

BigUint operator&(const BigUint& lhs, const BigUint& rhs)
{
    BigUint result;
    if (lhs.data().size() < rhs.data().size())
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

BigUint operator<<(const BigUint& lhs, const BigUint::Block rhs)
{
    if (
            lhs.zero() || !rhs ||
            (lhs.trivial() && rhs < BigUint::bitsPerBlock &&
            (lhs.data().front() &
                (BigUint::blockMax << (BigUint::bitsPerBlock - rhs))) == 0))
    {
        return BigUint(lhs.data().front() << rhs);
    }

    const std::size_t startBlocks(lhs.blockSize());
    const std::size_t shiftBlocks(rhs / BigUint::bitsPerBlock);
    const std::size_t shiftBits(rhs % BigUint::bitsPerBlock);

    BigUint result(std::vector<BigUint::Block>(startBlocks + shiftBlocks, 0));

    const auto& start(lhs.data());
    auto& val(result.data());

    BigUint::Block carry(0);

    for (std::size_t i(0); i < startBlocks; ++i)
    {
        val[i + shiftBlocks] = (start[i] << shiftBits) | carry;
        carry = shiftBits ?
            (start[i] >> (BigUint::bitsPerBlock - shiftBits)) : 0;
    }

    if (carry) val.push_back(carry);

    while (val.size() != 1 && val.back() == 0) val.pop_back();

    return result;
}

BigUint operator>>(const BigUint& lhs, const BigUint::Block rhs)
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
    const auto& lhsVal(lhs.data());
    const auto& rhsVal(rhs.data());

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
        if (rhs.trivial()) return lhs.data().front() < rhs.data().front();
        else return true;
    }

    const auto& lhsVal(lhs.data());
    const auto& rhsVal(rhs.data());

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

BigUint::Block BigUint::log2(const BigUint& in)
{
    if (in.trivial()) return std::log2(in.getSimple());
    return
        std::log2(in.data().back()) +
        (in.blockSize() - 1) * BigUint::bitsPerBlock;
}

BigUint BigUint::sqrt(const BigUint& in)
{
    return BigUint(1) << (log2(in) / 2);
}


/******************************************************************************
* Copyright (c) 2016, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#pragma once

#include <algorithm>
#include <cctype>
#include <string>

// Don't know where/which macro defines these things
#undef major
#undef minor

namespace entwine
{

class Version
{
public:
    Version() { }
    Version(int major, int minor = 0, int patch = 0)
        : m_major(major)
        , m_minor(minor)
        , m_patch(patch)
    { }

    Version(std::string s)
    {
        if (s.empty()) return;

        auto invalidCharacter([](char c)
        {
            return !(std::isdigit(c) || c == '.');
        });

        if (std::any_of(s.begin(), s.end(), invalidCharacter))
        {
            throw std::runtime_error("Invalid character in version string");
        }

        m_major = std::stoi(s);

        const std::size_t p(s.find_first_of('.'));
        if (p != std::string::npos && p < s.size() - 1)
        {
            m_minor = std::stoi(s.substr(p + 1));
        }
        else return;

        const std::size_t q(s.find_first_of('.', p + 1));
        if (q != std::string::npos && q < s.size() - 1)
        {
            m_patch = std::stoi(s.substr(q + 1));
        }
    }

    int major() const { return m_major; }
    int minor() const { return m_minor; }
    int patch() const { return m_patch; }

    std::string toString() const
    {
        return
            std::to_string(major()) + "." +
            std::to_string(minor()) + "." +
            std::to_string(patch());
    }

    bool empty() const
    {
        return !m_major && !m_minor && !m_patch;
    }

private:
    int m_major = 0;
    int m_minor = 0;
    int m_patch = 0;
};

inline bool operator<(const Version& a, const Version& b)
{
    if (a.major() < b.major()) return true;
    else if (a.major() > b.major()) return false;

    if (a.minor() < b.minor()) return true;
    else if (a.minor() > b.minor()) return false;

    return a.patch() < b.patch();
}

inline bool operator>=(const Version& a, const Version& b)
{
    return !(a < b);
}

inline bool operator==(const Version& a, const Version& b)
{
    return
        a.major() == b.major() &&
        a.minor() == b.minor() &&
        a.patch() == b.patch();
}

} // namespace entwine


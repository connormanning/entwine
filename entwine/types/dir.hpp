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

#include <entwine/types/point.hpp>

namespace entwine
{

enum class Dir : int
{
    swd = 0,
    sed = 1,
    nwd = 2,
    ned = 3,
    swu = 4,
    seu = 5,
    nwu = 6,
    neu = 7
};

static constexpr unsigned int EwBit = 0x01;
static constexpr unsigned int NsBit = 0x02;
static constexpr unsigned int UdBit = 0x04;

inline constexpr std::size_t dirHalfEnd() { return 4; }
inline constexpr std::size_t dirEnd() { return 8; }

// Get the direction from an origin O to a point P.
inline Dir getDirection(const Point& o, const Point& p)
{
    return static_cast<Dir>(
            (p.y >= o.y ? NsBit : 0) |
            (p.x >= o.x ? EwBit : 0) |
            (p.z >= o.z ? UdBit : 0));
}

inline Dir getDirection(const Point& o, const Point& p, bool force2d)
{
    if (force2d)
    {
        return static_cast<Dir>(
                (p.y >= o.y ? NsBit : 0) |  // North? +2.
                (p.x >= o.x ? EwBit : 0));  // East? +1.
    }
    else
    {
        return getDirection(o, p);
    }
}

inline bool isNorth(Dir dir) { return static_cast<int>(dir) & NsBit; }
inline bool isEast (Dir dir) { return static_cast<int>(dir) & EwBit; }
inline bool isUp   (Dir dir) { return static_cast<int>(dir) & UdBit; }

inline bool isSouth(Dir dir) { return !isNorth(dir); }
inline bool isWest(Dir dir) { return !isEast(dir); }
inline bool isDown(Dir dir) { return !isUp(dir); }

inline std::string dirToString(Dir dir)
{
    switch (dir)
    {
        case Dir::swd: return "swd"; break;
        case Dir::sed: return "sed"; break;
        case Dir::nwd: return "nwd"; break;
        case Dir::ned: return "ned"; break;
        case Dir::swu: return "swu"; break;
        case Dir::seu: return "seu"; break;
        case Dir::nwu: return "nwu"; break;
        case Dir::neu: return "neu"; break;
    }

    throw std::runtime_error("Cannot convert invalid Dir to string");
}
inline std::string toString(Dir dir) { return dirToString(dir); }

inline Dir stringToDir(const std::string& s)
{
    return static_cast<Dir>(
            (s[0] == 'n' ? NsBit : 0) |
            (s[1] == 'e' ? EwBit : 0) |
            (s[2] == 'u' ? UdBit : 0));
}

inline std::size_t toIntegral(Dir dir, bool force2d = false)
{
    std::size_t result(static_cast<std::size_t>(dir));
    if (force2d) result %= 4;
    return result;
}

inline Dir toDir(std::size_t val)
{
    return static_cast<Dir>(val);
}

inline std::ostream& operator<<(std::ostream& os, Dir dir)
{
    return os << dirToString(dir);
}

} // namespace entwine


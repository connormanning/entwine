/******************************************************************************
* Copyright (c) 2016, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <atomic>

namespace entwine
{

// A copy-constructible and assignable atomic wrapper for use in a vector.
template <typename T>
struct ElasticAtomic
{
    ElasticAtomic()
        : atom()
    { }

    ElasticAtomic(const std::atomic<T>& other)
        : atom(other.load())
    { }

    ElasticAtomic(const ElasticAtomic& other)
        : atom(other.atom.load())
    { }

    ElasticAtomic& operator=(const ElasticAtomic& other)
    {
        atom.store(other.atom.load());
        return *this;
    }

    std::atomic<T> atom;
};

} // namespace entwine


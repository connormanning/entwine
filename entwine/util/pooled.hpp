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

#include <entwine/third/pool/memory-pool.hpp>

namespace entwine
{

template<typename T>
class Pooled
{
public:
    Pooled(T* val, MemoryPool<T>& pool)
        : m_val(val)
        , m_pool(pool)
    { }

    ~Pooled()
    {
        m_pool.deleteElement(m_val);
    }

    T* val()
    {
        return m_val;
    }

private:
    T* m_val;
    MemoryPool<T>& m_pool;

    Pooled(const Pooled<T>&);
    Pooled<T> operator=(const Pooled<T>&);
};

} // namespace entwine


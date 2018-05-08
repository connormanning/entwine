/******************************************************************************
* Copyright (c) 2018, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <entwine/tree/slice.hpp>

#include <mutex>

namespace entwine
{

namespace
{
    std::mutex m;
    Slice::Info info;
}

void Slice::write(const Xyz& p, Cells&& cells) const
{
    {
        std::lock_guard<std::mutex> lock(m);
        ++info.written;
    }

    m_metadata.storage().write(
            m_out,
            m_tmp,
            m_pointPool,
            p.toString(m_depth) + m_metadata.postfix(m_depth),
            std::move(cells));
}

Cells Slice::read(const Xyz& p) const
{
    {
        std::lock_guard<std::mutex> lock(m);
        ++info.read;
    }

    return m_metadata.storage().read(
            m_out,
            m_tmp,
            m_pointPool,
            p.toString(m_depth) + m_metadata.postfix(m_depth));
}

Slice::Info Slice::latchInfo()
{
    std::lock_guard<std::mutex> lock(m);
    Slice::Info result(info);
    info.clear();
    return result;
}

} // namespace entwine


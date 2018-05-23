/******************************************************************************
* Copyright (c) 2018, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <entwine/new-reader/new-reader.hpp>

#include <entwine/util/unique.hpp>

namespace entwine
{

NewReader::NewReader(
        std::string data,
        std::string tmp,
        std::shared_ptr<NewCache> cache,
        std::shared_ptr<arbiter::Arbiter> a)
    : m_arbiter(maybeDefault(a))
    , m_ep(m_arbiter->getEndpoint(data))
    , m_tmp(m_arbiter->getEndpoint(
                tmp.size() ? tmp : arbiter::fs::getTempPath()))
    , m_metadata(m_ep)
    , m_hierarchy(m_ep)
    , m_cache(maybeDefault(cache))
    , m_pointPool(m_metadata.schema(), m_metadata.delta())
{ }

std::unique_ptr<NewCountQuery> NewReader::count(const Json::Value& j) const
{
    const NewQueryParams p(j);
    return makeUnique<NewCountQuery>(*this, p);
}

std::unique_ptr<NewReadQuery> NewReader::read(const Json::Value& j) const
{
    const NewQueryParams p(j);
    return makeUnique<NewReadQuery>(*this, p, Schema(j["schema"]));
}

Json::Value NewReader::hierarchy(const Json::Value& j) const
{
    Json::Value h;
    const NewQueryParams p(j);
    ChunkKey c(m_metadata);
    hierarchy(h, p, c);
    return h;
}

void NewReader::hierarchy(
        Json::Value& json,
        const NewQueryParams& p,
        const ChunkKey& c) const
{
    if (c.depth() >= p.de()) return;

    const auto& b(p.bounds());
    if (b != Bounds::everything() && !b.growBy(0.05).contains(c.bounds()))
    {
        return;
    }

    const uint64_t count(m_hierarchy.count(c.get()));
    if (c.depth() >= p.db()) json["n"] = count;

    if (c.depth() + 1 >= p.de()) return;

    for (std::size_t i(0); i < dirEnd(); ++i)
    {
        const auto dir(toDir(i));
        const ChunkKey next(c.getStep(dir));
        if (m_hierarchy.count(next.get()))
        {
            hierarchy(json[toString(dir)], p, next);
        }
    }
}

} // namespace entwine


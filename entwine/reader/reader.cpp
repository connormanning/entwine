/******************************************************************************
* Copyright (c) 2018, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <entwine/reader/reader.hpp>

#include <entwine/util/unique.hpp>

namespace entwine
{

Reader::Reader(
        std::string out,
        std::string tmp,
        std::shared_ptr<Cache> cache,
        std::shared_ptr<arbiter::Arbiter> a)
    : m_arbiter(maybeDefault(a))
    , m_ep(m_arbiter->getEndpoint(out))
    , m_tmp(m_arbiter->getEndpoint(
                tmp.size() ? tmp : arbiter::fs::getTempPath()))
    , m_metadata(m_ep)
    , m_hierarchy(m_metadata, m_ep)
    , m_cache(makeUnique<Cache>())
{ }

std::unique_ptr<CountQuery> Reader::count(const Json::Value& j) const
{
    const QueryParams p(j);
    return makeUnique<CountQuery>(*this, p);
}

std::unique_ptr<ReadQuery> Reader::read(const Json::Value& j) const
{
    const QueryParams p(j);
    return makeUnique<ReadQuery>(*this, p, Schema(j["schema"]));
}

} // namespace entwine


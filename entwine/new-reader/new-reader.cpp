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
        std::string out,
        std::string tmp,
        std::shared_ptr<NewCache> cache,
        std::shared_ptr<arbiter::Arbiter> a)
    : m_arbiter(maybeDefault(a))
    , m_ep(m_arbiter->getEndpoint(out))
    , m_tmp(m_arbiter->getEndpoint(
                tmp.size() ? tmp : arbiter::fs::getTempPath()))
    , m_metadata(m_ep)
    , m_hierarchy(m_metadata, m_ep)
    , m_cache(makeUnique<NewCache>())
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

} // namespace entwine


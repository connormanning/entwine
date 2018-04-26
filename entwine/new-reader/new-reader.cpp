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

/*
std::vector<char> NewReader::read(const Json::Value& j) const
{
    const NewQueryParams p(j);
    NewQuery q(*this, p);
    q.run();

    return std::vector<char>();
}
*/

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

Json::Value NewReader::fakeHierarchy(const Json::Value j) const
{
    const NewQueryParams p(j);

    Json::Value json;
    uint64_t d(p.db());

    fakeHierarchy(json, d, p);

    return json;
}

void NewReader::fakeHierarchy(
        Json::Value& json,
        uint64_t d,
        const NewQueryParams& p) const
{
    json["n"] = 1;

    if (++d >= p.de() || d >= 14) return;

    for (std::size_t i(0); i < dirEnd(); ++i)
    {
        fakeHierarchy(json[dirToString(toDir(i))], d, p);
    }
}

} // namespace entwine


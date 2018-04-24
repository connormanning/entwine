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
{
    /*
    std::cout << "M: " << m_metadata.toJson() << std::endl;
    std::cout << "E: " << m_ep.prefixedRoot() << std::endl;
    std::cout << "T: " << m_tmp.prefixedRoot() << std::endl;
    */
}

std::vector<char> NewReader::read(const Json::Value& j) const
{
    const NewQueryParams p(j);
    NewQuery q(*this, p);

    return std::vector<char>();
}

} // namespace entwine


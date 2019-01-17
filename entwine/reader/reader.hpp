/******************************************************************************
* Copyright (c) 2018, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#pragma once

#include <memory>
#include <string>

#include <entwine/reader/cache.hpp>
#include <entwine/reader/hierarchy-reader.hpp>
#include <entwine/reader/query.hpp>
#include <entwine/third/arbiter/arbiter.hpp>
#include <entwine/types/key.hpp>
#include <entwine/types/metadata.hpp>
#include <entwine/util/json.hpp>

namespace entwine
{

class Reader
{
public:
    Reader(
            std::string out,
            std::string tmp = "",
            std::shared_ptr<Cache> cache = std::shared_ptr<Cache>(),
            std::shared_ptr<arbiter::Arbiter> a =
                std::shared_ptr<arbiter::Arbiter>());

    std::unique_ptr<CountQuery> count(const json& j) const;
    std::unique_ptr<ReadQuery> read(const json& j) const;

    const Metadata& metadata() const { return m_metadata; }
    const HierarchyReader& hierarchy() const { return m_hierarchy; }
    const arbiter::Endpoint& ep() const { return m_ep; }
    const arbiter::Endpoint& tmp() const { return m_tmp; }
    Cache& cache() const { return *m_cache; }

    std::string path() const { return ep().prefixedRoot(); }

private:
    std::shared_ptr<arbiter::Arbiter> m_arbiter;
    arbiter::Endpoint m_ep;
    arbiter::Endpoint m_tmp;

    const Metadata m_metadata;
    const HierarchyReader m_hierarchy;

    std::unique_ptr<Cache> m_cache;
};

} // namespace entwine


/******************************************************************************
* Copyright (c) 2016, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <entwine/tree/builder.hpp>
#include <entwine/tree/merger.hpp>
#include <entwine/tree/new-clipper.hpp>
#include <entwine/tree/thread-pools.hpp>
#include <entwine/types/manifest.hpp>
#include <entwine/types/metadata.hpp>
#include <entwine/types/subset.hpp>
#include <entwine/util/pool.hpp>
#include <entwine/util/unique.hpp>

namespace entwine
{

Merger::Merger(const Config& config)
    : m_config(config)
    , m_verbose(config.verbose())
{
    m_outerScope.setArbiter(
            std::make_shared<arbiter::Arbiter>(config["arbiter"]));

    Config first(m_config);
    first["subset"]["id"] = 1;

    m_builder = makeUnique<Builder>(first, m_outerScope);

    if (!m_builder) throw std::runtime_error("Path not mergeable");

    m_builder->verbose(m_verbose);
    m_outerScope.setPointPool(m_builder->sharedPointPool());

    if (const Subset* subset = m_builder->metadata().subset())
    {
        m_of = subset->of();
    }
    else
    {
        throw std::runtime_error("Could not get number of subsets");
    }

    if (m_verbose)
    {
        std::cout << "Awakened 1 / " << m_of << std::endl;
    }
}

Merger::~Merger() { }

void Merger::go()
{
    auto clipper(makeUnique<NewClipper>(m_builder->registry()));
    for (m_id = 2; m_id <= m_of; ++m_id)
    {
        if (m_verbose)
        {
            std::cout << "Merging " << m_id << " / " << m_of << std::endl;
        }

        Config current(m_config);
        current["subset"]["id"] = Json::UInt64(m_id);
        current["subset"]["of"] = Json::UInt64(m_of);
        Builder b(current, m_outerScope);
        m_builder->merge(b, *clipper);
    }

    m_builder->makeWhole();

    if (m_verbose) std::cout << "Merge complete.  Saving..." << std::endl;
    clipper.reset();
    m_builder->save();
    m_builder.reset();
    if (m_verbose) std::cout << "\tFinal save complete." << std::endl;
}

} // namespace entwine


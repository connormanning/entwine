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
#include <entwine/tree/thread-pools.hpp>
#include <entwine/types/manifest.hpp>
#include <entwine/types/metadata.hpp>
#include <entwine/types/subset.hpp>
#include <entwine/util/pool.hpp>
#include <entwine/util/unique.hpp>

namespace entwine
{

Merger::Merger(
        const std::string path,
        const std::size_t threads,
        const bool verbose,
        std::shared_ptr<arbiter::Arbiter> arbiter)
    : m_builder()
    , m_path(path)
    , m_threads(threads)
    , m_outerScope(makeUnique<OuterScope>())
    , m_verbose(verbose)
{
    m_outerScope->setArbiter(arbiter);

    const std::size_t zero(0);
    m_builder = Builder::tryCreateExisting(
            m_path,
            ".",
            ThreadPools::getWorkThreads(threads),
            ThreadPools::getClipThreads(threads),
            &zero,
            *m_outerScope);

    if (!m_builder)
    {
        throw std::runtime_error("Path not mergeable");
    }
    else if (!m_builder->metadata().subset())
    {
        throw std::runtime_error(
                "This path is already whole - no merge needed");
    }

    m_builder->verbose(m_verbose);
    m_outerScope->setPointPool(m_builder->sharedPointPool());
    m_outerScope->setHierarchyPool(m_builder->sharedHierarchyPool());

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
        std::cout << "Awakened 1 / " << total() << std::endl;
    }
}

Merger::~Merger() { }

void Merger::go()
{
    m_builder->unbump();

    Pool pool(m_threads);
    std::size_t fetches(static_cast<float>(pool.size()) * 1.2);

    while (m_pos < total())
    {
        const std::size_t n(std::min(fetches, total() - m_pos));
        std::vector<std::unique_ptr<Builder>> builders(n);

        for (auto& b : builders)
        {
            const std::size_t id(m_pos++);
            if (m_verbose)
            {
                std::cout << "Merging " << m_pos << " / " << total() <<
                    std::endl;
            }

            pool.add([this, &b, id]()
            {
                b = Builder::tryCreateExisting(
                        m_path,
                        ".",
                        ThreadPools::getWorkThreads(m_threads),
                        ThreadPools::getClipThreads(m_threads),
                        &id,
                        *m_outerScope);

                if (!b) std::cout << "Create failed: " << id << std::endl;
            });
        }

        pool.cycle();

        for (auto& b : builders)
        {
            if (!b) throw std::runtime_error("Couldn't create subset");
            b->verbose(m_verbose);
            m_builder->merge(*b);
        }
    }

    m_builder->makeWhole();

    if (m_verbose) std::cout << "Merge complete.  Saving..." << std::endl;
    m_builder->save();
    m_builder.reset();
    if (m_verbose) std::cout << "\tFinal save complete." << std::endl;
}

} // namespace entwine


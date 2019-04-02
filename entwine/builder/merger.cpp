/******************************************************************************
* Copyright (c) 2016, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <entwine/builder/merger.hpp>

#include <cassert>

#include <entwine/builder/builder.hpp>
#include <entwine/builder/pruner.hpp>
#include <entwine/builder/registry.hpp>
#include <entwine/builder/thread-pools.hpp>
#include <entwine/types/metadata.hpp>
#include <entwine/types/subset.hpp>
#include <entwine/util/pool.hpp>
#include <entwine/util/unique.hpp>

namespace entwine
{

Merger::Merger(const Config& config)
    : m_config(config)
    , m_arbiter(std::make_shared<arbiter::Arbiter>(config.arbiter()))
    , m_verbose(m_config.verbose())
    , m_threads(m_config.totalThreads())
    , m_pool(m_threads)
{
    Config first(m_config);
    first.setSubsetId(1);

    m_builder = makeUnique<Builder>(first, m_arbiter);

    if (!m_builder) throw std::runtime_error("Path not mergeable");

    m_builder->verbose(m_verbose);

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
    auto pruner(makeUnique<Pruner>(m_builder->registry().cache()));

    m_id = 2;
    while (m_id <= m_of)
    {
        // One-based.
        const uint64_t n(std::min<uint64_t>(m_threads, m_of - m_id + 1));
        assert(m_id + n <= m_of + 1);

        std::vector<std::unique_ptr<Builder>> v(n);

        for (uint64_t i(0); i < n; ++i)
        {
            assert(m_id + i <= m_of);
            Config current(m_config);
            current.setSubsetId(m_id + i);
            current.setSubsetOf(m_of);
            current.setThreads(1);

            m_pool.add([this, &v, current, i]()
            {
                try
                {
                    v[i] = makeUnique<Builder>(current, m_arbiter);
                }
                catch (std::exception& e)
                {
                    std::cout << "Failed create " << (m_id + i) << ": " <<
                        e.what() << std::endl;
                }
                catch (...)
                {
                    std::cout << "Failed create " << (m_id + i) << ": " <<
                        "unknown error" << std::endl;
                }
            });
        }

        m_pool.cycle();

        if (m_verbose)
        {
            std::cout << "Merging " << m_id << " / " << m_of << std::endl;
        }

        for (uint64_t i(0); i < v.size(); ++i)
        {
            if (!v.at(i) || !v.at(i)->isContinuation())
            {
                throw std::runtime_error("A subset could not be created");
            }

            m_builder->merge(*v.at(i), *pruner);
        }

        m_id += n;
    }

    if (m_verbose)
    {
        std::cout << "Merged " << m_of << " / " << m_of << std::endl;
    }

    m_builder->makeWhole();

    if (m_verbose) std::cout << "Merge complete.  Saving..." << std::endl;
    pruner.reset();
    m_builder->save();
    m_builder.reset();
    if (m_verbose) std::cout << "\tFinal save complete." << std::endl;
}

} // namespace entwine


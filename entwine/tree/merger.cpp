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
#include <entwine/tree/manifest.hpp>
#include <entwine/tree/merger.hpp>
#include <entwine/types/metadata.hpp>
#include <entwine/types/subset.hpp>

namespace entwine
{

Merger::Merger(
        const std::string path,
        const std::size_t threads,
        std::shared_ptr<arbiter::Arbiter> arbiter)
    : m_builder()
    , m_path(path)
    , m_numSubsets(1)
    , m_threads(threads)
    , m_outerScope(new OuterScope())
{
    m_outerScope->setArbiter(arbiter);
    m_outerScope->setNodePool(std::make_shared<Node::NodePool>());

    m_builder = Builder::create(path, threads, *m_outerScope);
    if (!m_builder) throw std::runtime_error("Path not mergeable");

    m_outerScope->setPointPool(m_builder->sharedPointPool());

    if (const Subset* subset = m_builder->metadata().subset())
    {
        m_numSubsets = subset->of();
    }
}

Merger::~Merger() { }

void Merger::go()
{
    std::cout << "\t1 / " << m_numSubsets << std::flush;
    unsplit(*m_builder);
    std::cout << " done." << std::endl;

    for (std::size_t id(1); id < m_numSubsets; ++id)
    {
        std::cout << "\t" << (id + 1) << " / " << m_numSubsets << std::flush;

        auto current(Builder::create(m_path, id, *m_outerScope));
        if (!current) throw std::runtime_error("Couldn't create split builder");

        unsplit(*current);

        std::cout << " merging..." << std::flush;
        m_builder->merge(*current);

        std::cout << " done." << std::endl;
    }

    m_builder->makeWhole();
    m_builder->save();
}

void Merger::unsplit(Builder& builder)
{
    const Manifest& manifest(builder.metadata().manifest());
    if (!manifest.split()) return;

    std::cout << " unsplitting..." << std::flush;

    /*
    std::unique_ptr<std::size_t> subsetId(
            builder.subset() ?
                new std::size_t(builder.subset()->id()) : nullptr);

    std::size_t pos(manifest.split()->end());

    while (pos < manifest.size())
    {
        std::unique_ptr<Builder> nextSplit(
                new Builder(
                    builder.outEndpoint().root(),
                    m_threads,
                    subsetId.get(),
                    &pos));

        pos = nextSplit->metadata().manifest().split()->end();
        builder.unsplit(*nextSplit);
    }
    */
}

} // namespace entwine


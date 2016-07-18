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
#include <entwine/util/unique.hpp>

namespace entwine
{

Merger::Merger(
        const std::string path,
        const std::size_t threads,
        const std::size_t* subsetId,
        std::shared_ptr<arbiter::Arbiter> arbiter)
    : m_builder()
    , m_path(path)
    , m_others()
    , m_threads(threads)
    , m_outerScope(makeUnique<OuterScope>())
{
    m_outerScope->setArbiter(arbiter);

    m_builder = Builder::create(
            path,
            threads,
            subsetId,
            nullptr,
            *m_outerScope);

    if (!m_builder) throw std::runtime_error("Path not mergeable");

    m_outerScope->setPointPool(m_builder->sharedPointPool());

    if (!subsetId)
    {
        if (const Subset* subset = m_builder->metadata().subset())
        {
            // No subset ID was passed, so we will want to merge everything -
            // and we have already awakened subset 0.
            for (std::size_t i(1); i < subset->of(); ++i)
            {
                m_others.push_back(i);
            }
        }
    }
}

Merger::~Merger() { save(); }

void Merger::unsplit()
{
    const std::size_t total(m_others.size() + 1);

    if (const Subset* subset = m_builder->metadata().subset())
    {
        std::cout << "Unsplitting " <<
            (subset->id() + 1) << " / " << total << "..." << std::endl;
    }
    else
    {
        std::cout << "Unsplitting..." << std::endl;
    }

    unsplit(*m_builder);
    std::cout << "\tDone." << std::endl;

    for (const auto id : m_others)
    {
        std::cout << "Unsplitting " <<
            (id + 1) << " / " << total << "..." << std::endl;

        auto current(
                Builder::create(
                    m_path,
                    m_threads,
                    &id,
                    nullptr,
                    *m_outerScope));

        if (!current) throw std::runtime_error("Couldn't create split builder");

        unsplit(*current);
        current->save();

        std::cout << "\tDone." << std::endl;
    }
}

void Merger::merge()
{
    const std::size_t total(m_others.size() + 1);

    for (const auto id : m_others)
    {
        std::cout << "Merging " << id << " / " << total << std::endl;

        auto current(
                Builder::create(
                    m_path,
                    m_threads,
                    &id,
                    nullptr,
                    *m_outerScope));

        if (!current) throw std::runtime_error("Couldn't create subset");

        m_builder->merge(*current);

        std::cout << "\tDone." << std::endl;
    }

    m_builder->makeWhole();
}

void Merger::save()
{
    if (m_builder)
    {
        std::cout << "Saving..." << std::endl;
        m_builder->save();
        m_builder.reset();
        std::cout << "\tDone." << std::endl;
    }
}

void Merger::unsplit(Builder& builder)
{
    const Metadata& metadata(builder.metadata());
    const Manifest& manifest(metadata.manifest());

    if (!manifest.split()) return;

    std::unique_ptr<std::size_t> subsetId(
            metadata.subset() ?
                new std::size_t(metadata.subset()->id()) : nullptr);

    std::size_t pos(manifest.split()->end());

    while (pos < manifest.size())
    {
        auto nextSplit = Builder::create(
                builder.outEndpoint().root(),
                m_threads,
                subsetId.get(),
                &pos,
                *m_outerScope);

        std::cout << "\t\t" << pos << std::endl;

        pos = nextSplit->metadata().manifest().split()->end();
        builder.unsplit(*nextSplit);
    }
}

} // namespace entwine


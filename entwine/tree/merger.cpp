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
#include <entwine/types/subset.hpp>

namespace entwine
{

Merger::Merger(
        const std::string path,
        const std::size_t threads,
        std::shared_ptr<arbiter::Arbiter> arbiter)
    : m_builders()
    , m_path(path)
    , m_threads(threads)
    , m_arbiter(arbiter)
{
    auto first(Builder::create(path, m_threads, arbiter));
    if (!first) throw std::runtime_error("Path not mergeable");

    std::size_t subs(first->subset() ? first->subset()->of() : 1);
    m_builders.resize(subs);

    m_builders.front() = std::move(first);
}

Merger::~Merger() { }

void Merger::go()
{
    unsplit();
    merge();

    m_builders.front()->makeWhole();
    std::cout << "Made whole, saving..." << std::endl;
    m_builders.front()->save();
}

void Merger::unsplit()
{
    m_builders[0] = unsplitOne(std::move(m_builders.front()));

    for (std::size_t id(1); id < m_builders.size(); ++id)
    {
        auto current(Builder::create(m_path, m_threads, id, m_arbiter));
        if (!current) throw std::runtime_error("Couldn't create split builder");

        m_builders[id] = unsplitOne(std::move(current));
    }
}

std::unique_ptr<Builder> Merger::unsplitOne(
        std::unique_ptr<Builder> builder) const
{
    if (!builder->manifest().split()) return builder;

    std::unique_ptr<std::size_t> subsetId(
            builder->subset() ?
                new std::size_t(builder->subset()->id()) : nullptr);

    std::size_t pos(builder->manifest().split()->end());

    while (pos < builder->manifest().size())
    {
        std::unique_ptr<Builder> current(
                new Builder(
                    builder->outEndpoint().root(),
                    m_threads,
                    subsetId.get(),
                    &pos,
                    nullptr,
                    builder->m_pointPool));

        pos = current->manifest().split()->end();

        builder->unsplit(*current);
    }

    return builder;
}

void Merger::merge()
{
    if (m_builders.size() == 1) return;

    std::cout << "\t1 / " << m_builders.size() << std::endl;
    for (std::size_t id(1); id < m_builders.size(); ++id)
    {
        std::cout << "\t" << id + 1 << " / " << m_builders.size() << std::endl;
        m_builders.front()->merge(*m_builders[id]);
    }
}

} // namespace entwine


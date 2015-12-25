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
        std::shared_ptr<arbiter::Arbiter> arbiter)
    : m_builders()
    , m_path(path)
    , m_arbiter(arbiter)
{
    auto first(Builder::create(path, arbiter));
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
}

void Merger::unsplit()
{
    m_builders.front()->unsplit();

    for (std::size_t id(1); id < m_builders.size(); ++id)
    {
        auto current(Builder::create(m_path, id, m_arbiter));
        current->unsplit();

        m_builders[id] = std::move(current);
    }
}

void Merger::merge()
{
    std::cout << "\t1 / " << m_builders.size() << std::endl;
    for (std::size_t id(1); id < m_builders.size(); ++id)
    {
        std::cout << "\t" << id + 1 << " / " << m_builders.size() << std::endl;
        m_builders.front()->merge(*m_builders[id]);
    }

    m_builders.front()->makeWhole();
    m_builders.front()->save();
}

} // namespace entwine


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

#include <cstddef>
#include <cstdint>
#include <memory>

#include <entwine/types/bounds.hpp>
#include <entwine/types/dir.hpp>
#include <entwine/util/json.hpp>

namespace entwine
{

class Metadata;

class Subset
{
public:
    Subset(const Metadata& metadata, const json& j);
    static std::unique_ptr<Subset> create(const Metadata& m, const json& j);

    uint64_t id() const { return m_id; }
    uint64_t of() const { return m_of; }
    uint64_t splits() const { return m_splits; }

    bool primary() const { return m_id == 1; }

    const Bounds& bounds() const { return m_bounds; }

private:
    const uint64_t m_id;
    const uint64_t m_of;

    const uint64_t m_splits;
    Bounds m_bounds;
};

inline void to_json(json& j, const Subset& s)
{
    j = { { "id", s.id() }, { "of", s.of() } };
}

} // namespace entwine


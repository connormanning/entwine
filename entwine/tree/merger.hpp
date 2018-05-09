/******************************************************************************
* Copyright (c) 2016, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <memory>
#include <string>
#include <vector>

#include <entwine/tree/config.hpp>
#include <entwine/types/outer-scope.hpp>

namespace Json { class Value; }

namespace entwine
{

namespace arbiter { class Arbiter; }

class Builder;

class Merger
{
public:
    Merger(const Config& config);
    ~Merger();

    void go();

    std::size_t id() const { return m_id; }
    std::size_t of() const { return m_of; }

private:
    const Config m_config;
    std::unique_ptr<Builder> m_builder;
    OuterScope m_outerScope;

    std::size_t m_id = 1;
    std::size_t m_of = 0;
    bool m_verbose;
};

} // namespace entwine


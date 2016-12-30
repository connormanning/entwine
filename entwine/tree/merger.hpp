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

namespace Json { class Value; }

namespace entwine
{

namespace arbiter { class Arbiter; }

class Builder;
class OuterScope;

class Merger
{
public:
    Merger(
            std::string path,
            std::size_t threads,
            const std::size_t* subsetId = nullptr,
            bool verbose = false,
            std::shared_ptr<arbiter::Arbiter> arbiter = nullptr);

    ~Merger();

    void go();

    std::size_t index() const { return m_pos; }
    std::size_t total() const { return m_others.size() + 1; }

private:
    std::unique_ptr<Builder> m_builder;
    std::string m_path;
    std::vector<std::size_t> m_others;
    std::size_t m_threads;
    std::unique_ptr<OuterScope> m_outerScope;

    std::size_t m_pos;

    bool m_verbose;
};

} // namespace entwine


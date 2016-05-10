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

namespace arbiter { class Arbiter; }
namespace Json { class Value; }

namespace entwine
{

class Builder;

class Merger
{
public:
    Merger(
            std::string path,
            std::size_t threads,
            std::shared_ptr<arbiter::Arbiter> arbiter = nullptr);
    ~Merger();

    void go();

private:
    void unsplit();
    void merge();

    std::unique_ptr<Builder> unsplitOne(std::unique_ptr<Builder> builder) const;

    std::vector<std::unique_ptr<Builder>> m_builders;
    std::string m_path;
    std::size_t m_threads;
    std::shared_ptr<arbiter::Arbiter> m_arbiter;
};

} // namespace entwine


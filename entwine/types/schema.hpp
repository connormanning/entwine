/******************************************************************************
* Copyright (c) 2016, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#pragma once

#include <cstddef>
#include <vector>

#include <pdal/PointLayout.hpp>

#include <entwine/third/json/json.hpp>
#include <entwine/types/dim-info.hpp>

namespace entwine
{

class Schema
{
public:
    // Populate this schema later.  Call finalize() when done populating the
    // layout.
    Schema();

    // Schema layout will be finalized with these dims.
    explicit Schema(DimList dims);
    explicit Schema(const Json::Value& json);
    explicit Schema(const std::string& s);  // Will be parsed as JSON.

    Schema(const Schema& other);
    Schema& operator=(const Schema& other);

    ~Schema();

    void finalize();

    std::size_t pointSize() const
    {
        return m_layout->pointSize();
    }

    const DimList& dims() const
    {
        return m_dims;
    }

    bool contains(const std::string& name) const
    {
        const auto it(
                std::find_if(
                    m_dims.begin(),
                    m_dims.end(),
                    [&name](const DimInfo& d) { return d.name() == name; }));

        return it != m_dims.end();
    }

    pdal::Dimension::Id::Enum getId(const std::string& name) const
    {
        return pdalLayout().findDim(name);
    }

    pdal::PointLayout& pdalLayout() const
    {
        return *m_layout.get();
    }

    Json::Value toJson() const;

private:
    std::unique_ptr<pdal::PointLayout> m_layout;
    DimList m_dims;
};

inline bool operator==(const Schema& lhs, const Schema& rhs)
{
    return lhs.dims() == rhs.dims();
}

inline bool operator!=(const Schema& lhs, const Schema& rhs)
{
    return !(lhs == rhs);
}

inline std::ostream& operator<<(std::ostream& os, const Schema& schema)
{
    os << schema.toJson();
    return os;
}

} // namespace entwine


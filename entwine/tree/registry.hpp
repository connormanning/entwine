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
#include <memory>
#include <mutex>
#include <vector>

namespace Json
{
    class Value;
}

namespace entwine
{

class Chunk;
class Clipper;
class Cold;
class Entry;
class PointInfo;
class Roller;
class Source;
class Schema;
class Structure;

class Registry
{
public:
    Registry(Source& source, const Schema& schema, const Structure& structure);
    Registry(
            Source& source,
            const Schema& schema,
            const Structure& structure,
            const Json::Value& meta);

    ~Registry();

    bool addPoint(PointInfo** toAddPtr, Roller& roller, Clipper* clipper);

    void save(Json::Value& meta);

    Entry* getEntry(std::size_t index, Clipper* clipper);

    void clip(std::size_t index, Clipper* clipper);

private:
    bool tryAdd(PointInfo** toAddPtr, const Roller& roller, Clipper* clipper);

    Source& m_source;
    const Schema& m_schema;
    const Structure& m_structure;

    std::unique_ptr<Chunk> m_base;
    std::unique_ptr<Cold> m_cold;

    const std::vector<char> m_empty;
};

} // namespace entwine


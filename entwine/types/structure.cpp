/******************************************************************************
* Copyright (c) 2016, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <entwine/types/structure.hpp>

#include <cassert>
#include <cmath>
#include <iostream>

#include <entwine/tree/climber.hpp>
#include <entwine/tree/heuristics.hpp>
#include <entwine/types/subset.hpp>
#include <entwine/util/env.hpp>

namespace entwine
{

namespace
{
    const bool shallow(
            env("TESTING_SHALLOW") &&
            *env("TESTING_SHALLOW") == "true");
}

ChunkInfo::ChunkInfo(const Structure& structure, const Id& index)
    : m_structure(structure)
    , m_index(index)
    , m_chunkId(0)
    , m_depth(calcDepth(m_structure.factor(), index))
    , m_chunkOffset(0)
    , m_pointsPerChunk(0)
    , m_chunkNum(0)
{
    if (m_index < m_structure.coldIndexBegin())
    {
        throw std::runtime_error("Depth too low for chunk-info");
    }

    const Id levelIndex(calcLevelIndex(m_structure.dimensions(), m_depth));
    const std::size_t basePointsPerChunk(m_structure.basePointsPerChunk());

    const Id& sparseIndexBegin(m_structure.sparseIndexBegin());
    const Id& coldIndexBegin(m_structure.coldIndexBegin());

    if (!m_structure.dynamicChunks() || levelIndex <= sparseIndexBegin)
    {
        m_pointsPerChunk = basePointsPerChunk;
        const auto divMod((m_index - coldIndexBegin).divMod(m_pointsPerChunk));
        m_chunkNum = divMod.first.getSimple();
        m_chunkOffset = divMod.second.getSimple();
        m_chunkId = coldIndexBegin + m_chunkNum * m_pointsPerChunk;
    }
    else
    {
        const std::size_t sparseFirstSpan(
                pointsAtDepth(
                    m_structure.dimensions(),
                    m_structure.sparseDepthBegin()).getSimple());

        const std::size_t chunksPerSparseDepth(
            sparseFirstSpan / basePointsPerChunk);

        const std::size_t sparseDepthCount(
                m_depth - m_structure.sparseDepthBegin());

        m_pointsPerChunk =
            Id(basePointsPerChunk) *
            binaryPow(m_structure.dimensions(), sparseDepthCount);

        const Id coldIndexSpan(sparseIndexBegin - coldIndexBegin);
        const Id numColdChunks(coldIndexSpan / basePointsPerChunk);

        const Id prevLevelsChunkCount(
                numColdChunks +
                chunksPerSparseDepth * sparseDepthCount);

        const Id levelOffset(index - levelIndex);
        const auto divMod(levelOffset.divMod(m_pointsPerChunk));

        m_chunkNum = (prevLevelsChunkCount + divMod.first).getSimple();
        m_chunkOffset = divMod.second.getSimple();
        m_chunkId = levelIndex + divMod.first * m_pointsPerChunk;
    }
}

Id ChunkInfo::calcParentId(
        const Structure& structure,
        const Id& index,
        const std::size_t depth)
{
    if (index == structure.baseIndexBegin())
    {
        throw std::runtime_error("Base chunk has no parent");
    }

    const Id upOne(index >> 2);

    if (depth > structure.sparseDepthBegin())
    {
        return upOne;
    }
    else if (depth > structure.coldDepthBegin())
    {
        const auto& coldIndexBegin(structure.coldIndexBegin());
        const std::size_t ppc(structure.basePointsPerChunk());

        return coldIndexBegin + (upOne - coldIndexBegin) / ppc * ppc;
    }
    else return structure.baseIndexBegin();
}

Id ChunkInfo::calcLevelIndex(
        const std::size_t dimensions,
        const std::size_t depth)
{
    return (binaryPow(dimensions, depth) - 1) / ((1ULL << dimensions) - 1);
}

Id ChunkInfo::pointsAtDepth(
        const std::size_t dimensions,
        const std::size_t depth)
{
    return binaryPow(dimensions, depth);
}

Id ChunkInfo::binaryPow(
        const std::size_t baseLog2,
        const std::size_t exp)
{
    return Id(1) << (exp * baseLog2);
}

std::size_t ChunkInfo::logN(std::size_t val, std::size_t n)
{
    assert(n == 4 || n == 8);
    return log2(val) / log2(n);
}

std::size_t ChunkInfo::isPerfectLogN(std::size_t val, std::size_t n)
{
    return (1ULL << logN(val, n) * log2(n)) == val;
}

Structure::Structure(const Json::Value& json)
    : Structure(
            json["nullDepth"].asUInt64(),
            json["baseDepth"].asUInt64(),
            json["coldDepth"].asUInt64(),
            json["pointsPerChunk"].asUInt64(),
            2, /*
            json.isMember("dimensions") ?
                json["dimensions"].asUInt64() :
                (json["type"].asString() == "octree" ? 3 : 2),
            */
            json["numPointsHint"].asUInt64(),
            true, /*
            json.isMember("tubular") ?
                json["tubular"].asBool() :
                json["type"].asString() == "hybrid",
            */
            true, // json["dynamicChunks"].asBool(),
            json["prefixIds"].asBool(),
            json.isMember("mappedDepth") ?
                json["mappedDepth"].asUInt64() : json["sparseDepth"].asUInt64(),
            json["startDepth"].asUInt64(),
            json["sparseDepth"].asUInt64(),
            json["bumpDepth"].asUInt64())
{ }

Structure::Structure(
        const std::size_t nullDepth,
        const std::size_t baseDepth,
        const std::size_t coldDepth,
        const std::size_t pointsPerChunk,
        const std::size_t dimensions,
        const std::size_t numPointsHint,
        const bool tubular,
        const bool dynamicChunks,
        const bool prefixIds,
        const std::size_t mappedDepth,
        const std::size_t startDepth,
        const std::size_t sparseDepth,
        const std::size_t bumpDepth)
    // Various.
    : m_tubular(tubular)
    , m_dynamicChunks(dynamicChunks)
    , m_prefixIds(prefixIds)
    , m_dimensions(dimensions)
    , m_factor(1ULL << m_dimensions)
    , m_numPointsHint(numPointsHint)

    // Chunk-related.
    , m_pointsPerChunk(shallow ? 256 : pointsPerChunk)
    , m_nominalChunkDepth(ChunkInfo::logN(m_pointsPerChunk, m_factor))
    , m_nominalChunkIndex(
            ChunkInfo::calcLevelIndex(
                m_dimensions,
                m_nominalChunkDepth).getSimple())

    // Depths.
    , m_nullDepthBegin(0)
    , m_nullDepthEnd(shallow ? 0 : nullDepth)
    , m_baseDepthBegin(m_nullDepthEnd)
    , m_baseDepthEnd(shallow ? 4 : std::max({
                m_baseDepthBegin, baseDepth, m_nominalChunkDepth }))
    , m_coldDepthBegin(m_baseDepthEnd)
    , m_coldDepthEnd(coldDepth ? std::max(m_coldDepthBegin, coldDepth) : 0)
    , m_sparseDepthBegin(sparseDepth)
    , m_mappedDepthBegin(mappedDepth)
    , m_startDepth(startDepth)
    , m_bumpDepth(bumpDepth)

    // Indices.
    , m_nullIndexBegin(0)
    , m_nullIndexEnd(ChunkInfo::calcLevelIndex(dimensions, m_nullDepthEnd))
    , m_baseIndexBegin(m_nullIndexEnd)
    , m_baseIndexEnd(ChunkInfo::calcLevelIndex(dimensions, m_baseDepthEnd))
    , m_coldIndexBegin(m_baseIndexEnd)
    , m_coldIndexEnd(m_coldDepthEnd ?
            ChunkInfo::calcLevelIndex(dimensions, m_coldDepthEnd) : 0)
    , m_sparseIndexBegin(
            ChunkInfo::calcLevelIndex(dimensions, m_sparseDepthBegin))
    , m_mappedIndexBegin(
            ChunkInfo::calcLevelIndex(dimensions, m_mappedDepthBegin))
{
    if (shallow)
    {
        std::cout << "Using shallow test configuration" << std::endl;
    }

    if (m_baseDepthEnd < 4)
    {
        throw std::runtime_error("Base depth too small");
    }

    if (!m_pointsPerChunk && hasCold())
    {
        throw std::runtime_error(
                "Points per chunk not specified, but a cold depth was given.");
    }

    if (hasCold() && !ChunkInfo::isPerfectLogN(m_pointsPerChunk, m_factor))
    {
        throw std::runtime_error(
                "Invalid chunk specification - "
                "must be of the form 4^n for quadtree, or 8^n for octree");
    }

    const std::size_t activeMinDepth(
            m_bumpDepth ? m_bumpDepth : m_coldDepthBegin);

    const std::size_t activeNumPointsHint(
            std::max<std::size_t>(m_numPointsHint, 10000000));

    if (!mappedDepth)
    {
        m_mappedDepthBegin =
            std::ceil(std::log2(activeNumPointsHint) / std::log2(m_factor));
    }

    m_mappedDepthBegin = std::max(m_mappedDepthBegin, activeMinDepth);

    if (!sparseDepth)
    {
        m_sparseDepthBegin = m_mappedDepthBegin;
    }

    m_sparseDepthBegin = std::max(m_sparseDepthBegin, m_mappedDepthBegin);

    m_sparseIndexBegin =
        ChunkInfo::calcLevelIndex(m_dimensions, m_sparseDepthBegin);

    m_mappedIndexBegin =
        ChunkInfo::calcLevelIndex(m_dimensions, m_mappedDepthBegin);

    m_maxChunksPerDepth = 1;
    for (auto i(m_nominalChunkDepth); i < m_sparseDepthBegin; ++i)
    {
        m_maxChunksPerDepth *= m_factor;
    }
}

Json::Value Structure::toJson() const
{
    Json::Value json;

    json["nullDepth"] = static_cast<Json::UInt64>(nullDepthEnd());
    json["baseDepth"] = static_cast<Json::UInt64>(baseDepthEnd());
    json["coldDepth"] = static_cast<Json::UInt64>(coldDepthEnd());
    json["sparseDepth"] = static_cast<Json::UInt64>(sparseDepthBegin());
    json["mappedDepth"] = static_cast<Json::UInt64>(mappedDepthBegin());
    json["pointsPerChunk"] = static_cast<Json::UInt64>(basePointsPerChunk());
    json["dimensions"] = static_cast<Json::UInt64>(dimensions());
    json["numPointsHint"] = static_cast<Json::UInt64>(numPointsHint());
    json["tubular"] = m_tubular;
    json["dynamicChunks"] = m_dynamicChunks;
    json["prefixIds"] = m_prefixIds;

    if (m_startDepth)
    {
        json["startDepth"] = static_cast<Json::UInt64>(m_startDepth);
    }

    if (m_bumpDepth)
    {
        if (m_bumpDepth == m_baseDepthEnd)
        {
            // Save metadata as if the base depth end was never increased.
            json["baseDepth"] = static_cast<Json::UInt64>(m_bumpDepth);
        }
        else
        {
            json["bumpDepth"] = static_cast<Json::UInt64>(m_bumpDepth);
        }
    }

    return json;
}

void Structure::unbump()
{
    m_baseDepthEnd = m_bumpDepth;
    m_coldDepthBegin = m_bumpDepth;
    m_baseIndexEnd = ChunkInfo::calcLevelIndex(m_dimensions, m_baseDepthEnd);
    m_coldIndexBegin = m_baseIndexEnd;
}

ChunkInfo Structure::getInfoFromNum(const std::size_t chunkNum) const
{
    Id chunkId(0);

    if (hasCold())
    {
        if (hasSparse() && dynamicChunks())
        {
            const Id endFixed(
                    ChunkInfo::calcLevelIndex(
                        m_dimensions,
                        m_sparseDepthBegin + 1));

            const Id fixedSpan(endFixed - m_coldIndexBegin);
            const Id fixedNum(fixedSpan / m_pointsPerChunk);

            if (chunkNum < fixedNum)
            {
                chunkId = m_coldIndexBegin + chunkNum * m_pointsPerChunk;
            }
            else
            {
                const Id leftover(chunkNum - fixedNum);

                const std::size_t chunksPerSparseDepth(
                        numChunksAtDepth(m_sparseDepthBegin));

                const std::size_t depth(
                        (m_sparseDepthBegin + 1 +
                            leftover / chunksPerSparseDepth).getSimple());

                const std::size_t chunkNumInDepth(
                        (leftover % chunksPerSparseDepth).getSimple());

                const Id depthIndexBegin(
                        ChunkInfo::calcLevelIndex(
                            m_dimensions,
                            depth));

                const Id depthChunkSize(
                        ChunkInfo::pointsAtDepth(m_dimensions, depth) /
                        chunksPerSparseDepth);

                chunkId = depthIndexBegin + chunkNumInDepth * depthChunkSize;
            }
        }
        else
        {
            chunkId = m_coldIndexBegin + chunkNum * m_pointsPerChunk;
        }
    }

    return ChunkInfo(*this, chunkId);
}

std::size_t Structure::numChunksAtDepth(const std::size_t depth) const
{
    std::size_t num(0);

    if (!hasSparse() || !dynamicChunks() || depth <= m_sparseDepthBegin)
    {
        const Id depthSpan(
                ChunkInfo::calcLevelIndex(m_dimensions, depth + 1) -
                ChunkInfo::calcLevelIndex(m_dimensions, depth));

        num = (depthSpan / m_pointsPerChunk).getSimple();
    }
    else
    {
        const Id sparseFirstSpan(
                ChunkInfo::pointsAtDepth(m_dimensions, m_sparseDepthBegin));

        num = (sparseFirstSpan / m_pointsPerChunk).getSimple();
    }

    return num;
}

} // namespace entwine


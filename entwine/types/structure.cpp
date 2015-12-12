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

namespace entwine
{

namespace
{
    std::size_t log2(std::size_t val)
    {
        return std::log2(val);
    }
}

ChunkInfo::ChunkInfo(const Structure& structure, const Id& index)
    : m_structure(structure)
    , m_index(index)
    , m_chunkId(0)
    , m_depth(calcDepth(m_structure.factor(), index))
    , m_chunkOffset(0)
    , m_chunkPoints(0)
    , m_chunkNum(0)
{
    const Id levelIndex(calcLevelIndex(m_structure.dimensions(), m_depth));
    const std::size_t baseChunkPoints(m_structure.baseChunkPoints());

    const Id& sparseIndexBegin(m_structure.sparseIndexBegin());
    const Id& coldIndexBegin(m_structure.coldIndexBegin());

    if (!m_structure.dynamicChunks() || levelIndex <= sparseIndexBegin)
    {
        m_chunkPoints = baseChunkPoints;
        const auto divMod((m_index - coldIndexBegin).divMod(m_chunkPoints));
        m_chunkNum = divMod.first.getSimple();
        m_chunkOffset = divMod.second.getSimple();
        m_chunkId = coldIndexBegin + m_chunkNum * m_chunkPoints;
    }
    else
    {
        const std::size_t sparseFirstSpan(
                pointsAtDepth(
                    m_structure.dimensions(),
                    m_structure.sparseDepthBegin()).getSimple());

        const std::size_t chunksPerSparseDepth(
            sparseFirstSpan / baseChunkPoints);

        const std::size_t sparseDepthCount(
                m_depth - m_structure.sparseDepthBegin());

        m_chunkPoints =
            Id(baseChunkPoints) *
            binaryPow(m_structure.dimensions(), sparseDepthCount);

        const Id coldIndexSpan(sparseIndexBegin - coldIndexBegin);
        const Id numColdChunks(coldIndexSpan / baseChunkPoints);

        const Id prevLevelsChunkCount(
                numColdChunks +
                chunksPerSparseDepth * sparseDepthCount);

        const Id levelOffset(index - levelIndex);
        const auto divMod(levelOffset.divMod(m_chunkPoints));

        m_chunkNum = (prevLevelsChunkCount + divMod.first).getSimple();
        m_chunkOffset = divMod.second.getSimple();
        m_chunkId = levelIndex + divMod.first * m_chunkPoints;
    }
}

std::size_t ChunkInfo::calcDepth(
        const std::size_t factor,
        const Id& index)
{
    return log2(index * (factor - 1) + 1) / log2(factor);
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
    if (n != 4 && n != 8)
    {
        throw std::runtime_error("Invalid logN arg: " + std::to_string(n));
    }

    return log2(val) / log2(n);
}

std::size_t ChunkInfo::isPerfectLogN(std::size_t val, std::size_t n)
{
    return (1ULL << logN(val, n) * log2(n)) == val;
}

Structure::Structure(
        const std::size_t nullDepth,
        const std::size_t baseDepth,
        const std::size_t coldDepth,
        const std::size_t chunkPoints,
        const std::size_t dimensions,
        const std::size_t numPointsHint,
        const bool tubular,
        const bool dynamicChunks,
        const bool discardDuplicates,
        const bool prefixIds,
        const BBox* bbox,
        const std::pair<std::size_t, std::size_t> subset)
    : m_nullDepthBegin(0)
    , m_nullDepthEnd(nullDepth)
    , m_baseDepthBegin(m_nullDepthEnd)
    , m_baseDepthEnd(std::max(m_baseDepthBegin, baseDepth))
    , m_coldDepthBegin(m_baseDepthEnd)
    , m_coldDepthEnd(coldDepth ? std::max(m_coldDepthBegin, coldDepth) : 0)
    , m_sparseDepthBegin(0)
    , m_mappedDepthBegin(0)
    , m_sparseIndexBegin(0)
    , m_mappedIndexBegin(0)
    , m_chunkPoints(chunkPoints)
    , m_tubular(tubular)
    , m_dynamicChunks(dynamicChunks)
    , m_discardDuplicates(discardDuplicates)
    , m_prefixIds(prefixIds)
    , m_dimensions(dimensions)
    , m_factor(1ULL << m_dimensions)
    , m_numPointsHint(numPointsHint)
    , m_subset(subset.second ?
            new Subset(*this, bbox, subset.first, subset.second) : nullptr)
{
    loadIndexValues();
}

Structure::Structure(const Json::Value& json, const BBox& bbox)
    : m_nullDepthBegin(0)
    , m_nullDepthEnd(json["nullDepth"].asUInt64())
    , m_baseDepthBegin(m_nullDepthEnd)
    , m_baseDepthEnd(json["baseDepth"].asUInt64())
    , m_coldDepthBegin(m_baseDepthEnd)
    , m_coldDepthEnd(json["coldDepth"].asUInt64())
    , m_sparseDepthBegin(json["sparseDepth"].asUInt64())
    , m_mappedDepthBegin(json["mappedDepth"].asUInt64())
    , m_sparseIndexBegin(0)
    , m_mappedIndexBegin(0)
    , m_chunkPoints(json["chunkPoints"].asUInt64())
    , m_tubular(json["tubular"].asBool())
    , m_dynamicChunks(json["dynamicChunks"].asBool())
    , m_discardDuplicates(json["discardDuplicates"].asBool())
    , m_prefixIds(json["prefixIds"].asBool())
    , m_dimensions(json["dimensions"].asUInt64())
    , m_factor(1ULL << m_dimensions)
    , m_numPointsHint(json["numPointsHint"].asUInt64())
    , m_subset(
            json.isMember("subset") ?
                new Subset(*this, bbox, json["subset"]) : nullptr)
{
    loadIndexValues();
}

Structure::Structure(const Structure& other)
{
    m_nullDepthBegin = other.m_nullDepthBegin;
    m_nullDepthEnd = other.m_nullDepthEnd;
    m_baseDepthBegin = other.m_baseDepthBegin;
    m_baseDepthEnd = other.m_baseDepthEnd;
    m_coldDepthBegin = other.m_coldDepthBegin;
    m_coldDepthEnd = other.m_coldDepthEnd;
    m_sparseDepthBegin = other.m_sparseDepthBegin;
    m_mappedDepthBegin = other.m_mappedDepthBegin;

    m_nullIndexBegin = other.m_nullIndexBegin;
    m_nullIndexEnd = other.m_nullIndexEnd;
    m_baseIndexBegin = other.m_baseIndexBegin;
    m_baseIndexEnd = other.m_baseIndexEnd;
    m_coldIndexBegin = other.m_coldIndexBegin;
    m_coldIndexEnd = other.m_coldIndexEnd;
    m_sparseIndexBegin = other.m_sparseIndexBegin;
    m_mappedIndexBegin = other.m_mappedIndexBegin;

    m_chunkPoints = other.m_chunkPoints;

    m_nominalChunkIndex = other.m_nominalChunkIndex;
    m_nominalChunkDepth = other.m_nominalChunkDepth;

    m_tubular = other.m_tubular;
    m_dynamicChunks = other.m_dynamicChunks;
    m_discardDuplicates = other.m_discardDuplicates;
    m_prefixIds = other.m_prefixIds;

    m_dimensions = other.m_dimensions;
    m_factor = other.m_factor;
    m_numPointsHint = other.m_numPointsHint;

    m_subset.reset(other.m_subset ? new Subset(*other.m_subset) : nullptr);
}

void Structure::loadIndexValues()
{
    if (m_subset)
    {
        if (m_nullDepthEnd < m_subset->minNullDepth())
        {
            std::cout << "Bumping null depth to accommodate subset" <<
                std::endl;
        }

        m_nullDepthEnd = std::max(m_nullDepthEnd, m_subset->minNullDepth());
        m_baseDepthBegin = m_nullDepthEnd;
        m_baseDepthEnd = std::max(m_baseDepthBegin, m_baseDepthEnd);
        m_coldDepthBegin = m_baseDepthEnd;

        // Only snap coldDepthEnd upward if it's non-zero, since a zero value
        // means that the index is lossless.
        if (m_coldDepthEnd)
            m_coldDepthEnd = std::max(m_coldDepthBegin, m_coldDepthEnd);

        if (hasCold())
        {
            bool done(false);
            std::size_t bumped(0);

            do
            {
                const std::size_t coldFirstSpan(
                        ChunkInfo::pointsAtDepth(
                            m_dimensions,
                            m_coldDepthBegin).getSimple());

                std::size_t splits(m_factor);
                while (splits < m_subset->of()) splits *= m_factor;

                if (
                        (coldFirstSpan / m_chunkPoints) < splits ||
                        (coldFirstSpan / m_chunkPoints) % splits)
                {
                    ++m_baseDepthEnd;
                    ++m_coldDepthBegin;

                    if (++bumped > 8)
                    {
                        throw std::runtime_error(
                                "Base depth is far too shallow for the "
                                "specified subset");
                    }
                }
                else
                {
                    done = true;
                }
            }
            while (!done);

            if (bumped)
            {
                std::cout << "Bumping cold depth to accommodate subset" <<
                    std::endl;
            }
        }
    }

    if (m_baseDepthEnd < 4)
    {
        throw std::runtime_error("Base depth too small");
    }

    if (!m_chunkPoints && hasCold())
    {
        throw std::runtime_error(
                "Points per chunk not specified, but a cold depth was given.");
    }

    if (hasCold() && !ChunkInfo::isPerfectLogN(m_chunkPoints, m_factor))
    {
        throw std::runtime_error(
                "Invalid chunk specification - "
                "must be of the form 4^n for quadtree, or 8^n for octree");
    }

    m_nominalChunkDepth = ChunkInfo::logN(m_chunkPoints, m_factor);
    m_nominalChunkIndex =
        ChunkInfo::calcLevelIndex(
                m_dimensions,
                m_nominalChunkDepth).getSimple();

    m_nullIndexBegin = 0;
    m_nullIndexEnd = ChunkInfo::calcLevelIndex(m_dimensions, m_nullDepthEnd);
    m_baseIndexBegin = m_nullIndexEnd;
    m_baseIndexEnd = ChunkInfo::calcLevelIndex(m_dimensions, m_baseDepthEnd);
    m_coldIndexBegin = m_baseIndexEnd;
    m_coldIndexEnd =
        m_coldDepthEnd ?
            ChunkInfo::calcLevelIndex(m_dimensions, m_coldDepthEnd) : 0;

    if (m_numPointsHint)
    {
        if (!m_sparseDepthBegin)
        {
            if (m_dimensions == 2)
            {
                m_sparseDepthBegin =
                    std::ceil(std::log2(m_numPointsHint) / std::log2(m_factor));
            }
            else
            {
                m_sparseDepthBegin =
                    std::ceil(std::log2(m_numPointsHint) / std::log2(6));
            }

            m_sparseDepthBegin = std::max(m_sparseDepthBegin, m_coldDepthBegin);

            m_mappedDepthBegin =
                std::ceil(std::log2(m_numPointsHint) / std::log2(m_factor));
            m_mappedDepthBegin = std::max(m_mappedDepthBegin, m_coldDepthBegin);
        }

        m_sparseIndexBegin =
            ChunkInfo::calcLevelIndex(m_dimensions, m_sparseDepthBegin);

        m_mappedIndexBegin =
            ChunkInfo::calcLevelIndex(m_dimensions, m_mappedDepthBegin);
    }
    else
    {
        std::cout <<
            "No numPointsHint provided.  " <<
            "For more than a few billion points, " <<
            "there may be a large performance hit." << std::endl;
    }
}

Json::Value Structure::toJson() const
{
    Json::Value json;

    json["nullDepth"] = static_cast<Json::UInt64>(nullDepthEnd());
    json["baseDepth"] = static_cast<Json::UInt64>(baseDepthEnd());
    json["coldDepth"] = static_cast<Json::UInt64>(coldDepthEnd());
    json["sparseDepth"] = static_cast<Json::UInt64>(sparseDepthBegin());
    json["chunkPoints"] = static_cast<Json::UInt64>(baseChunkPoints());
    json["dimensions"] = static_cast<Json::UInt64>(dimensions());
    json["numPointsHint"] = static_cast<Json::UInt64>(numPointsHint());
    json["tubular"] = m_tubular;
    json["dynamicChunks"] = m_dynamicChunks;
    json["discardDuplicates"] = m_discardDuplicates;
    json["prefixIds"] = m_prefixIds;

    if (m_subset) json["subset"] = m_subset->toJson();

    return json;
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
            const Id fixedNum(fixedSpan / m_chunkPoints);

            if (chunkNum < fixedNum)
            {
                chunkId = m_coldIndexBegin + chunkNum * m_chunkPoints;
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
            chunkId = m_coldIndexBegin + chunkNum * m_chunkPoints;
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

        num = (depthSpan / m_chunkPoints).getSimple();
    }
    else
    {
        const Id sparseFirstSpan(
                ChunkInfo::pointsAtDepth(m_dimensions, m_sparseDepthBegin));

        num = (sparseFirstSpan / m_chunkPoints).getSimple();
    }

    return num;
}

void Structure::makeWhole()
{
    m_subset.reset();
}

std::string Structure::subsetPostfix() const
{
    std::string postfix("");
    if (m_subset) postfix += "-" + std::to_string(m_subset->id());
    return postfix;
}

} // namespace entwine


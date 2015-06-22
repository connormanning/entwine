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

#include <cmath>
#include <iostream>

#include <entwine/tree/roller.hpp>

namespace entwine
{

namespace
{
    // TODO Without using a big-integer type, we can't go beyond depth 32.
    const std::size_t maxDepth(31);

    std::size_t log2(std::size_t val)
    {
        // TODO.
        return std::floor(std::log2(val));
    }
}

ChunkInfo::ChunkInfo(const Structure& structure, const std::size_t index)
    : m_structure(structure)
    , m_index(index)
    , m_depth(calcDepth(m_structure.factor(), index))
    , m_chunkId(0)
    , m_chunkOffset(0)
    , m_chunkPoints(0)
    , m_chunkNum(0)
{
    const std::size_t levelIndex(
            calcLevelIndex(m_structure.dimensions(), m_depth));

    const std::size_t sparseIndexBegin(m_structure.sparseIndexBegin());
    const std::size_t baseChunkPoints(m_structure.baseChunkPoints());

    if (!m_structure.dynamicChunks() || levelIndex < sparseIndexBegin)
    {
        const std::size_t coldIndexBegin(m_structure.coldIndexBegin());

        m_chunkPoints = baseChunkPoints;
        m_chunkNum =    (m_index - coldIndexBegin) / m_chunkPoints;
        m_chunkOffset = (m_index - coldIndexBegin) % m_chunkPoints;
        m_chunkId = coldIndexBegin + m_chunkNum * m_chunkPoints;
    }
    else
    {
        const std::size_t sparseFirstSpan(
                pointsAtDepth(
                    m_structure.dimensions(),
                    m_structure.sparseDepthBegin()));

        const std::size_t chunksPerSparseDepth(
            sparseFirstSpan / baseChunkPoints);

        const std::size_t sparseDepthCount(
                m_depth - m_structure.sparseDepthBegin());

        m_chunkPoints =
            baseChunkPoints *
            binaryPow(m_structure.dimensions(), sparseDepthCount);

        const std::size_t coldIndexSpan(
                m_structure.sparseIndexBegin() - m_structure.coldIndexBegin());

        const std::size_t numColdChunks(coldIndexSpan / baseChunkPoints);

        const std::size_t prevLevelsChunkCount(
                numColdChunks +
                chunksPerSparseDepth * sparseDepthCount);

        const std::size_t levelOffset(index - levelIndex);

        m_chunkNum = prevLevelsChunkCount + levelOffset / m_chunkPoints;
        m_chunkOffset = levelOffset % m_chunkPoints;
        m_chunkId = levelIndex + (levelOffset / m_chunkPoints) * m_chunkPoints;
    }
}

std::size_t ChunkInfo::calcDepth(
        const std::size_t factor,
        const std::size_t index)
{
    return log2(index * (factor - 1) + 1) / log2(factor);
}

std::size_t ChunkInfo::calcLevelIndex(
        const std::size_t dimensions,
        const std::size_t depth)
{
    return (binaryPow(dimensions, depth) - 1) / ((1ULL << dimensions) - 1);
}

std::size_t ChunkInfo::pointsAtDepth(
        const std::size_t dimensions,
        const std::size_t depth)
{
    return binaryPow(dimensions, depth);
}

std::size_t ChunkInfo::binaryPow(
        const std::size_t baseLog2,
        const std::size_t exp)
{
    return 1ULL << (exp * baseLog2);
}

Structure::Structure(
        const std::size_t nullDepth,
        const std::size_t baseDepth,
        const std::size_t coldDepth,
        const std::size_t chunkPoints,
        const std::size_t dimensions,
        const std::size_t numPointsHint,
        const bool dynamicChunks,
        const std::pair<std::size_t, std::size_t> subset)
    : m_nullDepthBegin(0)
    , m_nullDepthEnd(std::min(nullDepth, maxDepth))
    , m_baseDepthBegin(m_nullDepthEnd)
    , m_baseDepthEnd(std::min(std::max(m_baseDepthBegin, baseDepth), maxDepth))
    , m_coldDepthBegin(m_baseDepthEnd)
    , m_coldDepthEnd(std::min(std::max(m_coldDepthBegin, coldDepth), maxDepth))
    , m_sparseDepthBegin(0)
    , m_sparseIndexBegin(0)
    , m_chunkPoints(chunkPoints)
    , m_dynamicChunks(dynamicChunks)
    , m_dimensions(dimensions)
    , m_factor(1ULL << m_dimensions)
    , m_numPointsHint(numPointsHint)
    , m_subset(subset)
{
    if (m_dimensions != 2)
    {
        throw std::runtime_error("Only quadtree supported now");
    }

    loadIndexValues();
}

Structure::Structure(const Json::Value& json)
    : m_nullDepthBegin(0)
    , m_nullDepthEnd(json["nullDepth"].asUInt64())
    , m_baseDepthBegin(m_nullDepthEnd)
    , m_baseDepthEnd(json["baseDepth"].asUInt64())
    , m_coldDepthBegin(m_baseDepthEnd)
    , m_coldDepthEnd(json["coldDepth"].asUInt64())
    , m_sparseDepthBegin(0)
    , m_sparseIndexBegin(0)
    , m_chunkPoints(json["chunkPoints"].asUInt64())
    , m_dynamicChunks(json["dynamicChunks"].asBool())
    , m_dimensions(json["dimensions"].asUInt64())
    , m_factor(1ULL << m_dimensions)
    , m_numPointsHint(json["numPointsHint"].asUInt64())
    , m_subset({ json["subset"][0].asUInt64(), json["subset"][1].asUInt64() })
{
    loadIndexValues();
}

void Structure::loadIndexValues()
{
    const std::size_t coldFirstSpan(
            ChunkInfo::pointsAtDepth(
                m_dimensions,
                m_coldDepthBegin));

    if (m_baseDepthEnd < 4)
    {
        throw std::runtime_error("Base depth too small");
    }

    if (!m_chunkPoints && hasCold())
    {
        // TODO Assign a default?
        throw std::runtime_error(
                "Points per chunk not specified, but a cold depth was given.");
    }

    if (hasCold())
    {
        if (!m_chunkPoints || (coldFirstSpan % m_chunkPoints))
        {
            throw std::runtime_error("Invalid chunk specification");
        }
    }

    m_nullIndexBegin = 0;
    m_nullIndexEnd = ChunkInfo::calcLevelIndex(m_dimensions, m_nullDepthEnd);
    m_baseIndexBegin = m_nullIndexEnd;
    m_baseIndexEnd = ChunkInfo::calcLevelIndex(m_dimensions, m_baseDepthEnd);
    m_coldIndexBegin = m_baseIndexEnd;
    m_coldIndexEnd = ChunkInfo::calcLevelIndex(m_dimensions, m_coldDepthEnd);

    if (m_numPointsHint)
    {
        m_sparseDepthBegin =
            std::max(
                    log2(m_numPointsHint) / log2(m_factor) + 1,
                    m_coldDepthBegin);

        m_sparseIndexBegin =
            ChunkInfo::calcLevelIndex(
                    m_dimensions,
                    m_sparseDepthBegin);
    }
    else
    {
        std::cout <<
            "No numPointsHint provided.  " <<
            "For more than a few billion points, " <<
            "there may be a large performance hit." << std::endl;
    }


    const std::size_t splits(m_subset.second);
    if (splits)
    {
        if (!m_nullDepthEnd || std::pow(4, m_nullDepthEnd) < splits)
        {
            throw std::runtime_error("Invalid null depth for requested subset");
        }

        if (!(splits == 4 || splits == 16 || splits == 64))
        {
            throw std::runtime_error("Invalid subset split");
        }

        if (m_subset.first >= m_subset.second)
        {
            throw std::runtime_error("Invalid subset identifier");
        }

        if (hasCold())
        {
            if (
                    (coldFirstSpan / m_chunkPoints) < splits ||
                    (coldFirstSpan / m_chunkPoints) % splits)
            {
                throw std::runtime_error("Invalid chunk size for this subset");
            }
        }
    }
}

Json::Value Structure::toJson() const
{
    Json::Value json;

    json["nullDepth"] = static_cast<Json::UInt64>(nullDepthEnd());
    json["baseDepth"] = static_cast<Json::UInt64>(baseDepthEnd());
    json["coldDepth"] = static_cast<Json::UInt64>(coldDepthEnd());
    json["chunkPoints"] = static_cast<Json::UInt64>(baseChunkPoints());
    json["dimensions"] = static_cast<Json::UInt64>(dimensions());
    json["numPointsHint"] = static_cast<Json::UInt64>(numPointsHint());
    json["dynamicChunks"] = m_dynamicChunks;
    json["subset"].append(static_cast<Json::UInt64>(m_subset.first));
    json["subset"].append(static_cast<Json::UInt64>(m_subset.second));

    return json;
}

std::size_t Structure::nullDepthBegin() const   { return m_nullDepthBegin; }
std::size_t Structure::nullDepthEnd() const     { return m_nullDepthEnd; }
std::size_t Structure::baseDepthBegin() const   { return m_baseDepthBegin; }
std::size_t Structure::baseDepthEnd() const     { return m_baseDepthEnd; }
std::size_t Structure::coldDepthBegin() const   { return m_coldDepthBegin; }
std::size_t Structure::coldDepthEnd() const     { return m_coldDepthEnd; }
std::size_t Structure::sparseDepthBegin() const { return m_sparseDepthBegin; }

std::size_t Structure::nullIndexBegin() const   { return m_nullIndexBegin; }
std::size_t Structure::nullIndexEnd() const     { return m_nullIndexEnd; }
std::size_t Structure::baseIndexBegin() const   { return m_baseIndexBegin; }
std::size_t Structure::baseIndexEnd() const     { return m_baseIndexEnd; }
std::size_t Structure::coldIndexBegin() const   { return m_coldIndexBegin; }
std::size_t Structure::coldIndexEnd() const     { return m_coldIndexEnd; }
std::size_t Structure::sparseIndexBegin() const { return m_sparseIndexBegin; }

std::size_t Structure::baseIndexSpan() const
{
    return m_baseIndexEnd - m_baseIndexBegin;
}

bool Structure::isWithinNull(const std::size_t index) const
{
    return index >= m_nullIndexBegin && index < m_nullIndexEnd;
}

bool Structure::isWithinBase(const std::size_t index) const
{
    return index >= m_baseIndexBegin && index < m_baseIndexEnd;
}

bool Structure::isWithinCold(const std::size_t index) const
{
    return index >= m_coldIndexBegin && index < m_coldIndexEnd;
}

bool Structure::hasNull() const
{
    return nullIndexEnd() > nullIndexBegin();
}

bool Structure::hasBase() const
{
    return baseIndexEnd() > baseIndexBegin();
}

bool Structure::hasCold() const
{
    return lossless() || coldIndexEnd() > coldIndexBegin();
}

bool Structure::hasSparse() const
{
    return m_sparseIndexBegin != 0;
}

bool Structure::inRange(const std::size_t index) const
{
    return lossless() || index < m_coldIndexEnd;
}

bool Structure::lossless() const
{
    return m_coldDepthEnd == 0;
}

bool Structure::dynamicChunks() const
{
    return m_dynamicChunks;
}

ChunkInfo Structure::getInfo(const std::size_t index) const
{
    return ChunkInfo(*this, index);
}

ChunkInfo Structure::getInfoFromNum(const std::size_t chunkNum) const
{
    std::size_t chunkId(0);

    if (hasCold())
    {
        if (hasSparse() && dynamicChunks())
        {
            const std::size_t endFixed(
                    ChunkInfo::calcLevelIndex(
                        m_dimensions,
                        m_sparseDepthBegin + 1));

            const std::size_t fixedSpan(endFixed - m_coldIndexBegin);
            const std::size_t fixedNum(fixedSpan / m_chunkPoints);

            if (chunkNum < fixedNum)
            {
                chunkId = m_coldIndexBegin + chunkNum * m_chunkPoints;
            }
            else
            {
                const std::size_t leftover(chunkNum - fixedNum);

                const std::size_t chunksPerSparseDepth(
                        numChunksAtDepth(m_sparseDepthBegin));

                const std::size_t depth(
                        m_sparseDepthBegin + 1 +
                        leftover / chunksPerSparseDepth);

                const std::size_t chunkNumInDepth(
                        leftover % chunksPerSparseDepth);

                const std::size_t depthIndexBegin(
                        ChunkInfo::calcLevelIndex(m_dimensions, depth));

                const std::size_t depthChunkSize(
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
        const std::size_t depthSpan(
                ChunkInfo::calcLevelIndex(m_dimensions, depth + 1) -
                ChunkInfo::calcLevelIndex(m_dimensions, depth));

        num = depthSpan / m_chunkPoints;
    }
    else
    {
        const std::size_t sparseFirstSpan(
                ChunkInfo::pointsAtDepth(m_dimensions, m_sparseDepthBegin));

        num = sparseFirstSpan / m_chunkPoints;
    }

    return num;
}

std::size_t Structure::baseChunkPoints() const
{
    return m_chunkPoints;
}

std::size_t Structure::dimensions() const
{
    return m_dimensions;
}

std::size_t Structure::factor() const
{
    return m_factor;
}

bool Structure::is3d() const
{
    return m_dimensions == 3;
}

std::size_t Structure::numPointsHint() const
{
    return m_numPointsHint;
}

bool Structure::isSubset() const
{
    return m_subset.second != 0;
}

std::pair<std::size_t, std::size_t> Structure::subset() const
{
    return m_subset;
}

void Structure::makeWhole()
{
    m_subset = { 0, 0 };
}

std::unique_ptr<BBox> Structure::subsetBBox(const BBox& full) const
{
    std::unique_ptr<BBox> result;

    Roller roller(full, *this);
    std::size_t times(0);

    // TODO Very temporary.
    if (m_subset.second == 4) times = 1;
    else if (m_subset.second == 16) times = 2;
    else if (m_subset.second == 64) times = 3;
    else throw std::runtime_error("Invalid subset split");

    if (times)
    {
        for (std::size_t i(0); i < times; ++i)
        {
            Roller::Dir dir(
                    static_cast<Roller::Dir>(m_subset.first >> (i * 2) & 0x03));

            if (dir == Roller::Dir::nw) roller.goNw();
            else if (dir == Roller::Dir::ne) roller.goNe();
            else if (dir == Roller::Dir::sw) roller.goSw();
            else roller.goSe();
        }

        result.reset(new BBox(roller.bbox()));
    }
    else
    {
        throw std::runtime_error("Invalid magnification subset");
    }

    return result;
}

std::string Structure::subsetPostfix() const
{
    std::string postfix("");

    if (isSubset()) postfix += "-" + std::to_string(m_subset.first);

    return postfix;
}

} // namespace entwine


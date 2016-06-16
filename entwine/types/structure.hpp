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

#include <entwine/third/json/json.hpp>
#include <entwine/types/point-pool.hpp>

namespace entwine
{

class Structure;
class Subset;

class ChunkInfo
{
public:
    ChunkInfo(const Structure& structure, const Id& index);

    static std::size_t calcDepth(std::size_t factor, const Id& index);

    static Id calcLevelIndex(
            std::size_t dimensions,
            std::size_t depth);

    static Id pointsAtDepth(
            std::size_t dimensions,
            std::size_t depth);

    // Note: Expects to receive number of dimensions, not factor.  Result will
    // be equal to std::pow(factor, exp), but we avoid a log2() call.
    static Id binaryPow(std::size_t baseLog2, std::size_t exp);

    static std::size_t logN(std::size_t val, std::size_t n);
    static std::size_t isPerfectLogN(std::size_t val, std::size_t n);

    std::size_t depth()             const { return m_depth; }
    const Id&   chunkId()           const { return m_chunkId; }
    const Id&   chunkOffset()       const { return m_chunkOffset; }
    const Id&   pointsPerChunk()    const { return m_pointsPerChunk; }
    std::size_t chunkNum()          const { return m_chunkNum; }

private:
    const Structure& m_structure;

    Id m_index;
    Id m_chunkId;
    std::size_t m_depth;
    Id m_chunkOffset;
    Id m_pointsPerChunk;
    std::size_t m_chunkNum;
};

class Structure
{
    friend class Subset;

public:
    Structure(
            std::size_t nullDepth,
            std::size_t baseDepth,
            std::size_t coldDepth,
            std::size_t pointsPerChunk,
            std::size_t dimensions,
            std::size_t numPointsHint,
            bool tubular,
            bool dynamicChunks,
            bool prefixIds,
            std::size_t sparseDepth = 0,
            std::size_t startDepth = 0);

    Structure(const Json::Value& json);

    Json::Value toJson() const;

    std::size_t nullDepthBegin() const   { return m_nullDepthBegin; }
    std::size_t nullDepthEnd() const     { return m_nullDepthEnd; }
    std::size_t baseDepthBegin() const   { return m_baseDepthBegin; }
    std::size_t baseDepthEnd() const     { return m_baseDepthEnd; }
    std::size_t coldDepthBegin() const   { return m_coldDepthBegin; }
    std::size_t coldDepthEnd() const     { return m_coldDepthEnd; }
    std::size_t sparseDepthBegin() const { return m_sparseDepthBegin; }
    std::size_t startDepth() const       { return m_startDepth; }

    const Id& nullIndexBegin() const   { return m_nullIndexBegin; }
    const Id& nullIndexEnd() const     { return m_nullIndexEnd; }
    const Id& baseIndexBegin() const   { return m_baseIndexBegin; }
    const Id& baseIndexEnd() const     { return m_baseIndexEnd; }
    const Id& coldIndexBegin() const   { return m_coldIndexBegin; }
    const Id& coldIndexEnd() const     { return m_coldIndexEnd; }
    const Id& sparseIndexBegin() const { return m_sparseIndexBegin; }

    std::size_t baseIndexSpan() const
    {
        return (m_baseIndexEnd - m_baseIndexBegin).getSimple();
    }

    bool isWithinNull(std::size_t depth) const
    {
        return depth >= m_nullDepthBegin && depth < m_nullDepthEnd;
    }

    bool isWithinBase(std::size_t depth) const
    {
        return depth >= m_baseDepthBegin && depth < m_baseDepthEnd;
    }

    bool isWithinCold(std::size_t depth) const
    {
        return
            depth >= m_coldDepthBegin && (lossless() || depth < m_coldDepthEnd);
    }

    bool hasNull() const    { return nullIndexEnd() > nullIndexBegin(); }
    bool hasBase() const    { return baseIndexEnd() > baseIndexBegin(); }
    bool hasSparse() const  { return m_sparseIndexBegin != 0; }
    bool hasCold() const
    {
        return lossless() || coldDepthEnd() > coldDepthBegin();
    }

    bool inRange(std::size_t depth) const
    {
        return lossless() || depth < m_coldDepthEnd;
    }

    bool lossless() const           { return m_coldDepthEnd == 0; }
    bool tubular() const            { return m_tubular; }
    bool dynamicChunks() const      { return m_dynamicChunks; }
    bool prefixIds() const          { return m_prefixIds; }
    bool is3d() const               { return m_dimensions == 3; }

    ChunkInfo getInfo(const Id& index) const { return ChunkInfo(*this, index); }
    std::size_t numPointsHint() const { return m_numPointsHint; }

    std::string typeString() const
    {
        if (m_tubular) return "hybrid";
        else if (is3d()) return "octree";
        else return "quadtree";
    }

    ChunkInfo getInfoFromNum(std::size_t chunkNum) const;
    std::size_t numChunksAtDepth(std::size_t depth) const;

    std::size_t basePointsPerChunk() const { return m_pointsPerChunk; }
    std::size_t dimensions() const { return m_dimensions; }
    std::size_t factor() const { return m_factor; } // Quadtree: 4, octree: 8.

    std::size_t nominalChunkIndex() const { return m_nominalChunkIndex; }
    std::size_t nominalChunkDepth() const { return m_nominalChunkDepth; }

    std::string maybePrefix(const Id& id) const
    {
        if (prefixIds())
        {
            // Prefix the ID with 4 base32-encoded characters based on its hash.
            // Useful for S3 sharding performance.
            const std::size_t hash(std::hash<Id>()(id));

            std::string prefix;
            char c(0);

            for (std::size_t i(0); i < 4; ++i)
            {
                c = (hash >> (i * 5)) & 0x1F;

                if (c < 26) prefix.push_back(c + 97);
                else prefix.push_back(c + 22);
            }

            return prefix + '-' + id.str();
        }
        else
        {
            return id.str();
        }
    }

    std::size_t maxChunksPerDepth() const { return m_maxChunksPerDepth; }

private:
    // Redundant values (since the beginning of one level is equal to the end
    // of the previous level) help to maintain a logical distinction between
    // layers.
    std::size_t m_nullDepthBegin;
    std::size_t m_nullDepthEnd;
    std::size_t m_baseDepthBegin;
    std::size_t m_baseDepthEnd;
    std::size_t m_coldDepthBegin;
    std::size_t m_coldDepthEnd;
    std::size_t m_sparseDepthBegin;
    std::size_t m_startDepth;

    Id m_nullIndexBegin;
    Id m_nullIndexEnd;
    Id m_baseIndexBegin;
    Id m_baseIndexEnd;
    Id m_coldIndexBegin;
    Id m_coldIndexEnd;
    Id m_sparseIndexBegin;

    bool m_tubular;
    bool m_dynamicChunks;
    bool m_discardDuplicates;
    bool m_prefixIds;

    std::size_t m_dimensions;
    std::size_t m_factor;
    std::size_t m_numPointsHint;

    std::size_t m_maxChunksPerDepth;

    std::size_t m_pointsPerChunk;

    // Chunk ID that spans the full bounds.  May not be an actual chunk since
    // it may reside in the base branch.
    std::size_t m_nominalChunkDepth;
    std::size_t m_nominalChunkIndex;
};

} // namespace entwine


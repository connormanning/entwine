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

#include <entwine/third/bigint/little-big-int.hpp>
#include <entwine/third/json/json.h>

namespace entwine
{

typedef BigUint Id;

class BBox;
class Structure;

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

    std::size_t depth()         const { return m_depth; }
    const Id&   chunkId()       const { return m_chunkId; }
    std::size_t chunkOffset()   const { return m_chunkOffset; }
    std::size_t chunkPoints()   const { return m_chunkPoints; }
    std::size_t chunkNum()      const { return m_chunkNum; }

private:
    const Structure& m_structure;

    Id m_index;
    Id m_chunkId;
    std::size_t m_depth;
    std::size_t m_chunkOffset;
    std::size_t m_chunkPoints;
    std::size_t m_chunkNum;
};

class Structure
{
public:
    Structure(
            std::size_t nullDepth,
            std::size_t baseDepth,
            std::size_t coldDepth,
            std::size_t chunkPoints,
            std::size_t dimensions,
            std::size_t numPointsHint,
            bool dynamicChunks,
            std::pair<std::size_t, std::size_t> subset = { 0, 0 });

    Structure(
            std::size_t nullDepth,
            std::size_t baseDepth,
            std::size_t chunkPoints,
            std::size_t dimensions,
            std::size_t numPointsHint,
            bool dynamicChunks,
            std::pair<std::size_t, std::size_t> subset = { 0, 0 });

    Structure(const Json::Value& json);

    Json::Value toJson() const;

    std::size_t nullDepthBegin() const;
    std::size_t nullDepthEnd() const;
    std::size_t baseDepthBegin() const;
    std::size_t baseDepthEnd() const;
    std::size_t coldDepthBegin() const;
    std::size_t coldDepthEnd() const;
    std::size_t sparseDepthBegin() const;

    const Id& nullIndexBegin() const;
    const Id& nullIndexEnd() const;
    const Id& baseIndexBegin() const;
    const Id& baseIndexEnd() const;
    const Id& coldIndexBegin() const;
    const Id& coldIndexEnd() const;
    const Id& sparseIndexBegin() const;

    std::size_t baseIndexSpan() const;

    bool isWithinNull(const Id& index) const;
    bool isWithinBase(const Id& index) const;
    bool isWithinCold(const Id& index) const;

    bool hasNull() const;
    bool hasBase() const;
    bool hasCold() const;
    bool hasSparse() const;

    bool inRange(const Id& index) const;
    bool lossless() const;
    bool dynamicChunks() const;

    ChunkInfo getInfo(const Id& index) const;
    ChunkInfo getInfoFromNum(std::size_t chunkNum) const;
    std::size_t numChunksAtDepth(std::size_t depth) const;

    std::size_t baseChunkPoints() const { return m_chunkPoints; }
    std::size_t dimensions() const { return m_dimensions; }
    std::size_t factor() const { return m_factor; } // Quadtree: 4, octree: 8.
    bool is3d() const;

    std::size_t nominalChunkIndex() const { return m_nominalChunkIndex; }
    std::size_t nominalChunkDepth() const { return m_nominalChunkDepth; }

    std::size_t numPointsHint() const;

    bool isSubset() const;
    std::pair<std::size_t, std::size_t> subset() const;
    void makeWhole();

    std::unique_ptr<BBox> subsetBBox(const BBox& full) const;
    std::string subsetPostfix() const;

private:
    void loadIndexValues();

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

    Id m_nullIndexBegin;
    Id m_nullIndexEnd;
    Id m_baseIndexBegin;
    Id m_baseIndexEnd;
    Id m_coldIndexBegin;
    Id m_coldIndexEnd;
    Id m_sparseIndexBegin;

    std::size_t m_chunkPoints;

    // Chunk ID that spans the full bounds.  May not be an actual chunk since
    // it may reside in the base branch.
    std::size_t m_nominalChunkIndex;
    std::size_t m_nominalChunkDepth;

    bool m_dynamicChunks;

    std::size_t m_dimensions;
    std::size_t m_factor;
    std::size_t m_numPointsHint;

    std::pair<std::size_t, std::size_t> m_subset;
};

} // namespace entwine


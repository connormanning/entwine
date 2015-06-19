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

#include <entwine/third/json/json.h>

namespace entwine
{

class BBox;
class Structure;

class ChunkInfo
{
public:
    ChunkInfo(const Structure& structure, std::size_t index);

    static std::size_t calcDepth(std::size_t factor, std::size_t index);

    static std::size_t calcLevelIndex(
            std::size_t dimensions,
            std::size_t depth);

    static std::size_t pointsAtDepth(
            std::size_t dimensions,
            std::size_t depth);

    std::size_t depth()         const { return m_depth; }
    std::size_t chunkId()       const { return m_chunkId; }
    std::size_t chunkOffset()   const { return m_chunkOffset; }
    std::size_t chunkPoints()   const { return m_chunkPoints; }
    std::size_t chunkNum()      const { return m_chunkNum; }

private:
    const Structure& m_structure;

    std::size_t m_index;
    std::size_t m_depth;
    std::size_t m_chunkId;
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

    // TODO Lossless ctor.
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

    std::size_t nullIndexBegin() const;
    std::size_t nullIndexEnd() const;
    std::size_t baseIndexBegin() const;
    std::size_t baseIndexEnd() const;
    std::size_t coldIndexBegin() const;
    std::size_t coldIndexEnd() const;
    std::size_t sparseIndexBegin() const;

    std::size_t baseIndexSpan() const;

    bool isWithinNull(std::size_t index) const;
    bool isWithinBase(std::size_t index) const;
    bool isWithinCold(std::size_t index) const;

    bool hasNull() const;
    bool hasBase() const;
    bool hasCold() const;
    bool hasSparse() const;

    bool inRange(std::size_t index) const;
    bool lossless() const;  // TODO Not yet supported.
    bool dynamicChunks() const;

    ChunkInfo getInfo(std::size_t index) const;
    ChunkInfo getInfoFromNum(std::size_t chunkNum) const;
    std::size_t numChunksAtDepth(std::size_t depth) const;

    std::size_t baseChunkPoints() const;
    std::size_t dimensions() const;
    std::size_t factor() const;             // 4 if quadtree, 8 if octree.
    bool is3d() const;

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

    std::size_t m_nullIndexBegin;
    std::size_t m_nullIndexEnd;
    std::size_t m_baseIndexBegin;
    std::size_t m_baseIndexEnd;
    std::size_t m_coldIndexBegin;
    std::size_t m_coldIndexEnd;
    std::size_t m_sparseIndexBegin;

    std::size_t m_chunkPoints;

    bool m_dynamicChunks;

    std::size_t m_dimensions;
    std::size_t m_factor;
    std::size_t m_numPointsHint;

    std::pair<std::size_t, std::size_t> m_subset;
};

} // namespace entwine


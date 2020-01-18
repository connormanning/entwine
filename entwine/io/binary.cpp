/******************************************************************************
* Copyright (c) 2020, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <entwine/io/binary.hpp>

#include <algorithm>

#include <pdal/PointRef.hpp>

#include <entwine/types/dimension.hpp>
#include <entwine/types/metadata.hpp>
#include <entwine/types/scale-offset.hpp>
#include <entwine/util/io.hpp>

namespace entwine
{
namespace io
{
namespace binary
{

void write(
    const Metadata& metadata,
    const Endpoints& endpoints,
    const std::string filename,
    BlockPointTable& table,
    const Bounds bounds)
{
    const auto packed = pack(metadata, table);
    ensurePut(endpoints.data, filename + ".bin", packed);
}

void read(
    const Metadata& metadata,
    const Endpoints& endpoints,
    const std::string filename,
    VectorPointTable& table)
{
    auto packed = ensureGetBinary(endpoints.data, filename + ".bin");
    unpack(metadata, table, std::move(packed));
}

std::vector<char> pack(const Metadata& m, BlockPointTable& src)
{
    const uint64_t np(src.size());

    auto layout = toLayout(m.schema);
    VectorPointTable dst(layout, np);

    // Handle XYZ separately since we might need to scale/offset them.
    const Schema others = omit(m.schema, { "X", "Y", "Z" });
    struct DimReg { DimId id; DimType type; uint64_t offset; };
    const auto registrations = std::accumulate(
        others.begin(),
        others.end(),
        std::vector<DimReg>(),
        [&layout](std::vector<DimReg> v, const Dimension& d)
        {
            const DimId id = layout.findDim(d.name);
            v.push_back({ id, d.type, layout.dimOffset(id) });
            return v;
        });

    pdal::PointRef srcPr(src, 0);
    pdal::PointRef dstPr(dst, 0);

    Point p;

    const auto so = getScaleOffset(m.schema);

    for (uint64_t i(0); i < np; ++i)
    {
        srcPr.setPointId(i);
        dstPr.setPointId(i);
        char* pos(dst.getPoint(i));

        // Handle XYZ, applying transformation if needed.
        p.x = srcPr.getFieldAs<double>(DimId::X);
        p.y = srcPr.getFieldAs<double>(DimId::Y);
        p.z = srcPr.getFieldAs<double>(DimId::Z);

        if (so) p = Point::scale(p, so->scale, so->offset).round();

        dstPr.setField(DimId::X, p.x);
        dstPr.setField(DimId::Y, p.y);
        dstPr.setField(DimId::Z, p.z);

        // Handle the rest of the dimensions.
        for (const auto& dim : registrations)
        {
            srcPr.getField(pos + dim.offset, dim.id, dim.type);
        }
    }

    return dst.data();
}

void unpack(
    const Metadata& m,
    VectorPointTable& dst,
    std::vector<char>&& packed)
{
    auto scaledLayout = toLayout(m.schema);
    VectorPointTable src(scaledLayout, std::move(packed));

    const uint64_t np(src.capacity());
    assert(np == dst.capacity());

    // For reading, our destination schema will always be normalized (i.e. XYZ
    // as doubles).  So we can just copy the full dimension list and then
    // transform XYZ in place, if necessary.
    auto absoluteLayout = toLayout(m.absoluteSchema);
    pdal::DimTypeList dimTypes(absoluteLayout.dimTypes());

    pdal::PointRef srcPr(src, 0);
    pdal::PointRef dstPr(dst, 0);

    Point p;

    const auto so = getScaleOffset(m.schema);

    for (uint64_t i(0); i < np; ++i)
    {
        srcPr.setPointId(i);
        dstPr.setPointId(i);
        char* pos(dst.getPoint(i));

        for (const pdal::DimType& dim : dimTypes)
        {
            srcPr.getField(
                    pos + absoluteLayout.dimOffset(dim.m_id),
                    dim.m_id,
                    dim.m_type);
        }

        if (so)
        {
            p.x = dstPr.getFieldAs<double>(DimId::X);
            p.y = dstPr.getFieldAs<double>(DimId::Y);
            p.z = dstPr.getFieldAs<double>(DimId::Z);

            p = Point::unscale(p, so->scale, so->offset);

            dstPr.setField(DimId::X, p.x);
            dstPr.setField(DimId::Y, p.y);
            dstPr.setField(DimId::Z, p.z);
        }
    }

    dst.clear(np);
}

} // namespace binary
} // namespace io
} // namespace entwine

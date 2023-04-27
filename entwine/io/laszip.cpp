/******************************************************************************
* Copyright (c) 2020, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <entwine/io/laszip.hpp>

#include <pdal/filters/SortFilter.hpp>
#include <pdal/io/BufferReader.hpp>
#include <pdal/io/LasReader.hpp>
#include <pdal/io/LasWriter.hpp>
#include <pdal/pdal_config.hpp>

#include <entwine/types/metadata.hpp>
#include <entwine/util/io.hpp>
#include <entwine/util/pdal-mutex.hpp>

namespace entwine
{
namespace io
{
namespace laszip
{

void write(
    const Metadata& metadata,
    const Endpoints& endpoints,
    const std::string filename,
    BlockPointTable& table,
    const Bounds bounds)
{
    const arbiter::Endpoint& out(endpoints.data);
    const arbiter::Endpoint& tmp(endpoints.tmp);

    const bool local(out.isLocal());
    const std::string localDir(local ? out.prefixedRoot() : tmp.prefixedRoot());
    const std::string localFile(
            (local ? filename : arbiter::crypto::encodeAsHex(filename)) +
            ".laz");

    pdal::BufferReader reader;
    auto view(std::make_shared<pdal::PointView>(table));
    for (std::size_t i(0); i < table.size(); ++i) view->getOrAddPoint(i);
    reader.addView(view);

    // See https://www.pdal.io/stages/writers.las.html
    const uint64_t timeMask(contains(metadata.schema, "GpsTime") ? 1 : 0);
    const uint64_t colorMask(contains(metadata.schema, "Red") ? 2 : 0);

    pdal::Options options;
    options.add("filename", localDir + localFile);

    if (metadata.internal.laz_14)
        options.add("minor_version", 4);
    else
        options.add("minor_version", 2);


    options.add("extra_dims", "all");
    options.add("software_id", "Entwine " + currentEntwineVersion().toString());

    options.add("dataformat_id", timeMask | colorMask);

    const auto so = getScaleOffset(metadata.schema);
    if (!so) throw std::runtime_error("Scale/offset is required for laszip");
    options.add("scale_x", so->scale.x);
    options.add("scale_y", so->scale.y);
    options.add("scale_z", so->scale.z);

    options.add("offset_x", so->offset.x);
    options.add("offset_y", so->offset.y);
    options.add("offset_z", so->offset.z);

    if (metadata.srs) options.add("a_srs", metadata.srs->wkt());

    std::unique_lock<std::mutex> lock(PdalMutex::get());

    pdal::Stage* prev(&reader);

    std::unique_ptr<pdal::SortFilter> sort;
    if (contains(metadata.schema, "GpsTime"))
    {
        sort = makeUnique<pdal::SortFilter>();

        pdal::Options so;
        so.add("dimension", "GpsTime");
        sort->setOptions(so);
        sort->setInput(*prev);

        prev = sort.get();
    }

    pdal::LasWriter writer;
    writer.setOptions(options);
    writer.setInput(*prev);
    writer.prepare(table);

    lock.unlock();

    writer.execute(table);

    if (!local)
    {
        ensurePut(out, filename + ".laz", tmp.getBinary(localFile));
        arbiter::remove(tmp.prefixedRoot() + localFile);
    }
}

void read(
    const Metadata& metadata,
    const Endpoints& endpoints,
    const std::string filename,
    VectorPointTable& table)
{
    auto handle(endpoints.data.getLocalHandle(filename + ".laz"));

    pdal::Options o;
    o.add("filename", handle.localPath());
    o.add("use_eb_vlr", true);

    pdal::LasReader reader;
    reader.setOptions(o);

    {
        std::lock_guard<std::mutex> lock(PdalMutex::get());
        reader.prepare(table);
    }

    reader.execute(table);
}

} // namespace laszip
} // namespace io
} // namespace entwine

/******************************************************************************
* Copyright (c) 2018, Connor Manning (connor@hobu.co)
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

#include <entwine/util/executor.hpp>

namespace entwine
{

void Laz::write(
        const arbiter::Endpoint& out,
        const arbiter::Endpoint& tmp,
        const std::string& filename,
        const Bounds& bounds,
        BlockPointTable& table) const
{
    const bool local(out.isLocal());
    const std::string localDir(
            local ? out.prefixedRoot() : tmp.prefixedRoot());
    const std::string localFile(
            (local ? filename : arbiter::crypto::encodeAsHex(filename)) +
            ".laz");

    const Schema& outSchema(m_metadata.outSchema());

    pdal::BufferReader reader;
    auto view(std::make_shared<pdal::PointView>(table));
    for (std::size_t i(0); i < table.size(); ++i) view->getOrAddPoint(i);
    reader.addView(view);

    // See https://www.pdal.io/stages/writers.las.html
    const uint64_t timeMask(outSchema.hasTime() ? 1 : 0);
    const uint64_t colorMask(outSchema.hasColor() ? 2 : 0);

    pdal::Options options;
    options.add("filename", localDir + localFile);
    options.add("minor_version", 2);
    options.add("extra_dims", "all");
    options.add("software_id", "Entwine " + currentEntwineVersion().toString());
    options.add("compression", "laszip");
    options.add("dataformat_id", timeMask | colorMask);

    options.add("scale_x", outSchema.scale().x);
    options.add("scale_y", outSchema.scale().y);
    options.add("scale_z", outSchema.scale().z);

    options.add("offset_x", outSchema.offset().x);
    options.add("offset_y", outSchema.offset().y);
    options.add("offset_z", outSchema.offset().z);

    if (m_metadata.srs().exists()) options.add("a_srs", m_metadata.srs().wkt());

    auto lock(Executor::getLock());

    pdal::Stage* prev(&reader);

    std::unique_ptr<pdal::SortFilter> sort;
    if (outSchema.contains("GpsTime"))
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
void Laz::read(
        const arbiter::Endpoint& out,
        const arbiter::Endpoint& tmp,
        const std::string& filename,
        VectorPointTable& table) const
{
    auto handle(out.getLocalHandle(filename + ".laz"));

    pdal::Options o;
    o.add("filename", handle->localPath());
    o.add("use_eb_vlr", true);

    pdal::LasReader reader;
    reader.setOptions(o);

    {
        auto lock(Executor::getLock());
        reader.prepare(table);
    }

    reader.execute(table);
}

} // namespace entwine


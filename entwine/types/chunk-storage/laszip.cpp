/******************************************************************************
* Copyright (c) 2017, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <entwine/types/chunk-storage/laszip.hpp>

#include <pdal/io/LasWriter.hpp>

#include <entwine/types/pooled-point-table.hpp>
#include <entwine/types/reprojection.hpp>
#include <entwine/util/executor.hpp>

namespace entwine
{

void LasZipStorage::write(Chunk& chunk) const
{
    Cell::PooledStack cellStack(chunk.acquire());
    const Schema& schema(chunk.schema());

    if (!m_metadata.delta())
    {
        throw std::runtime_error("Laszip storage requires scaling.");
    }

    const Delta& delta(*m_metadata.delta());

    CellTable cellTable(
            chunk.pool(),
            std::move(cellStack),
            makeUnique<Schema>(Schema::normalize(schema)));

    StreamReader reader(cellTable);

    const auto& outEndpoint(chunk.builder().outEndpoint());
    const auto& tmpEndpoint(chunk.builder().tmpEndpoint());

    const std::string localDir = outEndpoint.isLocal() ?
        outEndpoint.prefixedRoot() : tmpEndpoint.prefixedRoot();

    const std::string filename(m_metadata.basename(chunk.id()) + ".laz");

    const auto offset = Point::unscale(
            chunk.bounds().mid(),
            delta.scale(),
            delta.offset())
        .apply([](double d) { return std::floor(d); });

    // See https://www.pdal.io/stages/writers.las.html
    uint64_t timeMask(schema.hasTime() ? 1 : 0);
    uint64_t colorMask(schema.hasColor() ? 2 : 0);

    pdal::Options options;
    options.add("filename", localDir + filename);
    options.add("minor_version", 4);
    options.add("extra_dims", "all");
    options.add("software_id", "Entwine " + currentVersion().toString());
    options.add("compression", "laszip");
    options.add("dataformat_id", timeMask | colorMask);

    options.add("scale_x", delta.scale().x);
    options.add("scale_y", delta.scale().y);
    options.add("scale_z", delta.scale().z);

    options.add("offset_x", offset.x);
    options.add("offset_y", offset.y);
    options.add("offset_z", offset.z);

    if (auto r = m_metadata.reprojection()) options.add("a_srs", r->out());
    else if (m_metadata.srs().size()) options.add("a_srs", m_metadata.srs());

    auto lock(Executor::getLock());

    pdal::LasWriter writer;
    writer.setOptions(options);
    writer.setInput(reader);
    writer.prepare(cellTable);

    lock.unlock();

    writer.execute(cellTable);

    if (!outEndpoint.isLocal())
    {
        io::ensurePut(outEndpoint, filename, tmpEndpoint.getBinary(filename));

        arbiter::fs::remove(tmpEndpoint.prefixedRoot() + filename);
    }
}

Cell::PooledStack LasZipStorage::read(
        const arbiter::Endpoint& endpoint,
        PointPool& pool,
        const Id& id) const
{
    const std::string basename(m_metadata.basename(id) + ".laz");
    std::string localFile(endpoint.prefixedRoot() + basename);

    if (!endpoint.isLocal())
    {
        const std::string tmp(arbiter::fs::getTempPath());
        localFile = tmp + basename;

        static const arbiter::drivers::Fs fs;
        fs.put(localFile, *io::ensureGet(endpoint, basename));
    }

    CellTable table(pool, makeUnique<Schema>(Schema::normalize(pool.schema())));

    if (auto preview = Executor::get().preview(localFile))
    {
        table.resize(preview->numPoints);
    }

    if (!Executor::get().run(table, localFile))
    {
        throw std::runtime_error("Could not execute laszip chunk " + localFile);
    }

    return table.acquire();
}

} // namespace entwine


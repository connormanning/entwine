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

void LasZipStorage::write(
        const arbiter::Endpoint& out,
        const arbiter::Endpoint& tmp,
        PointPool& pointPool,
        const std::string& filename,
        const Bounds& bounds,
        Cell::PooledStack&& cells) const
{
    if (!m_metadata.delta())
    {
        throw std::runtime_error("Laszip storage requires scaling.");
    }

    const bool local(out.isLocal());
    const std::string localDir(
            local ? out.prefixedRoot() : tmp.prefixedRoot());
    const std::string localFile(
            (local ? filename : arbiter::crypto::encodeAsHex(filename)) +
            ".laz");

    /*
    std::cout << "mkdirp " << localDir << std::endl;
    if (local && !arbiter::fs::mkdirp(localDir + localFile))
    {
        throw std::runtime_error("Couldn't create " + localDir + localFile);
    }
    */

    const Schema& schema(m_metadata.schema());
    const Delta& delta(*m_metadata.delta());

    CellTable table(
            pointPool,
            std::move(cells),
            makeUnique<Schema>(Schema::normalize(schema)));

    StreamReader reader(table);

    const auto offset = Point::unscale(
            bounds.mid(),
            delta.scale(),
            delta.offset())
        .apply([](double d) { return std::floor(d); });

    // See https://www.pdal.io/stages/writers.las.html
    uint64_t timeMask(schema.hasTime() ? 1 : 0);
    uint64_t colorMask(schema.hasColor() ? 2 : 0);

    pdal::Options options;
    options.add("filename", localDir + localFile);
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
    writer.prepare(table);

    lock.unlock();

    writer.execute(table);

    if (!local)
    {
        io::ensurePut(out, filename, tmp.getBinary(localFile));
        arbiter::fs::remove(tmp.prefixedRoot() + localFile);
    }
}

Cell::PooledStack LasZipStorage::read(
        const arbiter::Endpoint& out,
        const arbiter::Endpoint& tmp,
        PointPool& pool,
        const std::string& filename) const
{
    const std::string basename(filename + ".laz");
    std::string localFile(out.prefixedRoot() + basename);
    bool copied(false);

    if (!out.isLocal() && out.tryGetSize(basename))
    {
        localFile = arbiter::util::join(
                tmp.prefixedRoot(),
                arbiter::crypto::encodeAsHex(
                    out.prefixedRoot()) + "-" + basename);
        copied = true;

        static const arbiter::drivers::Fs fs;
        fs.put(localFile, *io::ensureGet(out, basename));
    }

    CellTable table(pool, makeUnique<Schema>(Schema::normalize(pool.schema())));

    if (auto preview = Executor::get().preview(localFile))
    {
        table.resize(preview->numPoints);
    }

    const bool good(Executor::get().run(table, localFile));
    if (copied) arbiter::fs::remove(localFile);
    if (!good) throw std::runtime_error("Laszip read failure: " + localFile);

    return table.acquire();
}

} // namespace entwine


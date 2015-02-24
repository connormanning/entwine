/******************************************************************************
* Copyright (c) 2016, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <cmath>
#include <iomanip>

#include <pdal/BufferReader.hpp>
#include <pdal/Charbuf.hpp>
#include <pdal/Compression.hpp>
#include <pdal/Dimension.hpp>
#include <pdal/PointContext.hpp>
#include <pdal/ReprojectionFilter.hpp>

#include "compression/compression-stream.hpp"
#include "types/bbox.hpp"

int main(int argc, char** argv)
{
    pdal::PointContext pointContext;
    pointContext.registerDim(pdal::Dimension::Id::X);
    pointContext.registerDim(pdal::Dimension::Id::Y);
    pointContext.registerDim(pdal::Dimension::Id::Z);
    pointContext.registerOrAssignDim(
            "OriginId",
            pdal::Dimension::Type::Unsigned64);

    std::string path = "./out/0";

    std::ifstream dataStream;
    dataStream.open(path, std::ifstream::in | std::ifstream::binary);
    if (!dataStream.good())
    {
        throw std::runtime_error("Could not open " + path);
    }

    double xMin(0), yMin(0), xMax(0), yMax(0);
    dataStream.read(reinterpret_cast<char*>(&xMin), sizeof(double));
    dataStream.read(reinterpret_cast<char*>(&yMin), sizeof(double));
    dataStream.read(reinterpret_cast<char*>(&xMax), sizeof(double));
    dataStream.read(reinterpret_cast<char*>(&yMax), sizeof(double));
    BBox bbox(Point(xMin, yMin), Point(xMax, yMax));

    uint64_t uncSize(0), cmpSize(0);
    dataStream.read(reinterpret_cast<char*>(&uncSize), sizeof(uint64_t));
    dataStream.read(reinterpret_cast<char*>(&cmpSize), sizeof(uint64_t));

    std::vector<char> compressed(cmpSize);
    dataStream.read(compressed.data(), compressed.size());

    CompressionStream decompressionStream(compressed);
    pdal::LazPerfDecompressor<CompressionStream> decompressor(
            decompressionStream,
            pointContext.dimTypes());

    std::shared_ptr<std::vector<char>> uncompressed(
            new std::vector<char>(uncSize));

    decompressor.decompress(uncompressed->data(), uncSize);

    pdal::Charbuf charbuf(*uncompressed.get());
    std::istream stream(&charbuf);
    pdal::PointBufferPtr pointBuffer(
            new pdal::PointBuffer(
                stream,
                pointContext,
                0,
                uncompressed->size() / pointContext.pointSize()));
    std::unique_ptr<pdal::BufferReader> bufferReader(new pdal::BufferReader());

    // Set bounding box at the end for reproj.
    std::size_t s(pointBuffer->size());
    pointBuffer->setField<double>(pdal::Dimension::Id::X, s, 192325.727246);
    pointBuffer->setField<double>(pdal::Dimension::Id::Y, s, 4455899.672669);
    ++s;
    pointBuffer->setField<double>(pdal::Dimension::Id::X, s, 770274.931198);
    pointBuffer->setField<double>(pdal::Dimension::Id::Y, s, 4833815.152277);

    bufferReader->addBuffer(pointBuffer);
    bufferReader->setSpatialReference(pdal::SpatialReference("EPSG:26915"));

    // Reproject to Web Mercator.
    pdal::Options srsOptions;
    srsOptions.add(
            pdal::Option(
                "in_srs",
                pdal::SpatialReference("EPSG:26915")));
    srsOptions.add(
            pdal::Option(
                "out_srs",
                pdal::SpatialReference("EPSG:3857")));

    std::unique_ptr<pdal::ReprojectionFilter> filter(
            new pdal::ReprojectionFilter());
    filter->setInput(bufferReader.get());
    filter->setOptions(srsOptions);

    filter->prepare(pointContext);
    pdal::PointBufferPtr out(
            *filter->execute(pointContext).begin());

    double x(0), y(0);
    xMin = 999999999;
    yMin = 999999999;
    xMax = -999999999;
    yMax = -999999999;

    for (std::size_t i(0); i < out->size() - 2; ++i)
    {
        // Zero out fields in the output that were zero at the input, since
        // they were probably reprojected to junk.
        if (std::abs(
                    pointBuffer->getFieldAs<double>(
                        pdal::Dimension::Id::X, i)) < 1)
        {
            out->setField<double>(pdal::Dimension::Id::X, i, 0);
            out->setField<double>(pdal::Dimension::Id::Y, i, 0);
        }

        x = out->getFieldAs<double>(pdal::Dimension::Id::X, i);
        y = out->getFieldAs<double>(pdal::Dimension::Id::Y, i);

        if (std::abs(x) > 1 && std::abs(y) > 1)
        {
            xMin = std::min(xMin, x);
            yMin = std::min(yMin, y);
            xMax = std::max(xMax, x);
            yMax = std::max(yMax, y);
        }
    }

    // Log data bounds.
    std::cout << "DATA: " << std::setprecision(16) << "(" <<
        xMin << "," << yMin << ") (" <<
        xMax << "," << yMax << ")" << std::endl;

    // Set to reprojected initial bounding box.
    xMin = out->getFieldAs<double>(pdal::Dimension::Id::X, out->size() - 2);
    yMin = out->getFieldAs<double>(pdal::Dimension::Id::Y, out->size() - 2);
    xMax = out->getFieldAs<double>(pdal::Dimension::Id::X, out->size() - 1);
    yMax = out->getFieldAs<double>(pdal::Dimension::Id::Y, out->size() - 1);

    // Log input bounds.
    std::cout << "INIT: " << std::setprecision(16) << "(" <<
        xMin << "," << yMin << ") (" <<
        xMax << "," << yMax << ")" << std::endl;

    std::string outPath = "./out/re";
    std::ofstream outStream;
    outStream.open(
            outPath,
            std::ofstream::out | std::ofstream::trunc | std::ofstream::binary);

    // TODO Need to use actual reprojected input bounds.
    outStream.write(reinterpret_cast<const char*>(&xMin), sizeof(double));
    outStream.write(reinterpret_cast<const char*>(&yMin), sizeof(double));
    outStream.write(reinterpret_cast<const char*>(&xMax), sizeof(double));
    outStream.write(reinterpret_cast<const char*>(&yMax), sizeof(double));

    const uint64_t uncompressedSize(out->size() * out->pointSize());

    CompressionStream compressionStream;
    pdal::LazPerfCompressor<CompressionStream> compressor(
            compressionStream,
            out->dimTypes());

    std::vector<char> bytes(out->pointSize());
    for (std::size_t i(0); i < out->size() - 2; ++i) // Don't include bbox.
    {
        out->getPackedPoint(out->dimTypes(), i, bytes.data());
        compressor.compress(bytes.data(), bytes.size());
    }

    compressor.done();

    std::shared_ptr<std::vector<char>> compressedOut(
            new std::vector<char>(compressionStream.data().size()));

    std::memcpy(
            compressedOut->data(),
            compressionStream.data().data(),
            compressedOut->size());
    const uint64_t compressedSize(compressedOut->size());

    outStream.write(
            reinterpret_cast<const char*>(&uncompressedSize),
            sizeof(uint64_t));
    outStream.write(
            reinterpret_cast<const char*>(&compressedSize),
            sizeof(uint64_t));
    outStream.write(compressedOut->data(), compressedOut->size());
    outStream.close();
}


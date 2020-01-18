/******************************************************************************
* Copyright (c) 2019, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <entwine/util/io.hpp>

#include <pdal/util/IStream.hpp>
#include <pdal/util/OStream.hpp>

#include <chrono>
#include <mutex>
#include <thread>

namespace entwine
{

namespace
{

std::mutex mutex;

void sleep(const int tried, const std::string message)
{
    // Linear back-off should be fine.
    std::this_thread::sleep_for(std::chrono::seconds(tried));

    if (message.size())
    {
        std::lock_guard<std::mutex> lock(mutex);
        std::cout << "Failure #" << tried << ": " << message << std::endl;
    }
}

template <typename F>
bool loop(F f, const int tries, std::string message = "")
{
    int tried = 0;

    do
    {
        try
        {
            f();
            return true;
        }
        catch (...) { }

        sleep(tried + 1, message);
    }
    while (++tried < tries);

    return false;
}

arbiter::http::Headers getRangeHeader(int start, int end = 0)
{
    arbiter::http::Headers h;
    h["Range"] = "bytes=" + std::to_string(start) + "-" +
        (end ? std::to_string(end - 1) : "");
    return h;
}

} // unnamed namespace

bool putWithRetry(
    const arbiter::Endpoint& ep,
    const std::string& path,
    const std::vector<char>& data,
    const int tries)
{
    const auto f = [&ep, &path, &data]() { ep.put(path, data); };
    return loop(f, tries, "Failed to put " + path);
}

bool putWithRetry(
    const arbiter::Endpoint& ep,
    const std::string& path,
    const std::string& s,
    const int tries)
{
    return putWithRetry(ep, path, std::vector<char>(s.begin(), s.end()), tries);
}

void ensurePut(
    const arbiter::Endpoint& ep,
    const std::string& path,
    const std::vector<char>& data,
    const int tries)
{
    if (!putWithRetry(ep, path, data, tries))
    {
        throw FatalError("Failed to put to " + path);
    }
}

void ensurePut(
    const arbiter::Endpoint& ep,
    const std::string& path,
    const std::string& s,
    const int tries)
{
    ensurePut(ep, path, std::vector<char>(s.begin(), s.end()), tries);
}

optional<std::vector<char>> getBinaryWithRetry(
    const arbiter::Endpoint& ep,
    const std::string& path,
    const int tries)
{
    std::vector<char> data;
    const auto f = [&ep, &path, &data]() { data = ep.getBinary(path); };
    const std::string message =
        "Failed to get " +
        arbiter::join(ep.prefixedRoot(), path);

    if (loop(f, tries, message)) return data;
    else return { };
}

optional<std::string> getWithRetry(
    const arbiter::Endpoint& ep,
    const std::string& path,
    const int tries)
{
    const auto v(getBinaryWithRetry(ep, path, tries));

    if (v) return std::string(v->begin(), v->end());
    else return { };
}

optional<std::string> getWithRetry(
    const arbiter::Arbiter& a,
    const std::string& path,
    const int tries)
{
    std::string data;
    const auto f = [&a, &path, &data]() { data = a.get("path"); };
    const std::string message = "Failed to get " + path;

    if (loop(f, tries, message)) return data;
    return { };
}

std::vector<char> ensureGetBinary(
    const arbiter::Endpoint& ep,
    const std::string& path,
    const int tries)
{
    const auto v(getBinaryWithRetry(ep, path, tries));
    if (v) return *v;
    else throw FatalError("Failed to get " + path);
}

std::string ensureGet(
    const arbiter::Endpoint& ep,
    const std::string& path,
    const int tries)
{
    const auto v(getWithRetry(ep, path, tries));
    if (v) return *v;
    else throw FatalError("Failed to get " + path);
}

std::string ensureGet(
    const arbiter::Arbiter& a,
    const std::string& path,
    const int tries)
{
    const auto v(getWithRetry(a, path, tries));
    if (v) return *v;
    else throw FatalError("Failed to get " + path);
}

arbiter::LocalHandle ensureGetLocalHandle(
    const arbiter::Arbiter& a,
    const std::string& path,
    const int tries)
{
    for (int tried = 0; tried < tries; ++tried)
    {
        try { return a.getLocalHandle(path); }
        catch(...) { }
    }

    throw std::runtime_error("Failed to get " + path);
}

arbiter::LocalHandle getPointlessLasFile(
    const std::string& path,
    const std::string& tmp,
    const arbiter::Arbiter& a)
{
    const uint64_t maxHeaderSize(375);

    const uint64_t minorVersionPos(25);
    const uint64_t headerSizePos(94);
    const uint64_t pointOffsetPos(96);
    const uint64_t evlrOffsetPos(235);
    const uint64_t evlrNumberPos(evlrOffsetPos + 8);

    std::string fileSignature;
    uint8_t minorVersion(0);
    uint16_t headerSize(0);
    uint32_t pointOffset(0);
    uint64_t evlrOffset(0);
    uint32_t evlrNumber(0);

    std::string header(a.get(path, getRangeHeader(0, maxHeaderSize)));

    std::stringstream headerStream(
            header,
            std::ios_base::in | std::ios_base::out | std::ios_base::binary);

    pdal::ILeStream is(&headerStream);
    pdal::OLeStream os(&headerStream);

    is.seek(0);
    is.get(fileSignature, 4);

    if (fileSignature != "LASF")
    {
        throw std::runtime_error(
            "Invalid file signature for .las or .laz file: must be LASF");
    }

    is.seek(minorVersionPos);
    is >> minorVersion;

    is.seek(headerSizePos);
    is >> headerSize;

    is.seek(pointOffsetPos);
    is >> pointOffset;

    if (minorVersion >= 4)
    {
        is.seek(evlrOffsetPos);
        is >> evlrOffset;

        is.seek(evlrNumberPos);
        is >> evlrNumber;

        // Modify the header such that the EVLRs come directly after the VLRs -
        // removing the point data itself.
        os.seek(evlrOffsetPos);
        os << pointOffset;
    }

    // Extract the modified header, VLRs, and append the EVLRs.
    header = headerStream.str();
    std::vector<char> data(header.data(), header.data() + headerSize);

    const bool hasVlrs = headerSize < pointOffset;
    if (hasVlrs)
    {
        const auto vlrs = a.getBinary(
            path,
            getRangeHeader(headerSize, pointOffset));
        data.insert(data.end(), vlrs.begin(), vlrs.end());
    }

    const bool hasEvlrs = evlrNumber && evlrOffset;
    if (hasEvlrs)
    {
        const auto evlrs = a.getBinary(path, getRangeHeader(evlrOffset));
        data.insert(data.end(), evlrs.begin(), evlrs.end());
    }

    const std::string extension(arbiter::getExtension(path));
    const std::string basename(
        std::to_string(arbiter::randomNumber()) +
        (extension.size() ? "." + extension : ""));

    const std::string outputPath = arbiter::join(tmp, basename);
    a.put(outputPath, data);
    return arbiter::LocalHandle(outputPath, true);
}
} // namespace entwine

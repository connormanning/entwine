/// Arbiter amalgamated source (https://github.com/connormanning/arbiter).
/// It is intended to be used with #include "arbiter.hpp"

// //////////////////////////////////////////////////////////////////////
// Beginning of content of file: LICENSE
// //////////////////////////////////////////////////////////////////////

/*
The MIT License (MIT)

Copyright (c) 2015 Connor Manning

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.


*/

// //////////////////////////////////////////////////////////////////////
// End of content of file: LICENSE
// //////////////////////////////////////////////////////////////////////






#include "arbiter.hpp"

#ifndef ARBITER_IS_AMALGAMATION
#error "Compile with -I PATH_TO_ARBITER_DIRECTORY"
#endif


// //////////////////////////////////////////////////////////////////////
// Beginning of content of file: arbiter/arbiter.cpp
// //////////////////////////////////////////////////////////////////////

#ifndef ARBITER_IS_AMALGAMATION
#include <arbiter/arbiter.hpp>

#include <arbiter/driver.hpp>
#endif

#include <algorithm>

namespace arbiter
{

namespace
{
    const std::string delimiter("://");

    const std::size_t concurrentHttpReqs(32);
    const std::size_t httpRetryCount(8);
}

Arbiter::Arbiter(std::string awsUser)
    : m_drivers()
    , m_pool(concurrentHttpReqs, httpRetryCount)
{
    m_drivers["fs"] =   std::make_shared<FsDriver>(FsDriver());
    m_drivers["http"] = std::make_shared<HttpDriver>(HttpDriver(m_pool));

    std::unique_ptr<AwsAuth> auth(AwsAuth::find(awsUser));

    if (auth)
    {
        m_drivers["s3"] = std::make_shared<S3Driver>(S3Driver(m_pool, *auth));
    }
}

Arbiter::~Arbiter()
{ }

std::string Arbiter::get(const std::string path) const
{
    return getDriver(path).get(stripType(path));
}

std::vector<char> Arbiter::getBinary(const std::string path) const
{
    return getDriver(path).getBinary(stripType(path));
}

void Arbiter::put(const std::string path, const std::string& data) const
{
    return getDriver(path).put(stripType(path), data);
}

void Arbiter::put(const std::string path, const std::vector<char>& data) const
{
    return getDriver(path).put(stripType(path), data);
}

bool Arbiter::isRemote(const std::string path) const
{
    return getDriver(path).isRemote();
}

std::vector<std::string> Arbiter::resolve(
        const std::string path,
        const bool verbose) const
{
    return getDriver(path).resolve(stripType(path), verbose);
}

Endpoint Arbiter::getEndpoint(const std::string root) const
{
    return Endpoint(getDriver(root), stripType(root));
}

const Driver& Arbiter::getDriver(const std::string path) const
{
    return *m_drivers.at(parseType(path));
}

std::unique_ptr<fs::LocalHandle> Arbiter::getLocalHandle(
        const std::string path,
        const Endpoint& tempEndpoint) const
{
    std::unique_ptr<fs::LocalHandle> localHandle;

    if (isRemote(path))
    {
        std::string name(path);
        std::replace(name.begin(), name.end(), '/', '-');
        std::replace(name.begin(), name.end(), '\\', '-');

        tempEndpoint.putSubpath(name, getBinary(path));

        localHandle.reset(
                new fs::LocalHandle(tempEndpoint.root() + name, true));
    }
    else
    {
        localHandle.reset(new fs::LocalHandle(path, false));
    }

    return localHandle;
}

std::string Arbiter::parseType(const std::string path) const
{
    std::string type("fs");
    const std::size_t pos(path.find(delimiter));

    if (pos != std::string::npos)
    {
        type = path.substr(0, pos);
    }

    return type;
}

std::string Arbiter::stripType(const std::string raw)
{
    std::string result(raw);
    const std::size_t pos(raw.find(delimiter));

    if (pos != std::string::npos)
    {
        result = raw.substr(pos + delimiter.size());
    }

    return result;
}

} // namespace arbiter


// //////////////////////////////////////////////////////////////////////
// End of content of file: arbiter/arbiter.cpp
// //////////////////////////////////////////////////////////////////////






// //////////////////////////////////////////////////////////////////////
// Beginning of content of file: arbiter/driver.cpp
// //////////////////////////////////////////////////////////////////////

#ifndef ARBITER_IS_AMALGAMATION
#include <arbiter/driver.hpp>

#include <arbiter/arbiter.hpp>
#endif

namespace arbiter
{

std::unique_ptr<std::vector<char>> Driver::tryGetBinary(std::string path) const
{
    std::unique_ptr<std::vector<char>> data(new std::vector<char>());

    if (!get(path, *data))
    {
        data.reset();
    }

    return data;
}

std::vector<char> Driver::getBinary(std::string path) const
{
    std::vector<char> data;

    if (!get(path, data))
    {
        throw std::runtime_error("Could not read file " + path);
    }

    return data;
}

std::unique_ptr<std::string> Driver::tryGet(std::string path) const
{
    std::unique_ptr<std::string> result;
    std::unique_ptr<std::vector<char>> data(tryGetBinary(path));
    if (data) result.reset(new std::string(data->begin(), data->end()));
    return result;
}

std::string Driver::get(const std::string path) const
{
    const std::vector<char> data(getBinary(path));
    return std::string(data.begin(), data.end());
}

void Driver::put(std::string path, const std::string& data) const
{
    put(path, std::vector<char>(data.begin(), data.end()));
}

std::vector<std::string> Driver::resolve(
        std::string path,
        const bool verbose) const
{
    std::vector<std::string> results;

    if (path.size() > 1 && path.back() == '*')
    {
        if (verbose)
        {
            std::cout << "Resolving [" << type() << "]: " << path << " ..." <<
                std::flush;
        }

        results = glob(path, verbose);

        if (verbose)
        {
            std::cout << "\n\tResolved to " << results.size() <<
                " paths." << std::endl;
        }
    }
    else
    {
        if (type() != "fs") path = type() + "://" + path;

        results.push_back(path);
    }

    return results;
}

} // namespace arbiter


// //////////////////////////////////////////////////////////////////////
// End of content of file: arbiter/driver.cpp
// //////////////////////////////////////////////////////////////////////






// //////////////////////////////////////////////////////////////////////
// Beginning of content of file: arbiter/endpoint.cpp
// //////////////////////////////////////////////////////////////////////

#ifndef ARBITER_IS_AMALGAMATION
#include <arbiter/endpoint.hpp>

#include <arbiter/driver.hpp>
#endif

namespace arbiter
{

namespace
{
    std::string postfixSlash(std::string path)
    {
        if (path.empty()) throw std::runtime_error("Invalid root path");
        if (path.back() != '/') path.push_back('/');
        return path;
    }
}

Endpoint::Endpoint(const Driver& driver, const std::string root)
    : m_driver(driver)
    , m_root(postfixSlash(root))
{ }

std::string Endpoint::root() const
{
    return m_root;
}

std::string Endpoint::type() const
{
    return m_driver.type();
}

bool Endpoint::isRemote() const
{
    return m_driver.isRemote();
}

std::string Endpoint::getSubpath(const std::string subpath) const
{
    return m_driver.get(fullPath(subpath));
}

std::unique_ptr<std::string> Endpoint::tryGetSubpath(const std::string subpath)
    const
{
    return m_driver.tryGet(fullPath(subpath));
}

std::vector<char> Endpoint::getSubpathBinary(const std::string subpath) const
{
    return m_driver.getBinary(fullPath(subpath));
}

std::unique_ptr<std::vector<char>> Endpoint::tryGetSubpathBinary(
        const std::string subpath) const
{
    return m_driver.tryGetBinary(fullPath(subpath));
}

void Endpoint::putSubpath(
        const std::string subpath,
        const std::string& data) const
{
    m_driver.put(fullPath(subpath), data);
}

void Endpoint::putSubpath(
        const std::string subpath,
        const std::vector<char>& data) const
{
    m_driver.put(fullPath(subpath), data);
}

std::string Endpoint::fullPath(const std::string& subpath) const
{
    return m_root + subpath;
}

} // namespace arbiter


// //////////////////////////////////////////////////////////////////////
// End of content of file: arbiter/endpoint.cpp
// //////////////////////////////////////////////////////////////////////






// //////////////////////////////////////////////////////////////////////
// Beginning of content of file: arbiter/drivers/fs.cpp
// //////////////////////////////////////////////////////////////////////

#ifndef ARBITER_IS_AMALGAMATION
#include <arbiter/drivers/fs.hpp>
#endif

#ifndef ARBITER_WINDOWS
#include <glob.h>
#include <sys/stat.h>
#else
#include <locale>
#include <codecvt>
#endif

#include <cstdlib>
#include <fstream>
#include <stdexcept>

namespace arbiter
{

namespace
{
    // Binary output, overwriting any existing file with a conflicting name.
    const std::ios_base::openmode binaryTruncMode(
            std::ofstream::binary |
            std::ofstream::out |
            std::ofstream::trunc);

    void noHome()
    {
        throw std::runtime_error("No home directory found");
    }
}

bool FsDriver::get(std::string path, std::vector<char>& data) const
{
    bool good(false);

    path = fs::expandTilde(path);
    std::ifstream stream(path, std::ios::in | std::ios::binary);

    if (stream.good())
    {
        stream.seekg(0, std::ios::end);
        data.resize(static_cast<std::size_t>(stream.tellg()));
        stream.seekg(0, std::ios::beg);
        stream.read(data.data(), data.size());
        stream.close();
        good = true;
    }

    return good;
}

void FsDriver::put(std::string path, const std::vector<char>& data) const
{
    path = fs::expandTilde(path);
    std::ofstream stream(path, binaryTruncMode);

    if (!stream.good())
    {
        throw std::runtime_error("Could not open " + path + " for writing");
    }

    stream.write(data.data(), data.size());

    if (!stream.good())
    {
        throw std::runtime_error("Error occurred while writing " + path);
    }
}

std::vector<std::string> FsDriver::glob(std::string path, bool) const
{
    std::vector<std::string> results;

#ifndef ARBITER_WINDOWS
    path = fs::expandTilde(path);

    glob_t buffer;
    struct stat info;

    ::glob(path.c_str(), GLOB_NOSORT | GLOB_TILDE, 0, &buffer);

    for (std::size_t i(0); i < buffer.gl_pathc; ++i)
    {
        const std::string val(buffer.gl_pathv[i]);

        if (stat(val.c_str(), &info) == 0)
        {
            if (S_ISREG(info.st_mode))
            {
                results.push_back(val);
            }
        }
        else
        {
            throw std::runtime_error("Error globbing - POSIX stat failed");
        }
    }

    globfree(&buffer);
#else
    std::wstring_convert<std::codecvt_utf8_utf16<wchar_t>> converter;
    const std::wstring wide(converter.from_bytes(path));

    WIN32_FIND_DATA data;
    HANDLE hFind(FindFirstFile(wide.c_str(), &data));

    if (hFind != INVALID_HANDLE_VALUE)
    {
        do
        {
            if ((data.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY) == 0)
            {
                results.push_back(converter.to_bytes(data.cFileName));
            }
        }
        while (FindNextFile(hFind, &data));
    }
#endif

    return results;
}

namespace fs
{

bool mkdirp(std::string dir)
{
    dir = expandTilde(dir);

#ifndef ARBITER_WINDOWS
    const bool err(::mkdir(dir.c_str(), S_IRWXU | S_IRGRP | S_IROTH));
    return (!err || errno == EEXIST);
#else
    throw std::runtime_error("Windows mkdirp not done yet.");
#endif
}

bool remove(std::string filename)
{
    filename = expandTilde(filename);

#ifndef ARBITER_WINDOWS
    return ::remove(filename.c_str()) == 0;
#else
    throw std::runtime_error("Windows remove not done yet.");
#endif
}

std::string expandTilde(std::string in)
{
    std::string out(in);

    if (!in.empty() && in.front() == '~')
    {
#ifndef ARBITER_WINDOWS
        if (!getenv("HOME"))
        {
            noHome();
        }

        static const std::string home(getenv("HOME"));
#else
        if (
                !getenv("USERPROFILE") &&
                !(getenv("HOMEDRIVE") && getenv("HOMEPATH")))
        {
            noHome();
        }

        static const std::string home(
                getenv("USERPROFILE") ? getenv("USERPROFILE") :
                    (getenv("HOMEDRIVE") + getenv("HOMEPATH"));
#endif

        out = home + in.substr(1);
    }

    return out;
}

LocalHandle::LocalHandle(const std::string localPath, const bool isRemote)
    : m_localPath(expandTilde(localPath))
    , m_isRemote(isRemote)
{ }

LocalHandle::~LocalHandle()
{
    if (m_isRemote) fs::remove(fs::expandTilde(m_localPath));
}

} // namespace fs
} // namespace arbiter


// //////////////////////////////////////////////////////////////////////
// End of content of file: arbiter/drivers/fs.cpp
// //////////////////////////////////////////////////////////////////////






// //////////////////////////////////////////////////////////////////////
// Beginning of content of file: arbiter/drivers/http.cpp
// //////////////////////////////////////////////////////////////////////

#ifndef ARBITER_IS_AMALGAMATION
#include <arbiter/drivers/http.hpp>
#endif

#ifdef ARBITER_WINDOWS
#undef min
#undef max
#endif

#include <algorithm>
#include <cstring>
#include <iostream>

namespace
{
    struct PutData
    {
        PutData(const std::vector<char>& data)
            : data(data)
            , offset(0)
        { }

        const std::vector<char>& data;
        std::size_t offset;
    };

    std::size_t getCb(
            const char* in,
            std::size_t size,
            std::size_t num,
            std::vector<char>* out)
    {
        const std::size_t fullBytes(size * num);
        const std::size_t startSize(out->size());

        out->resize(out->size() + fullBytes);
        std::memcpy(out->data() + startSize, in, fullBytes);

        return fullBytes;
    }

    std::size_t putCb(
            char* out,
            std::size_t size,
            std::size_t num,
            PutData* in)
    {
        const std::size_t fullBytes(
                std::min(
                    size * num,
                    in->data.size() - in->offset));
        std::memcpy(out, in->data.data() + in->offset, fullBytes);

        in->offset += fullBytes;
        return fullBytes;
    }

    size_t eatLogging(void *out, size_t size, size_t num, void *in)
    {
        return size * num;
    }

    const bool followRedirect(true);
    const bool verbose(false);

    const auto baseSleepTime(std::chrono::milliseconds(1));
    const auto maxSleepTime (std::chrono::milliseconds(4096));

    const std::map<char, std::string> sanitizers
    {
        { ' ', "%20" },
        { '!', "%21" },
        { '"', "%22" },
        { '#', "%23" },
        { '$', "%24" },
        { '\'', "%27" },
        { '(', "%28" },
        { ')', "%29" },
        { '*', "%2A" },
        { '+', "%2B" },
        { ',', "%2C" },
        { ';', "%3B" },
        { '<', "%3C" },
        { '>', "%3E" },
        { '@', "%40" },
        { '[', "%5B" },
        { '\\', "%5C" },
        { ']', "%5D" },
        { '^', "%5E" },
        { '`', "%60" },
        { '{', "%7B" },
        { '|', "%7C" },
        { '}', "%7D" },
        { '~', "%7E" }
    };
}

namespace arbiter
{

HttpDriver::HttpDriver(HttpPool& pool)
    : m_pool(pool)
{ }

bool HttpDriver::get(std::string path, std::vector<char>& data) const
{
    bool good(false);

    auto http(m_pool.acquire());
    HttpResponse res(http.get(path));

    if (res.ok())
    {
        data = res.data();
        good = true;
    }

    return good;
}

void HttpDriver::put(
        std::string path,
        const std::vector<char>& data) const
{
    auto http(m_pool.acquire());

    if (!http.put(path, data).ok())
    {
        throw std::runtime_error("Couldn't HTTP PUT to " + path);
    }
}

std::string HttpDriver::sanitize(std::string path)
{
    std::string result;

    for (const auto c : path)
    {
        auto it(sanitizers.find(c));

        if (it == sanitizers.end())
        {
            result += c;
        }
        else
        {
            result += it->second;
        }
    }

    return result;
}

Curl::Curl()
    : m_curl(0)
    , m_headers(0)
    , m_data()
{
    m_curl = curl_easy_init();
}

Curl::~Curl()
{
    curl_easy_cleanup(m_curl);
    curl_slist_free_all(m_headers);
    m_headers = 0;
}

void Curl::init(std::string path, const std::vector<std::string>& headers)
{
    // Reset our curl instance and header list.
    curl_slist_free_all(m_headers);
    m_headers = 0;

    // Set path.
    curl_easy_setopt(m_curl, CURLOPT_URL, path.c_str());

    // Needed for multithreaded Curl usage.
    curl_easy_setopt(m_curl, CURLOPT_NOSIGNAL, 1L);

    // Substantially faster DNS lookups without IPv6.
    curl_easy_setopt(m_curl, CURLOPT_IPRESOLVE, CURL_IPRESOLVE_V4);

    // Don't wait forever.
    curl_easy_setopt(m_curl, CURLOPT_TIMEOUT, 120);

    // Configuration options.
    if (verbose)        curl_easy_setopt(m_curl, CURLOPT_VERBOSE, 1L);
    if (followRedirect) curl_easy_setopt(m_curl, CURLOPT_FOLLOWLOCATION, 1L);

    // Insert supplied headers.
    for (std::size_t i(0); i < headers.size(); ++i)
    {
        m_headers = curl_slist_append(m_headers, headers[i].c_str());
    }
}

HttpResponse Curl::get(std::string path, std::vector<std::string> headers)
{
    int httpCode(0);
    std::vector<char> data;

    path = HttpDriver::sanitize(path);
    init(path, headers);

    // Register callback function and date pointer to consume the result.
    curl_easy_setopt(m_curl, CURLOPT_WRITEFUNCTION, getCb);
    curl_easy_setopt(m_curl, CURLOPT_WRITEDATA, &data);

    // Insert all headers into the request.
    curl_easy_setopt(m_curl, CURLOPT_HTTPHEADER, m_headers);

    // Run the command.
    curl_easy_perform(m_curl);
    curl_easy_getinfo(m_curl, CURLINFO_RESPONSE_CODE, &httpCode);

    curl_easy_reset(m_curl);
    return HttpResponse(httpCode, data);
}

HttpResponse Curl::put(
        std::string path,
        const std::vector<char>& data,
        std::vector<std::string> headers)
{
    path = HttpDriver::sanitize(path);
    init(path, headers);

    int httpCode(0);

    std::unique_ptr<PutData> putData(new PutData(data));

    // Register callback function and data pointer to create the request.
    curl_easy_setopt(m_curl, CURLOPT_READFUNCTION, putCb);
    curl_easy_setopt(m_curl, CURLOPT_READDATA, putData.get());

    // Insert all headers into the request.
    curl_easy_setopt(m_curl, CURLOPT_HTTPHEADER, m_headers);

    // Specify that this is a PUT request.
    curl_easy_setopt(m_curl, CURLOPT_PUT, 1L);

    // Must use this for binary data, otherwise curl will use strlen(), which
    // will likely be incorrect.
    curl_easy_setopt(
            m_curl,
            CURLOPT_INFILESIZE_LARGE,
            static_cast<curl_off_t>(data.size()));

    // Hide Curl's habit of printing things to console even with verbose set
    // to false.
    curl_easy_setopt(m_curl, CURLOPT_WRITEFUNCTION, eatLogging);

    // Run the command.
    curl_easy_perform(m_curl);
    curl_easy_getinfo(m_curl, CURLINFO_RESPONSE_CODE, &httpCode);

    curl_easy_reset(m_curl);
    return HttpResponse(httpCode);
}

///////////////////////////////////////////////////////////////////////////////

HttpResource::HttpResource(
        HttpPool& pool,
        Curl& curl,
        const std::size_t id,
        const std::size_t retry)
    : m_pool(pool)
    , m_curl(curl)
    , m_id(id)
    , m_retry(retry)
{ }

HttpResource::~HttpResource()
{
    m_pool.release(m_id);
}

HttpResponse HttpResource::get(
        const std::string path,
        const Headers headers)
{
    auto f([this, path, headers]()->HttpResponse
    {
        return m_curl.get(path, headers);
    });

    return exec(f);
}

HttpResponse HttpResource::put(
        std::string path,
        const std::vector<char>& data,
        Headers headers)
{
    auto f([this, path, &data, headers]()->HttpResponse
    {
        return m_curl.put(path, data, headers);
    });

    return exec(f);
}

HttpResponse HttpResource::exec(std::function<HttpResponse()> f)
{
    HttpResponse res;
    std::size_t tries(0);

    do
    {
        res = f();
    }
    while (res.retry() && tries++ < m_retry);

    return res;
}

///////////////////////////////////////////////////////////////////////////////

HttpPool::HttpPool(std::size_t concurrent, std::size_t retry)
    : m_curls(concurrent)
    , m_available(concurrent)
    , m_retry(retry)
    , m_mutex()
    , m_cv()
{
    for (std::size_t i(0); i < concurrent; ++i)
    {
        m_available[i] = i;
        m_curls[i].reset(new Curl());
    }
}

HttpResource HttpPool::acquire()
{
    std::unique_lock<std::mutex> lock(m_mutex);
    m_cv.wait(lock, [this]()->bool { return !m_available.empty(); });

    const std::size_t id(m_available.back());
    Curl& curl(*m_curls[id]);

    m_available.pop_back();

    return HttpResource(*this, curl, id, m_retry);
}

void HttpPool::release(const std::size_t id)
{
    std::unique_lock<std::mutex> lock(m_mutex);
    m_available.push_back(id);
    lock.unlock();

    m_cv.notify_one();
}

} // namespace arbiter


// //////////////////////////////////////////////////////////////////////
// End of content of file: arbiter/drivers/http.cpp
// //////////////////////////////////////////////////////////////////////






// //////////////////////////////////////////////////////////////////////
// Beginning of content of file: arbiter/drivers/s3.cpp
// //////////////////////////////////////////////////////////////////////

#ifndef ARBITER_IS_AMALGAMATION
#include <arbiter/drivers/s3.hpp>
#endif

#include <algorithm>
#include <cctype>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <functional>
#include <iostream>
#include <thread>

#ifndef ARBITER_IS_AMALGAMATION
#include <arbiter/arbiter.hpp>
#include <arbiter/drivers/fs.hpp>
#include <arbiter/third/xml/xml.hpp>
#include <arbiter/util/crypto.hpp>
#endif

namespace arbiter
{

namespace
{
    const std::string baseUrl(".s3.amazonaws.com/");

    std::string getQueryString(const Query& query)
    {
        std::string result;

        bool first(true);
        for (const auto& q : query)
        {
            result += (first ? "?" : "&") + q.first + "=" + q.second;
            first = false;
        }

        return result;
    }

    struct Resource
    {
        Resource(std::string fullPath)
        {
            const std::size_t split(fullPath.find("/"));

            bucket = fullPath.substr(0, split);

            if (split != std::string::npos)
            {
                object = fullPath.substr(split + 1);
            }
        }

        std::string buildPath(Query query = Query()) const
        {
            const std::string queryString(getQueryString(query));
            return "http://" + bucket + baseUrl + object + queryString;
        }

        std::string bucket;
        std::string object;
    };

    typedef Xml::xml_node<> XmlNode;

    const std::string badResponse("Unexpected contents in AWS response");
}

AwsAuth::AwsAuth(const std::string access, const std::string hidden)
    : m_access(access)
    , m_hidden(hidden)
{ }

std::unique_ptr<AwsAuth> AwsAuth::find(std::string user)
{
    std::unique_ptr<AwsAuth> auth;

    if (user.empty())
    {
        user = getenv("AWS_PROFILE") ? getenv("AWS_PROFILE") : "default";
    }

    FsDriver fs;
    std::unique_ptr<std::string> file(fs.tryGet("~/.aws/credentials"));

    // First, try reading credentials file.
    if (file)
    {
        std::size_t index(0);
        std::size_t pos(0);
        std::vector<std::string> lines;

        do
        {
            index = file->find('\n', pos);
            std::string line(file->substr(pos, index - pos));

            line.erase(
                    std::remove_if(line.begin(), line.end(), ::isspace),
                    line.end());

            lines.push_back(line);

            pos = index + 1;
        }
        while (index != std::string::npos);

        if (lines.size() >= 3)
        {
            std::size_t i(0);

            const std::string userFind("[" + user + "]");
            const std::string accessFind("aws_access_key_id=");
            const std::string hiddenFind("aws_secret_access_key=");

            while (i < lines.size() - 2 && !auth)
            {
                if (lines[i].find(userFind) != std::string::npos)
                {
                    const std::string& accessLine(lines[i + 1]);
                    const std::string& hiddenLine(lines[i + 2]);

                    std::size_t accessPos(accessLine.find(accessFind));
                    std::size_t hiddenPos(hiddenLine.find(hiddenFind));

                    if (
                            accessPos != std::string::npos &&
                            hiddenPos != std::string::npos)
                    {
                        const std::string access(
                                accessLine.substr(
                                    accessPos + accessFind.size(),
                                    accessLine.find(';')));

                        const std::string hidden(
                                hiddenLine.substr(
                                    hiddenPos + hiddenFind.size(),
                                    hiddenLine.find(';')));

                        auth.reset(new AwsAuth(access, hidden));
                    }
                }

                ++i;
            }
        }
    }

    // Fall back to environment settings.
    if (!auth)
    {
        if (getenv("AWS_ACCESS_KEY_ID") && getenv("AWS_SECRET_ACCESS_KEY"))
        {
            auth.reset(
                    new AwsAuth(
                        getenv("AWS_ACCESS_KEY_ID"),
                        getenv("AWS_SECRET_ACCESS_KEY")));
        }
        else if (
                getenv("AMAZON_ACCESS_KEY_ID") &&
                getenv("AMAZON_SECRET_ACCESS_KEY"))
        {
            auth.reset(
                    new AwsAuth(
                        getenv("AMAZON_ACCESS_KEY_ID"),
                        getenv("AMAZON_SECRET_ACCESS_KEY")));
        }
    }

    return auth;
}

std::string AwsAuth::access() const
{
    return m_access;
}

std::string AwsAuth::hidden() const
{
    return m_hidden;
}

S3Driver::S3Driver(HttpPool& pool, const AwsAuth auth)
    : m_pool(pool)
    , m_auth(auth)
{ }

bool S3Driver::get(std::string rawPath, std::vector<char>& data) const
{
    return get(rawPath, Query(), data);
}

bool S3Driver::get(
        std::string rawPath,
        const Query& query,
        std::vector<char>& data,
        const Headers userHeaders) const
{
    rawPath = HttpDriver::sanitize(rawPath);
    const Resource resource(rawPath);

    const std::string path(resource.buildPath(query));

    Headers headers(httpGetHeaders(rawPath));
    headers.insert(headers.end(), userHeaders.begin(), userHeaders.end());

    auto http(m_pool.acquire());

    HttpResponse res(http.get(path, headers));

    if (res.ok())
    {
        data = res.data();
        return true;
    }
    else
    {
        return false;
    }
}

std::vector<char> S3Driver::getBinary(
        std::string rawPath,
        Headers headers) const
{
    std::vector<char> data;

    if (!get(Arbiter::stripType(rawPath), Query(), data, headers))
    {
        throw std::runtime_error("Couldn't S3 GET " + rawPath);
    }

    return data;
}

std::string S3Driver::get(std::string rawPath, Headers headers) const
{
    std::vector<char> data(getBinary(rawPath, headers));
    return std::string(data.begin(), data.end());
}

std::vector<char> S3Driver::get(std::string rawPath, const Query& query) const
{
    std::vector<char> data;

    if (!get(rawPath, query, data))
    {
        throw std::runtime_error("Couldn't S3 GET " + rawPath);
    }

    return data;
}

void S3Driver::put(std::string rawPath, const std::vector<char>& data) const
{
    const Resource resource(rawPath);

    const std::string path(resource.buildPath());
    const Headers headers(httpPutHeaders(rawPath));

    auto http(m_pool.acquire());

    if (!http.put(path, data, headers).ok())
    {
        throw std::runtime_error("Couldn't S3 PUT to " + rawPath);
    }
}

std::vector<std::string> S3Driver::glob(std::string path, bool verbose) const
{
    std::vector<std::string> results;
    path.pop_back();

    // https://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketGET.html
    const Resource resource(path);
    const std::string& bucket(resource.bucket);
    const std::string& object(resource.object);
    const std::string prefix(resource.object.empty() ? "" : resource.object);

    Query query;

    if (prefix.size()) query["prefix"] = prefix;

    bool more(false);

    do
    {
        if (verbose) std::cout << "." << std::flush;

        auto data = get(resource.bucket + "/", query);
        data.push_back('\0');

        Xml::xml_document<> xml;

        // May throw Xml::parse_error.
        xml.parse<0>(data.data());

        if (XmlNode* topNode = xml.first_node("ListBucketResult"))
        {
            if (XmlNode* truncNode = topNode->first_node("IsTruncated"))
            {
                std::string t(truncNode->value());
                std::transform(t.begin(), t.end(), t.begin(), tolower);

                more = (t == "true");
            }

            if (XmlNode* conNode = topNode->first_node("Contents"))
            {
                for ( ; conNode; conNode = conNode->next_sibling())
                {
                    if (XmlNode* keyNode = conNode->first_node("Key"))
                    {
                        std::string key(keyNode->value());

                        // The prefix may contain slashes (i.e. is a sub-dir)
                        // but we only include the top level after that.
                        if (key.find('/', prefix.size()) == std::string::npos)
                        {
                            results.push_back("s3://" + bucket + "/" + key);

                            if (more)
                            {
                                query["marker"] =
                                    object + key.substr(prefix.size());
                            }
                        }
                    }
                    else
                    {
                        throw std::runtime_error(badResponse);
                    }
                }
            }
            else
            {
                throw std::runtime_error(badResponse);
            }
        }
        else
        {
            throw std::runtime_error(badResponse);
        }

        xml.clear();
    }
    while (more);

    return results;
}

std::vector<std::string> S3Driver::httpGetHeaders(std::string filePath) const
{
    const std::string httpDate(getHttpDate());
    const std::string signedEncodedString(
            getSignedEncodedString(
                "GET",
                filePath,
                httpDate));

    const std::string dateHeader("Date: " + httpDate);
    const std::string authHeader(
            "Authorization: AWS " +
            m_auth.access() + ":" +
            signedEncodedString);
    std::vector<std::string> headers;
    headers.push_back(dateHeader);
    headers.push_back(authHeader);
    return headers;
}

std::vector<std::string> S3Driver::httpPutHeaders(std::string filePath) const
{
    const std::string httpDate(getHttpDate());
    const std::string signedEncodedString(
            getSignedEncodedString(
                "PUT",
                filePath,
                httpDate,
                "application/octet-stream"));

    const std::string typeHeader("Content-Type: application/octet-stream");
    const std::string dateHeader("Date: " + httpDate);
    const std::string authHeader(
            "Authorization: AWS " +
            m_auth.access() + ":" +
            signedEncodedString);

    std::vector<std::string> headers;
    headers.push_back(typeHeader);
    headers.push_back(dateHeader);
    headers.push_back(authHeader);
    headers.push_back("Transfer-Encoding:");
    headers.push_back("Expect:");
    return headers;
}

std::string S3Driver::getHttpDate() const
{
    time_t rawTime;
    char charBuf[80];

    time(&rawTime);

#ifndef ARBITER_WINDOWS
    tm* timeInfoPtr = localtime(&rawTime);
#else
    tm timeInfo;
    localtime_s(&timeInfo, &rawTime);
    tm* timeInfoPtr(&timeInfo);
#endif

    strftime(charBuf, 80, "%a, %d %b %Y %H:%M:%S %z", timeInfoPtr);
    std::string stringBuf(charBuf);

    return stringBuf;
}

std::string S3Driver::getSignedEncodedString(
        std::string command,
        std::string file,
        std::string httpDate,
        std::string contentType) const
{
    const std::string toSign(getStringToSign(
                command,
                file,
                httpDate,
                contentType));

    const std::vector<char> signedData(signString(toSign));
    return encodeBase64(signedData);
}

std::string S3Driver::getStringToSign(
        std::string command,
        std::string file,
        std::string httpDate,
        std::string contentType) const
{
    return
        command + "\n" +
        "\n" +
        contentType + "\n" +
        httpDate + "\n" +
        "/" + file;
}

std::vector<char> S3Driver::signString(std::string input) const
{
    return crypto::hmacSha1(m_auth.hidden(), input);
}

std::string S3Driver::encodeBase64(std::vector<char> data) const
{
    std::vector<uint8_t> input;
    for (std::size_t i(0); i < data.size(); ++i)
    {
        char c(data[i]);
        input.push_back(*reinterpret_cast<uint8_t*>(&c));
    }

    const std::string vals(
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/");

    std::size_t fullSteps(input.size() / 3);
    while (input.size() % 3) input.push_back(0);
    uint8_t* pos(input.data());
    uint8_t* end(input.data() + fullSteps * 3);

    std::string output(fullSteps * 4, '_');
    std::size_t outIndex(0);

    const uint32_t mask(0x3F);

    while (pos != end)
    {
        uint32_t chunk((*pos) << 16 | *(pos + 1) << 8 | *(pos + 2));

        output[outIndex++] = vals[(chunk >> 18) & mask];
        output[outIndex++] = vals[(chunk >> 12) & mask];
        output[outIndex++] = vals[(chunk >>  6) & mask];
        output[outIndex++] = vals[chunk & mask];

        pos += 3;
    }

    if (end != input.data() + input.size())
    {
        const std::size_t num(pos - end == 1 ? 2 : 3);
        uint32_t chunk(*(pos) << 16 | *(pos + 1) << 8 | *(pos + 2));

        output.push_back(vals[(chunk >> 18) & mask]);
        output.push_back(vals[(chunk >> 12) & mask]);
        if (num == 3) output.push_back(vals[(chunk >> 6) & mask]);
    }

    while (output.size() % 4) output.push_back('=');

    return output;
}

} // namespace arbiter


// //////////////////////////////////////////////////////////////////////
// End of content of file: arbiter/drivers/s3.cpp
// //////////////////////////////////////////////////////////////////////






// //////////////////////////////////////////////////////////////////////
// Beginning of content of file: arbiter/util/crypto.cpp
// //////////////////////////////////////////////////////////////////////

#ifndef ARBITER_IS_AMALGAMATION
#include <arbiter/util/crypto.hpp>
#endif

#include <cstdint>

#define ROTLEFT(a, b) ((a << b) | (a >> (32 - b)))

namespace arbiter
{
namespace crypto
{
namespace
{
    const std::size_t block(64);

    std::vector<char> append(
            const std::vector<char>& a,
            const std::vector<char>& b)
    {
        std::vector<char> out(a);
        out.insert(out.end(), b.begin(), b.end());
        return out;
    }

    std::vector<char> append(
            const std::vector<char>& a,
            const std::string& b)
    {
        return append(a, std::vector<char>(b.begin(), b.end()));
    }

    // SHA1 implementation:
    //      https://github.com/B-Con/crypto-algorithms
    //
    // HMAC:
    //      https://en.wikipedia.org/wiki/Hash-based_message_authentication_code
    //
    typedef struct
    {
        uint8_t data[64];
        uint32_t datalen;
        unsigned long long bitlen;
        uint32_t state[5];
        uint32_t k[4];
    } SHA1_CTX;

    void sha1_transform(SHA1_CTX *ctx, const uint8_t* data)
    {
        uint32_t a, b, c, d, e, i, j, t, m[80];

        for (i = 0, j = 0; i < 16; ++i, j += 4)
        {
            m[i] =
                (data[j] << 24) + (data[j + 1] << 16) +
                (data[j + 2] << 8) + (data[j + 3]);
        }

        for ( ; i < 80; ++i)
        {
            m[i] = (m[i - 3] ^ m[i - 8] ^ m[i - 14] ^ m[i - 16]);
            m[i] = (m[i] << 1) | (m[i] >> 31);
        }

        a = ctx->state[0];
        b = ctx->state[1];
        c = ctx->state[2];
        d = ctx->state[3];
        e = ctx->state[4];

        for (i = 0; i < 20; ++i) {
            t = ROTLEFT(a, 5) + ((b & c) ^ (~b & d)) + e + ctx->k[0] + m[i];
            e = d;
            d = c;
            c = ROTLEFT(b, 30);
            b = a;
            a = t;
        }
        for ( ; i < 40; ++i) {
            t = ROTLEFT(a, 5) + (b ^ c ^ d) + e + ctx->k[1] + m[i];
            e = d;
            d = c;
            c = ROTLEFT(b, 30);
            b = a;
            a = t;
        }
        for ( ; i < 60; ++i) {
            t = ROTLEFT(a, 5) + ((b & c) ^ (b & d) ^ (c & d)) + e +
                ctx->k[2] + m[i];
            e = d;
            d = c;
            c = ROTLEFT(b, 30);
            b = a;
            a = t;
        }
        for ( ; i < 80; ++i) {
            t = ROTLEFT(a, 5) + (b ^ c ^ d) + e + ctx->k[3] + m[i];
            e = d;
            d = c;
            c = ROTLEFT(b, 30);
            b = a;
            a = t;
        }

        ctx->state[0] += a;
        ctx->state[1] += b;
        ctx->state[2] += c;
        ctx->state[3] += d;
        ctx->state[4] += e;
    }

    void sha1_init(SHA1_CTX *ctx)
    {
        ctx->datalen = 0;
        ctx->bitlen = 0;
        ctx->state[0] = 0x67452301;
        ctx->state[1] = 0xEFCDAB89;
        ctx->state[2] = 0x98BADCFE;
        ctx->state[3] = 0x10325476;
        ctx->state[4] = 0xc3d2e1f0;
        ctx->k[0] = 0x5a827999;
        ctx->k[1] = 0x6ed9eba1;
        ctx->k[2] = 0x8f1bbcdc;
        ctx->k[3] = 0xca62c1d6;
    }

    void sha1_update(SHA1_CTX *ctx, const uint8_t* data, size_t len)
    {
        for (std::size_t i(0); i < len; ++i)
        {
            ctx->data[ctx->datalen] = data[i];
            ++ctx->datalen;
            if (ctx->datalen == 64)
            {
                sha1_transform(ctx, ctx->data);
                ctx->bitlen += 512;
                ctx->datalen = 0;
            }
        }
    }

    void sha1_final(SHA1_CTX *ctx, uint8_t* hash)
    {
        uint32_t i;

        i = ctx->datalen;

        // Pad whatever data is left in the buffer.
        if (ctx->datalen < 56)
        {
            ctx->data[i++] = 0x80;
            while (i < 56)
                ctx->data[i++] = 0x00;
        }
        else
        {
            ctx->data[i++] = 0x80;
            while (i < 64)
                ctx->data[i++] = 0x00;
            sha1_transform(ctx, ctx->data);
            std::memset(ctx->data, 0, 56);
        }

        // Append to the padding the total message's length in bits and
        // transform.
        ctx->bitlen += ctx->datalen * 8;
        ctx->data[63] = static_cast<uint8_t>(ctx->bitlen);
        ctx->data[62] = static_cast<uint8_t>(ctx->bitlen >> 8);
        ctx->data[61] = static_cast<uint8_t>(ctx->bitlen >> 16);
        ctx->data[60] = static_cast<uint8_t>(ctx->bitlen >> 24);
        ctx->data[59] = static_cast<uint8_t>(ctx->bitlen >> 32);
        ctx->data[58] = static_cast<uint8_t>(ctx->bitlen >> 40);
        ctx->data[57] = static_cast<uint8_t>(ctx->bitlen >> 48);
        ctx->data[56] = static_cast<uint8_t>(ctx->bitlen >> 56);
        sha1_transform(ctx, ctx->data);

        // Since this implementation uses little endian byte ordering and MD
        // uses big endian, reverse all the bytes when copying the final state
        // to the output hash.
        for (i = 0; i < 4; ++i)
        {
            hash[i]      = (ctx->state[0] >> (24 - i * 8)) & 0x000000ff;
            hash[i + 4]  = (ctx->state[1] >> (24 - i * 8)) & 0x000000ff;
            hash[i + 8]  = (ctx->state[2] >> (24 - i * 8)) & 0x000000ff;
            hash[i + 12] = (ctx->state[3] >> (24 - i * 8)) & 0x000000ff;
            hash[i + 16] = (ctx->state[4] >> (24 - i * 8)) & 0x000000ff;
        }
    }

    std::vector<char> sha1(const std::vector<char>& data)
    {
        SHA1_CTX ctx;
        std::vector<char> out(20);

        sha1_init(&ctx);
        sha1_update(
                &ctx,
                reinterpret_cast<const uint8_t*>(data.data()),
                data.size());
        sha1_final(&ctx, reinterpret_cast<uint8_t*>(out.data()));

        return out;
    }

    std::string sha1(const std::string& data)
    {
        auto hashed(sha1(std::vector<char>(data.begin(), data.end())));
        return std::string(hashed.begin(), hashed.end());
    }

} // unnamed namespace

std::vector<char> hmacSha1(std::string key, const std::string message)
{
    if (key.size() > block) key = sha1(key);
    if (key.size() < block) key.insert(key.end(), block - key.size(), 0);

    std::vector<char> okeypad(block, 0x5c);
    std::vector<char> ikeypad(block, 0x36);

    for (std::size_t i(0); i < block; ++i)
    {
        okeypad[i] ^= key[i];
        ikeypad[i] ^= key[i];
    }

    return sha1(append(okeypad, sha1(append(ikeypad, message))));
}

} // namespace crypto
} // namespace arbiter

// //////////////////////////////////////////////////////////////////////
// End of content of file: arbiter/util/crypto.cpp
// //////////////////////////////////////////////////////////////////////






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

namespace arbiter
{

namespace
{
    const std::string delimiter("://");
}

Arbiter::Arbiter(AwsAuth* awsAuth)
    : m_drivers()
    , m_pool(32, 8)
{
    m_drivers["fs"] = std::make_shared<FsDriver>(FsDriver());
    m_drivers["http"] = std::make_shared<HttpDriver>(HttpDriver(m_pool));

    if (awsAuth)
    {
        m_drivers["s3"] =
            std::make_shared<S3Driver>(S3Driver(m_pool, *awsAuth));
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

void Arbiter::put(const std::string path, const std::string& data)
{
    return getDriver(path).put(stripType(path), data);
}

void Arbiter::put(const std::string path, const std::vector<char>& data)
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

Driver& Arbiter::getDriver(const std::string path) const
{
    return *m_drivers.at(parseType(path));
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

std::string Driver::get(std::string path)
{
    const std::vector<char> data(getBinary(path));
    return std::string(data.begin(), data.end());
}

void Driver::put(std::string path, const std::string& data)
{
    put(path, std::vector<char>(data.begin(), data.end()));
}

std::vector<std::string> Driver::resolve(std::string path, bool verbose)
{
    std::vector<std::string> results;

    if (path.size() > 2 && path.substr(path.size() - 2) == "/*")
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

Endpoint::Endpoint(Driver& driver, const std::string root)
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

std::string Endpoint::getSubpath(const std::string subpath)
{
    return m_driver.get(fullPath(subpath));
}

std::vector<char> Endpoint::getSubpathBinary(const std::string subpath)
{
    return m_driver.getBinary(fullPath(subpath));
}

void Endpoint::putSubpath(const std::string subpath, const std::string& data)
{
    m_driver.put(fullPath(subpath), data);
}

void Endpoint::putSubpath(
        const std::string subpath,
        const std::vector<char>& data)
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

#include <fstream>
#include <glob.h>
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
}

std::vector<char> FsDriver::getBinary(const std::string path)
{
    std::ifstream stream(path, std::ios::in | std::ios::binary);

    if (!stream.good())
    {
        throw std::runtime_error("Could not read file " + path);
    }

    stream.seekg(0, std::ios::end);
    std::vector<char> data(static_cast<std::size_t>(stream.tellg()));
    stream.seekg(0, std::ios::beg);
    stream.read(data.data(), data.size());
    stream.close();

    return data;
}

void FsDriver::put(const std::string path, const std::vector<char>& data)
{
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

std::vector<std::string> FsDriver::glob(const std::string path, bool)
{
    // TODO Platform dependent.
    std::vector<std::string> results;

    glob_t buffer;

    ::glob(path.c_str(), GLOB_NOSORT | GLOB_TILDE, 0, &buffer);

    for (std::size_t i(0); i < buffer.gl_pathc; ++i)
    {
        results.push_back(buffer.gl_pathv[i]);
    }

    globfree(&buffer);

    return results;
}

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
}

namespace arbiter
{

HttpDriver::HttpDriver(HttpPool& pool)
    : m_pool(pool)
{ }

std::vector<char> HttpDriver::getBinary(const std::string path)
{
    auto http(m_pool.acquire());
    HttpResponse res(http.get(path));

    if (res.ok()) return res.data();
    else throw std::runtime_error("Couldn't HTTP GET " + path);
}

void HttpDriver::put(const std::string path, const std::vector<char>& data)
{
    auto http(m_pool.acquire());

    if (!http.put(path, data).ok())
    {
        throw std::runtime_error("Couldn't HTTP PUT to " + path);
    }
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
    curl_easy_setopt(m_curl, CURLOPT_INFILESIZE_LARGE, data.size());

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
#include <chrono>
#include <cstring>
#include <functional>
#include <iostream>
#include <thread>

#ifndef ARBITER_IS_AMALGAMATION
#include <arbiter/third/xml/xml.hpp>
#endif

#include <openssl/hmac.h>

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

std::vector<char> S3Driver::getBinary(const std::string rawPath)
{
    return get(rawPath, Query());
}

std::vector<char> S3Driver::get(const std::string rawPath, const Query& query)
{
    const Resource resource(rawPath);

    const std::string path(resource.buildPath(query));
    const Headers headers(httpGetHeaders(rawPath));

    auto http(m_pool.acquire());

    HttpResponse res(http.get(path, headers));

    if (res.ok())
    {
        return res.data();
    }
    else
    {
        // TODO If verbose:
        std::cout << std::string(res.data().begin(), res.data().end()) <<
            std::endl;
        throw std::runtime_error("Couldn't S3 GET " + rawPath);
    }
}

void S3Driver::put(std::string rawPath, const std::vector<char>& data)
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

std::vector<std::string> S3Driver::glob(std::string path, bool verbose)
{
    std::vector<std::string> results;

    if (path.size() < 2 || path.substr(path.size() - 2) != "/*")
    {
        throw std::runtime_error("Invalid glob path: " + path);
    }

    path.resize(path.size() - 2);

    // https://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketGET.html
    const Resource resource(path);
    const std::string& bucket(resource.bucket);
    const std::string& object(resource.object);
    const std::string prefix(
            resource.object.empty() ? "" : resource.object + "/");

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

            XmlNode* conNode(topNode->first_node("Contents"));

            if (!conNode) throw std::runtime_error(badResponse);

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
                                (object.size() ? object + "/" : "") +
                                key.substr(prefix.size());
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
    tm* timeInfo = localtime(&rawTime);

    strftime(charBuf, 80, "%a, %d %b %Y %H:%M:%S %z", timeInfo);
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
    std::vector<char> hash(20, ' ');
    unsigned int outLength(0);

    HMAC_CTX ctx;
    HMAC_CTX_init(&ctx);

    HMAC_Init(
            &ctx,
            m_auth.hidden().data(),
            m_auth.hidden().size(),
            EVP_sha1());
    HMAC_Update(
            &ctx,
            reinterpret_cast<const uint8_t*>(input.data()),
            input.size());
    HMAC_Final(
            &ctx,
            reinterpret_cast<uint8_t*>(hash.data()),
            &outLength);
    HMAC_CTX_cleanup(&ctx);

    return hash;
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






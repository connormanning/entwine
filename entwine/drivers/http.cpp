/******************************************************************************
* Copyright (c) 2016, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <entwine/drivers/http.hpp>

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

    const bool followRedirect(true);
    const bool verbose(false);
}

namespace entwine
{

Curl::Curl(std::size_t id)
    : m_curl(0)
    , m_headers(0)
    , m_data()
    , m_id(id)
{
    m_curl = curl_easy_init();
}

Curl::~Curl()
{
    curl_easy_cleanup(m_curl);
    curl_slist_free_all(m_headers);
    m_headers = 0;
}

void Curl::init(std::string url, const std::vector<std::string>& headers)
{
    // Reset our curl instance and header list.
    curl_slist_free_all(m_headers);
    m_headers = 0;

    // Set URL.
    curl_easy_setopt(m_curl, CURLOPT_URL, url.c_str());

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

HttpResponse Curl::get(std::string url, std::vector<std::string> headers)
{
    init(url, headers);

    int httpCode(0);
    std::shared_ptr<std::vector<char>> data(new std::vector<char>());

    // Register callback function and date pointer to consume the result.
    curl_easy_setopt(m_curl, CURLOPT_WRITEFUNCTION, getCb);
    curl_easy_setopt(m_curl, CURLOPT_WRITEDATA, data.get());

    // Insert all headers into the request.
    curl_easy_setopt(m_curl, CURLOPT_HTTPHEADER, m_headers);

    // Run the command.
    curl_easy_perform(m_curl);
    curl_easy_getinfo(m_curl, CURLINFO_RESPONSE_CODE, &httpCode);

    HttpResponse res(httpCode, data);
    curl_easy_reset(m_curl);
    return res;
}

HttpResponse Curl::put(
        std::string url,
        std::vector<std::string> headers,
        const std::vector<char>& data)
{
    init(url, headers);

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

    // Run the command.
    curl_easy_perform(m_curl);
    curl_easy_getinfo(m_curl, CURLINFO_RESPONSE_CODE, &httpCode);

    curl_easy_reset(m_curl);
    return HttpResponse(httpCode);
}

///////////////////////////////////////////////////////////////////////////////

CurlBatch::CurlBatch(std::size_t id, std::size_t batchSize)
    : m_available(batchSize)
    , m_curls(batchSize, 0)
    , m_id(id)
    , m_mutex()
    , m_cv()
{
    for (std::size_t i(0); i < batchSize; ++i)
    {
        m_available[i] = i;
        m_curls[i].reset(new Curl(i));
    }
}

HttpResponse CurlBatch::get(
        std::string url,
        std::vector<std::string> headers)
{
    auto curl(acquire());
    HttpResponse res(curl->get(url, headers));
    release(curl);
    return res;
}

HttpResponse CurlBatch::put(
        std::string url,
        std::vector<std::string> headers,
        const std::vector<char>& data)
{
    auto curl(acquire());
    HttpResponse res(curl->put(url, headers, data));
    release(curl);
    return res;
}

std::shared_ptr<Curl> CurlBatch::acquire()
{
    std::unique_lock<std::mutex> lock(m_mutex);
    m_cv.wait(lock, [this]()->bool { return !m_available.empty(); });
    auto curl(m_curls[m_available.back()]);
    m_available.pop_back();
    lock.unlock();
    return curl;
}

void CurlBatch::release(std::shared_ptr<Curl> curl)
{
    std::unique_lock<std::mutex> lock(m_mutex);
    m_available.push_back(curl->id());
    lock.unlock();
    m_cv.notify_one();
}

///////////////////////////////////////////////////////////////////////////////

CurlPool::CurlPool(std::size_t numBatches, std::size_t batchSize)
    : m_available(numBatches)
    , m_curlBatches()
    , m_mutex()
    , m_cv()
{
    for (std::size_t i(0); i < numBatches; ++i)
    {
        m_available[i] = i;
        m_curlBatches.insert(std::make_pair(
                    i,
                    std::shared_ptr<CurlBatch>(new CurlBatch(i, batchSize))));
    }
}

std::shared_ptr<CurlBatch> CurlPool::acquire()
{
    std::unique_lock<std::mutex> lock(m_mutex);
    m_cv.wait(lock, [this]()->bool { return !m_curlBatches.empty(); });
    std::shared_ptr<CurlBatch> curlBatch(m_curlBatches[m_available.back()]);
    m_available.pop_back();
    lock.unlock();
    return curlBatch;
}

void CurlPool::release(std::shared_ptr<CurlBatch> curlBatch)
{
    std::unique_lock<std::mutex> lock(m_mutex);
    m_available.push_back(curlBatch->id());
    lock.unlock();
    m_cv.notify_one();
}

} // namespace entwine


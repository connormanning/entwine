/******************************************************************************
* Copyright (c) 2016, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <entwine/drivers/s3.hpp>

#include <chrono>
#include <cstring>
#include <functional>
#include <iostream>
#include <thread>

#include <openssl/hmac.h>

namespace entwine
{

namespace
{
    const std::size_t httpAttempts(60);
    const auto baseSleepTime(std::chrono::milliseconds(1));
    const auto maxSleepTime (std::chrono::milliseconds(4096));

    const std::string baseUrl(".s3.amazonaws.com/");

    // TODO Configure.  Also move this elsewhere.
    const std::size_t curlNumBatches(16);
    const std::size_t curlBatchSize(64);
    entwine::CurlPool curlPool(curlNumBatches, curlBatchSize);

    std::size_t split(std::string fullPath)
    {
        if (fullPath.back() == '/') fullPath.pop_back();

        const std::size_t pos(fullPath.find("/"));

        if (pos == std::string::npos || pos + 1 >= fullPath.size())
        {
            throw std::runtime_error("Invalid bucket specification");
        }

        return pos;
    }

    std::string getBucket(std::string fullPath)
    {
        return fullPath.substr(0, split(fullPath));
    }

    std::string getObject(std::string fullPath)
    {
        return fullPath.substr(split(fullPath) + 1);
    }
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

S3Driver::S3Driver(const AwsAuth& auth)
    : m_auth(auth)
    , m_curlBatch(curlPool.acquire())
{ }

S3Driver::~S3Driver()
{
    curlPool.release(m_curlBatch);
}

std::vector<char> S3Driver::get(const std::string path)
{
    auto getFunc([this, path]()->HttpResponse { return tryGet(path); });

    HttpResponse res(httpExec(getFunc, httpAttempts));

    if (res.code() != 200)
    {
        std::cout <<
            res.code() << ": " <<
            std::string(res.data().begin(), res.data().end()) <<
            std::endl;
        throw std::runtime_error("Couldn't fetch " + path);
    }

    return res.data();
}

void S3Driver::put(std::string path, const std::vector<char>& data)
{
    auto putFunc([this, path, &data]()->HttpResponse
    {
        return tryPut(path, data);
    });

    if (httpExec(putFunc, httpAttempts).code() != 200)
    {
        throw std::runtime_error("Couldn't write " + path);
    }
}

HttpResponse S3Driver::httpExec(
        std::function<HttpResponse()> f,
        const std::size_t tries)
{
    std::size_t fails(0);
    HttpResponse res;
    auto sleepTime(baseSleepTime);

    do
    {
        res = f();

        if (res.code() != 200)
        {
            std::this_thread::sleep_for(sleepTime);
            sleepTime *= 2;
            if (sleepTime > maxSleepTime) sleepTime = maxSleepTime;
        }
    }
    while (res.code() != 200 && ++fails < tries);

    return res;
}

HttpResponse S3Driver::tryGet(std::string path)
{
    const std::string endpoint(
            "http://" + getBucket(path) + baseUrl + getObject(path));
    return m_curlBatch->get(endpoint, httpGetHeaders(path));
}

HttpResponse S3Driver::tryPut(std::string path, const std::vector<char>& data)
{
    const std::string endpoint(
            "http://" + getBucket(path) + baseUrl + getObject(path));
    return m_curlBatch->put(endpoint, httpPutHeaders(path), data);
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

} // namespace entwine


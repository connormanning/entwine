/******************************************************************************
* Copyright (c) 2016, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <entwine/http/s3.hpp>

#include <iostream>
#include <cstring>
#include <thread>

#include <openssl/hmac.h>

#include <entwine/http/collector.hpp>

namespace
{
    // TODO Configure.
    const std::size_t curlNumBatches(16);
    const std::size_t curlBatchSize(64);
    static entwine::CurlPool curlPool(curlNumBatches, curlBatchSize);
}

namespace entwine
{

S3::S3(
        std::string awsAccessKeyId,
        std::string awsSecretAccessKey,
        std::string baseAwsUrl,
        std::string bucketName)
    : m_awsAccessKeyId(awsAccessKeyId)
    , m_awsSecretAccessKey(awsSecretAccessKey)
    , m_baseAwsUrl(baseAwsUrl)
    , m_bucketName(prefixSlash(bucketName))
    , m_curlBatch(curlPool.acquire())
{ }

S3::S3(const S3Info& s3Info)
    : m_awsAccessKeyId(s3Info.awsAccessKeyId)
    , m_awsSecretAccessKey(s3Info.awsSecretAccessKey)
    , m_baseAwsUrl(s3Info.baseAwsUrl)
    , m_bucketName(prefixSlash(s3Info.bucketName))
    , m_curlBatch(curlPool.acquire())
{ }

S3::~S3()
{
    curlPool.release(m_curlBatch);
}

std::vector<std::string> S3::getHeaders(std::string filePath) const
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
            m_awsAccessKeyId + ":" +
            signedEncodedString);
    std::vector<std::string> headers;
    headers.push_back(dateHeader);
    headers.push_back(authHeader);
    return headers;
}

std::vector<std::string> S3::putHeaders(std::string filePath) const
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
            m_awsAccessKeyId + ":" +
            signedEncodedString);

    std::vector<std::string> headers;
    headers.push_back(typeHeader);
    headers.push_back(dateHeader);
    headers.push_back(authHeader);
    headers.push_back("Transfer-Encoding:");
    headers.push_back("Expect:");
    return headers;
}

HttpResponse S3::get(std::string file)
{
    const std::string filePath(m_bucketName + prefixSlash(file));
    const std::string endpoint("http://" + m_baseAwsUrl + filePath);

    return m_curlBatch->get(endpoint, getHeaders(filePath));
}

void S3::get(uint64_t id, std::string file, GetCollector* collector)
{
    std::thread t([this, id, file, collector]() {
        collector->insert(id, file, get(file));
    });

    t.detach();
}

HttpResponse S3::put(
        std::string file,
        const std::shared_ptr<std::vector<char>> data)
{
    const std::string filePath(m_bucketName + prefixSlash(file));
    const std::string endpoint("http://" + m_baseAwsUrl + filePath);

    return m_curlBatch->put(endpoint, putHeaders(filePath), data);
}

HttpResponse S3::put(std::string file, const std::string& data)
{
    std::shared_ptr<std::vector<char>> vec(
            new std::vector<char>(data.begin(), data.end()));
    return put(file, vec);
}

void S3::put(
        uint64_t id,
        std::string file,
        const std::shared_ptr<std::vector<char>> data,
        PutCollector* collector)
{
    std::thread t([this, id, file, data, collector]() {
        collector->insert(id, put(file, data), data);
    });

    t.detach();
}

std::string S3::getHttpDate() const
{
    time_t rawTime;
    char charBuf[80];

    time(&rawTime);
    tm* timeInfo = localtime(&rawTime);

    strftime(charBuf, 80, "%a, %d %b %Y %H:%M:%S %z", timeInfo);
    std::string stringBuf(charBuf);

    return stringBuf;
}

std::string S3::getSignedEncodedString(
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

std::string S3::getStringToSign(
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
        file;
}

std::vector<char> S3::signString(std::string input) const
{
    std::vector<char> hash(20, ' ');
    unsigned int outLength(0);

    HMAC_CTX ctx;
    HMAC_CTX_init(&ctx);

    HMAC_Init(
            &ctx,
            m_awsSecretAccessKey.data(),
            m_awsSecretAccessKey.size(),
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

std::string S3::encodeBase64(std::vector<char> data) const
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

std::string S3::prefixSlash(const std::string& in) const
{
    if (in.size() && in[0] != '/') return "/" + in;
    else return in;
}

} // namespace entwine


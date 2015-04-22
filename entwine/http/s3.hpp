/******************************************************************************
* Copyright (c) 2016, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#pragma once

#include <string>
#include <vector>

#include <entwine/http/curl.hpp>

namespace entwine
{

struct S3Info
{
    S3Info(
            std::string baseAwsUrl,
            std::string bucketName,
            std::string awsAccessKeyId,
            std::string awsSecretAccessKey)
        : exists(true)
        , baseAwsUrl(baseAwsUrl)
        , bucketName(bucketName)
        , awsAccessKeyId(awsAccessKeyId)
        , awsSecretAccessKey(awsSecretAccessKey)
    { }

    S3Info()
        : exists(false)
        , baseAwsUrl()
        , bucketName()
        , awsAccessKeyId()
        , awsSecretAccessKey()
    { }

    const bool exists;
    const std::string baseAwsUrl;
    const std::string bucketName;
    const std::string awsAccessKeyId;
    const std::string awsSecretAccessKey;
};

class S3
{
public:
    S3(
            std::string awsAccessKeyId,
            std::string awsSecretAccessKey,
            std::string baseAwsUrl = "s3.amazonaws.com",
            std::string bucketName = "");

    S3(const S3Info& s3Info);

    ~S3();

    HttpResponse get(std::string file);

    HttpResponse put(std::string file, const std::vector<char>& data);
    HttpResponse put(std::string file, const std::string& data);

    const std::string& baseAwsUrl() const { return m_baseAwsUrl; }
    const std::string& bucketName() const { return m_bucketName; }

private:
    std::vector<std::string> getHeaders(std::string filePath) const;
    std::vector<std::string> putHeaders(std::string filePath) const;

    std::string getHttpDate() const;

    std::string getSignedEncodedString(
            std::string command,
            std::string file,
            std::string httpDate,
            std::string contentType = "") const;

    std::string getStringToSign(
            std::string command,
            std::string file,
            std::string httpDate,
            std::string contentType) const;

    std::vector<char> signString(std::string input) const;
    std::string encodeBase64(std::vector<char> input) const;
    std::string prefixSlash(const std::string& in) const;

    const std::string m_awsAccessKeyId;
    const std::string m_awsSecretAccessKey;
    const std::string m_baseAwsUrl;
    const std::string m_bucketName;

    std::shared_ptr<CurlBatch> m_curlBatch;
};

} // namespace entwine


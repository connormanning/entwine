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

#include <memory>
#include <string>
#include <vector>

#include <entwine/drivers/driver.hpp>
#include <entwine/http/curl.hpp>

namespace entwine
{

class AwsAuth
{
public:
    AwsAuth(std::string access, std::string hidden);

    std::string access() const;
    std::string hidden() const;

private:
    std::string m_access;
    std::string m_hidden;
};

class S3Driver : public Driver
{
public:
    S3Driver(const AwsAuth& awsAuth);
    ~S3Driver();

    virtual std::vector<char> get(std::string path);
    virtual void put(std::string path, const std::vector<char>& data);

private:
    // TODO Move somewhere else.
    HttpResponse httpExec(std::function<HttpResponse()> f, std::size_t tries);

    HttpResponse tryGet(std::string file);
    HttpResponse tryPut(std::string file, const std::vector<char>& data);

    std::vector<std::string> httpGetHeaders(std::string filePath) const;
    std::vector<std::string> httpPutHeaders(std::string filePath) const;

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

    AwsAuth m_auth;
    std::shared_ptr<CurlBatch> m_curlBatch;
};

} // namespace entwine


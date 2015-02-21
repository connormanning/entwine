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

#include <set>
#include <cstdint>
#include <memory>
#include <vector>

#include "s3.hpp"

// Collector classes to gather asynchronous S3 responses.

class Collector
{
public:
    Collector(std::size_t numExpected);
    virtual ~Collector();

    // Blocks until the specified number of responses have been received.
    void waitFor(std::size_t num);

    // Returns true if one or more 500 errors were received.
    bool shouldSlowDown() const;
    void shouldSlowDown(bool val);

protected:
    // Increment our receive count.
    void inc();

    const std::size_t m_expected;
    std::size_t m_received;
    bool m_shouldSlowDown;
    std::set<std::size_t> m_waitList;

    std::mutex m_mutex;
    std::condition_variable m_cv;
};

class PutCollector : public Collector
{
public:
    PutCollector(std::size_t numExpected);
    ~PutCollector();

    // Insert a response.
    void insert(
            uint64_t id,
            HttpResponse res,
            const std::shared_ptr<std::vector<uint8_t>> data);

    // Returns list of errors.
    // NOTE: This will block until all expected responses have been received.
    // NOTE: This is a destructive operation, which will clear the error
    // list from the Collector.
    std::map<uint64_t, const std::shared_ptr<std::vector<uint8_t>>> errs();

private:
    std::map<uint64_t, const std::shared_ptr<std::vector<uint8_t>>> m_errs;
};

class GetCollector : public Collector
{
public:
    GetCollector(std::size_t numExpected);
    ~GetCollector();

    void insert(uint64_t id, std::string file, HttpResponse res);

    // Returns list of errors.
    // NOTE: This will block until all expected responses have been received.
    // NOTE: This is a destructive operation, which will clear the error
    // list from the Collector.
    std::map<uint64_t, std::string> errs();

    // This call assumes that all HTTP activity is complete, so a caller must
    // first make sure the result of errs() is empty, which will ensure that
    // all responses have been correctly received.
    std::map<uint64_t, const std::shared_ptr<std::vector<uint8_t>>> responses();

private:
    std::map<uint64_t, const std::shared_ptr<std::vector<uint8_t>>> m_data;
    std::map<uint64_t, std::string> m_errs;
};


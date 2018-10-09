/******************************************************************************
* Copyright (c) 2018, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#pragma once

#include "entwine.hpp"

#include <entwine/builder/config.hpp>
#include <entwine/third/arbiter/arbiter.hpp>
#include <entwine/types/key.hpp>
#include <entwine/util/pool.hpp>

namespace entwine
{
namespace app
{

class Update : public App
{
private:
    virtual void addArgs() override;
    virtual void run() override;

    void copyHierarchy(const Dxyz& key = Dxyz()) const;
    void copyFileMetadata() const;

    std::unique_ptr<arbiter::Arbiter> m_arbiter;
    std::unique_ptr<arbiter::Endpoint> m_ep;
    std::unique_ptr<Pool> m_pool;
    Config m_config;

    Json::Value m_metadata;
};

} // namespace app
} // namespace entwine


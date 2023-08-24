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

#include <cmath>
#include <cstddef>
#include <memory>
#include <stdexcept>
#include <string>

#include <entwine/io/io.hpp>
#include <entwine/third/arbiter/arbiter.hpp>
#include <entwine/types/bounds.hpp>
#include <entwine/types/defs.hpp>
#include <entwine/types/dimension.hpp>
#include <entwine/types/endpoints.hpp>
#include <entwine/types/metadata.hpp>
#include <entwine/types/reprojection.hpp>
#include <entwine/types/source.hpp>
#include <entwine/types/srs.hpp>
#include <entwine/types/subset.hpp>
#include <entwine/types/threads.hpp>
#include <entwine/types/version.hpp>
#include <entwine/util/json.hpp>
#include <entwine/util/optional.hpp>
#include <entwine/util/unique.hpp>

namespace entwine
{
namespace config
{

Endpoints getEndpoints(const json& j);
Metadata getMetadata(const json& j);

std::unique_ptr<arbiter::Arbiter> getArbiter(const json& j);
StringList getInput(const json& j);
std::string getOutput(const json& j);
std::string getTmp(const json& j);

io::Type getDataType(const json& j);

Bounds getBoundsConforming(const json& j);
Bounds getBounds(const json& j);
Schema getSchema(const json& j);
optional<Reprojection> getReprojection(const json& j);
optional<Srs> getSrs(const json& j);
optional<Subset> getSubset(const json& j);
optional<Scale> getScale(const json& j);

json getPipeline(const json& j);

unsigned getThreads(const json& j);
Threads getCompoundThreads(const json& j);
Version getEptVersion(const json& j);

bool getVerbose(const json& j);
bool getDeep(const json& j);
bool getStats(const json& j);
bool getForce(const json& j);
bool getAbsolute(const json& j);
bool getAllowOriginId(const json& j);
bool getWithSchemaStats(const json& j);

uint64_t getSpan(const json& j);
uint64_t getMinNodeSize(const json& j);
uint64_t getMaxNodeSize(const json& j);
uint64_t getCacheSize(const json& j);
uint64_t getSleepCount(const json& j);
uint64_t getProgressInterval(const json& j);
uint64_t getLimit(const json& j);
uint64_t getHierarchyStep(const json& j);

} // namespace config
} // namespace entwine

/******************************************************************************
* Copyright (c) 2018, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include "update.hpp"

#include <entwine/types/files.hpp>

namespace entwine
{
namespace app
{

void Update::addArgs()
{
    m_ap.setUsage("entwine update <path> (<options>)");

    addOutput("Path to update", true);
    addConfig();
    addSimpleThreads();
    addArbiter();
}

void Update::run()
{
    m_config = Config(m_json);

    m_arbiter = makeUnique<arbiter::Arbiter>(m_config["arbiter"]);
    m_ep = makeUnique<arbiter::Endpoint>(
            m_arbiter->getEndpoint(m_config.output()));
    m_pool = makeUnique<Pool>(m_config.totalThreads());

    m_metadata = parse(m_ep->get("entwine.json"));

    if (m_ep->isLocal())
    {
        arbiter::fs::mkdirp(m_ep->root() + "ept-hierarchy");
        arbiter::fs::mkdirp(m_ep->root() + "ept-metadata");
    }

    std::cout << "Updating hierarchy... " << std::flush;
    copyHierarchy();
    m_pool->await();
    std::cout << "done." << std::endl;

    std::cout << "Updating per-file metadata... " << std::flush;
    copyFileMetadata();
    std::cout << "done." << std::endl;

    std::cout << "Updating EPT control files..." << std::flush;

    // Build metadata.
    Json::Value buildMeta(parse(m_ep->get("entwine-build.json")));
    buildMeta["software"] = "Entwine";
    buildMeta["version"] = currentVersion().toString();
    m_ep->put("ept-build.json", toPreciseString(buildMeta));

    // Main metadata.
    Schema schema(m_metadata["schema"]);

    if (m_metadata.isMember("scale"))
    {
        Scale scale(m_metadata["scale"]);
        Offset offset(m_metadata["offset"]);

        Json::Value removed;
        m_metadata.removeMember("scale", &removed);
        m_metadata.removeMember("offset", &removed);

        schema.setScaleOffset(scale, offset);
    }

    m_metadata["schema"] = schema.toJson();

    Srs srs(m_metadata["srs"].asString());
    m_metadata["srs"] = srs.toJson();

    m_ep->put("ept.json", toPreciseString(m_metadata));
    std::cout << "done." << std::endl;

    std::cout << "Update complete." << std::endl;
}

void Update::copyHierarchy(const Dxyz& root) const
{
    const auto fromEp(m_ep->getSubEndpoint("h/"));
    const auto toEp(m_ep->getSubEndpoint("ept-hierarchy"));

    const uint64_t hierarchyStep(m_metadata["hierarchyStep"].asUInt64());

    const auto json(parse(fromEp.get(root.toString() + ".json")));
    toEp.put(
            root.toString() + ".json",
            root.depth() ? toFastString(json) : json.toStyledString());

    for (const auto str : json.getMemberNames())
    {
        const Dxyz key(str);
        if (
                hierarchyStep &&
                key.depth() > root.depth() &&
                key.depth() % hierarchyStep == 0)
        {
            m_pool->add([this, key]() { copyHierarchy(key); });
        }
    }
}

void Update::copyFileMetadata() const
{
    const Files files(parse(m_ep->get("entwine-files.json")));
    files.save(*m_ep, "", m_config, true);
}

} // namespace app
} // namespace entwine


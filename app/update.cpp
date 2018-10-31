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
    m_json["verbose"] = true;
}

void Update::run()
{
    m_config = Config(m_json);

    m_arbiter = makeUnique<arbiter::Arbiter>(m_config["arbiter"]);
    m_ep = makeUnique<arbiter::Endpoint>(
            m_arbiter->getEndpoint(m_config.output()));
    m_pool = makeUnique<Pool>(std::max<uint64_t>(4, m_config.totalThreads()));

    m_metadata = parse(m_ep->get("entwine.json"));

    if (m_ep->isLocal())
    {
        arbiter::fs::mkdirp(m_ep->root() + "ept-hierarchy");
        arbiter::fs::mkdirp(m_ep->root() + "ept-metadata");
    }

    std::cout << "Updating hierarchy..." << std::endl;
    copyHierarchy();
    std::cout << "done." << std::endl;

    std::cout << "Updating per-file metadata... " << std::flush;
    copyFileMetadata();
    std::cout << "done." << std::endl;

    std::cout << "Updating EPT control files..." << std::flush;

    // Build metadata.
    Json::Value buildMeta(parse(m_ep->get("entwine-build.json")));
    buildMeta["software"] = "Entwine";
    buildMeta["version"] = currentEptVersion().toString();
    m_ep->put("ept-build.json", toPreciseString(buildMeta));

    // Main metadata.
    Schema schema(m_metadata["schema"]);

    Json::Value removed;
    if (m_metadata.isMember("scale"))
    {
        Scale scale(m_metadata["scale"]);
        Offset offset(m_metadata["offset"]);

        m_metadata.removeMember("scale", &removed);
        m_metadata.removeMember("offset", &removed);

        schema.setScaleOffset(scale, offset);
    }

    m_metadata["schema"] = schema.toJson();

    Srs srs(m_metadata["srs"].asString());
    m_metadata["srs"] = srs.toJson();
    m_metadata.removeMember("hierarchyStep", &removed);

    m_ep->put("ept.json", toPreciseString(m_metadata));
    std::cout << "done." << std::endl;

    std::cout << "Update complete." << std::endl;
}

void Update::copyHierarchy() const
{
    const std::string from(m_ep->prefixedRoot() + "h/*");
    const std::vector<std::string> files(m_arbiter->resolve(from));
    const auto toEp(m_ep->getSubEndpoint("ept-hierarchy"));
    const uint64_t hierarchyStep(m_metadata["hierarchyStep"].asUInt64());

    uint64_t i(0);

    for (const auto file : files)
    {
        if (i % 1000 == 0)
        {
            std::cout << "\t" << i << " / " << files.size() << std::endl;
        }
        ++i;

        Dxyz root;
        bool copy(false);

        try
        {
            // If this is a subset hierarchy, it will be formatted as
            // D-X-Y-Z-S, we don't want to copy it.
            root = Dxyz(arbiter::Arbiter::stripExtension(
                        arbiter::util::getBasename(file)));
            copy = true;
        }
        catch (...) { }

        if (copy)
        {
            m_pool->add([this, &toEp, file, root, hierarchyStep]()
            {
                Json::Value json(parse(m_arbiter->get(file)));

                for (const auto str : json.getMemberNames())
                {
                    const Dxyz key(str);

                    if (
                            hierarchyStep &&
                            key.depth() > root.depth() &&
                            key.depth() % hierarchyStep == 0)
                    {
                        json[str] = -1;
                    }
                }

                toEp.put(
                        root.toString() + ".json",
                        root.depth() ?
                            toFastString(json) : json.toStyledString());
            });
        }
    }

    m_pool->await();
}

void Update::copyFileMetadata() const
{
    const Files files(parse(m_ep->get("entwine-files.json")));
    files.save(*m_ep, "", m_config, true);
}

} // namespace app
} // namespace entwine


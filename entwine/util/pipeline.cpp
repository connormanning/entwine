/******************************************************************************
* Copyright (c) 2019, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <entwine/util/pipeline.hpp>

#include <algorithm>

#include <pdal/io/LasReader.hpp>

namespace entwine
{

json::const_iterator findStage(const json& pipeline, const std::string type)
{
    return std::find_if(
        pipeline.begin(),
        pipeline.end(),
        [type](const json& stage) { return stage.value("type", "") == type; });
}

json::iterator findStage(json& pipeline, const std::string type)
{
    return std::find_if(
        pipeline.begin(),
        pipeline.end(),
        [type](const json& stage) { return stage.value("type", "") == type; });
}

json& findOrAppendStage(json& pipeline, const std::string type)
{
    auto it = findStage(pipeline, type);
    if (it != pipeline.end()) return *it;

    pipeline.push_back({ { "type", type } });
    return pipeline.back();
}

json omitStage(json pipeline, const std::string type)
{
    auto it = findStage(pipeline, type);
    if (it == pipeline.end()) return pipeline;

    pipeline.erase(it);
    return pipeline;
}

pdal::Stage* findStage(pdal::Stage& last, const std::string type)
{
    pdal::Stage* current(&last);

    do
    {
        if (current->getName() == type) return current;

        if (current->getInputs().size() > 1)
        {
            throw std::runtime_error("Invalid pipeline - must be linear");
        }

        current = current->getInputs().size()
            ? current->getInputs().at(0)
            : nullptr;
    }
    while (current);

    return nullptr;
}

pdal::Stage& getStage(pdal::PipelineManager& pm)
{
    if (pdal::Stage* s = pm.getStage()) return *s;
    throw std::runtime_error("Invalid pipeline - no stages");
}

pdal::Reader& getReader(pdal::Stage& last)
{
    pdal::Stage& first(getFirst(last));

    if (pdal::Reader* reader = dynamic_cast<pdal::Reader*>(&first))
    {
        return *reader;
    }

    throw std::runtime_error("Invalid pipeline - must start with reader");
}

pdal::Stage& getFirst(pdal::Stage& last)
{
    pdal::Stage* current(&last);

    while (current->getInputs().size())
    {
        if (current->getInputs().size() > 1)
        {
            throw std::runtime_error("Invalid pipeline - must be linear");
        }

        current = current->getInputs().at(0);
    }

    return *current;
}

json getMetadata(pdal::Reader& reader)
{
    return json::parse(pdal::Utils::toJSON(reader.getMetadata()));
}

optional<ScaleOffset> getScaleOffset(const pdal::Reader& reader)
{
    if (const auto* las = dynamic_cast<const pdal::LasReader*>(&reader))
    {
        const auto& h(las->header());
        return ScaleOffset(
            Scale(h.scaleX(), h.scaleY(), h.scaleZ()),
            Offset(h.offsetX(), h.offsetY(), h.offsetZ())
        );
    }
    return { };
}

} // namespace entwine

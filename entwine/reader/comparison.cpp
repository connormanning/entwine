/******************************************************************************
* Copyright (c) 2016, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <entwine/reader/comparison.hpp>

#include <json/json.h>

#include <pdal/Dimension.hpp>
#include <pdal/util/Utils.hpp>

#include <entwine/types/files.hpp>
#include <entwine/types/metadata.hpp>
#include <entwine/types/schema.hpp>
#include <entwine/util/json.hpp>

namespace entwine
{

namespace
{

double extractComparisonValue(
        const Metadata& metadata,
        const std::string& dimensionName,
        const Json::Value& val)
{
    double d(0);

    if (dimensionName == "Path")
    {
        if (!val.isString())
        {
            throw std::runtime_error(
                    "Invalid path - must be string: " + val.toStyledString());
        }

        // If this dimension is a path, we need to convert the path
        // string to an Origin.
        const std::string path(val.asString());
        const Origin origin(metadata.files().find(path));
        if (origin == invalidOrigin)
        {
            throw std::runtime_error("Could not find path: " + path);
        }

        d = origin;
    }
    else
    {
        if (!val.isConvertibleTo(Json::ValueType::realValue))
        {
            throw std::runtime_error(
                    "Invalid comparison value: " + val.toStyledString());
        }

        if (dimensionName == "OriginId")
        {
            const Origin origin(val.asUInt64());
            if (origin > metadata.files().size())
            {
                throw std::runtime_error(
                        "Could not find origin: " + std::to_string(origin));
            }
        }

        d = val.asDouble();
    }

    return d;
}

std::unique_ptr<Bounds> maybeExtractBounds(
        const Metadata& metadata,
        const std::string& dimensionName,
        const double val,
        const ComparisonType type)
{
    std::unique_ptr<Bounds> b;

    if (dimensionName == "Path" || dimensionName == "OriginId")
    {
        const Origin origin(val);
        const auto& fileInfo(metadata.files().get(origin));

        // File info is stored absolutely positioned, so convert it into the
        // local coordinate system for the index.
        if (const Bounds* bounds = fileInfo.bounds())
        {
            b = makeUnique<Bounds>(*bounds);
        }
        else
        {
            throw std::runtime_error(
                "Could not extract bounds for origin: " +
                std::to_string(origin));
        }
    }
    else
    {
        pdal::Dimension::Id id(pdal::Dimension::id(dimensionName));

        // Optimize spatial filters if they are inequalities.
        if (DimInfo::isXyz(id))
        {
            auto min(Bounds::everything().min());
            auto max(Bounds::everything().max());
            const std::size_t pos(pdal::Utils::toNative(id) - 1);

            bool inequality(false);

            if (type == ComparisonType::lt || type == ComparisonType::lte)
            {
                max[pos] = val;
                inequality = true;
            }
            else if (type == ComparisonType::gt || type == ComparisonType::gte)
            {
                min[pos] = val;
                inequality = true;
            }

            if (inequality)
            {
                b = makeUnique<Bounds>(min, max);
            }
        }
    }

    return b;
}

} // unnamed namespace

std::unique_ptr<Comparison> Comparison::create(
        const Metadata& metadata,
        std::string dimensionName,
        const Json::Value& val)
{
    auto op(ComparisonOperator::create(metadata, dimensionName, val));
    if (dimensionName == "Path") dimensionName = "OriginId";

    const auto id(metadata.schema().getId(dimensionName));
    if (id == pdal::Dimension::Id::Unknown)
    {
        throw std::runtime_error("Unknown dimension: " + dimensionName);
    }

    return makeUnique<Comparison>(id, dimensionName, std::move(op));
}

std::unique_ptr<ComparisonOperator> ComparisonOperator::create(
        const Metadata& metadata,
        const std::string& dimensionName,
        const Json::Value& json)
{
    if (json.isObject())
    {
        if (json.size() != 1)
        {
            throw std::runtime_error(
                    "Invalid comparison object: " + json.toStyledString());
        }

        const auto key(json.getMemberNames().at(0));
        const ComparisonType co(toComparisonType(key));
        const auto& val(json[key]);

        if (isSingle(co))
        {
            const double d(
                    extractComparisonValue(metadata, dimensionName, val));

            auto ub(maybeExtractBounds(metadata, dimensionName, d, co));
            auto b(ub.get());

            if (dimensionName == "Path" || dimensionName == "OriginId")
            {
                if (co != ComparisonType::eq)
                {
                    throw std::runtime_error(
                            toString(co) + " not supported for dimension: " +
                            dimensionName);
                }
            }

            switch (co)
            {
            case ComparisonType::eq:
                return createSingle(co, std::equal_to<double>(), d, b);
                break;
            case ComparisonType::gt:
                return createSingle(co, std::greater<double>(), d, b);
                break;
            case ComparisonType::gte:
                return createSingle(co, std::greater_equal<double>(), d, b);
                break;
            case ComparisonType::lt:
                return createSingle(co, std::less<double>(), d, b);
                break;
            case ComparisonType::lte:
                return createSingle(co, std::less_equal<double>(), d, b);
                break;
            case ComparisonType::ne:
                return createSingle(co, std::not_equal_to<double>(), d, b);
                break;
            default:
                throw std::runtime_error("Invalid single comparison operator");
            }
        }
        else
        {
            if (!val.isArray())
            {
                throw std::runtime_error("Invalid comparison list");
            }

            std::vector<double> vals;
            std::vector<Bounds> boundsList;

            for (const Json::Value& single : val)
            {
                const double d(
                        extractComparisonValue(
                            metadata,
                            dimensionName,
                            single));

                vals.push_back(d);

                if (auto b = maybeExtractBounds(metadata, dimensionName, d, co))
                {
                    boundsList.push_back(*b);
                }
            }

            if (dimensionName == "Path" || dimensionName == "OriginId")
            {
                if (co != ComparisonType::in)
                {
                    throw std::runtime_error(
                            toString(co) + " not supported for dimension: " +
                            dimensionName);
                }
            }

            if (co == ComparisonType::in)
            {
                return makeUnique<ComparisonAny>(vals, boundsList);
            }
            else if (co == ComparisonType::nin)
            {
                return makeUnique<ComparisonNone>(vals, boundsList);
            }
            else throw std::runtime_error("Invalid multi comparison operator");
        }
    }
    else
    {
        Json::Value next;
        next["$eq"] = json;
        return create(metadata, dimensionName, next);
    }
}

} // namespace entwine


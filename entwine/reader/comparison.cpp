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

#include <pdal/Dimension.hpp>
#include <pdal/util/Utils.hpp>

#include <entwine/types/files.hpp>
#include <entwine/types/metadata.hpp>
#include <entwine/types/schema.hpp>

namespace entwine
{

namespace
{

double extractComparisonValue(
        const Metadata& metadata,
        const std::string& dimName,
        const json& val)
{
    double d(0);

    if (dimName == "Path")
    {
        if (!val.is_string())
        {
            throw std::runtime_error(
                    "Invalid path - must be string: " + val.dump(2));
        }

        // If this dimension is a path, we need to convert the path
        // string to an Origin.
        const std::string path(val.get<std::string>());
        const Origin origin(metadata.files().find(path));
        if (origin == invalidOrigin)
        {
            throw std::runtime_error("Could not find path: " + path);
        }

        d = origin;
    }
    else
    {
        if (!val.is_number())
        {
            throw std::runtime_error(
                    "Invalid comparison value: " + val.dump(2));
        }

        if (dimName == "OriginId")
        {
            const Origin origin(val.get<uint64_t>());
            if (origin > metadata.files().size())
            {
                throw std::runtime_error(
                        "Could not find origin: " + std::to_string(origin));
            }
        }

        d = val.get<double>();
    }

    return d;
}

std::unique_ptr<Bounds> maybeExtractBounds(
        const Metadata& metadata,
        const std::string& dimName,
        const double val,
        const ComparisonType type)
{
    std::unique_ptr<Bounds> b;

    if (dimName == "Path" || dimName == "OriginId")
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
        pdal::Dimension::Id id(pdal::Dimension::id(dimName));

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
        std::string dimName,
        const json& val)
{
    auto op(ComparisonOperator::create(metadata, dimName, val));
    if (dimName == "Path") dimName = "OriginId";

    const auto id(metadata.schema().getId(dimName));
    if (id == pdal::Dimension::Id::Unknown)
    {
        throw std::runtime_error("Unknown dimension: " + dimName);
    }

    return makeUnique<Comparison>(id, dimName, std::move(op));
}

std::unique_ptr<ComparisonOperator> ComparisonOperator::create(
        const Metadata& metadata,
        const std::string& dimName,
        const json& j)
{
    if (!j.is_object())
    {
        return create(metadata, dimName, json { { "$eq", j } });
    }

    if (j.size() != 1)
    {
        throw std::runtime_error("Invalid comparison object: " + j.dump(2));
    }

    const auto front(j.begin());
    const auto key(front.key());
    const ComparisonType co(toComparisonType(key));
    const auto& val(front.value());

    if (isSingle(co))
    {
        const double d(extractComparisonValue(metadata, dimName, val));

        auto ub(maybeExtractBounds(metadata, dimName, d, co));
        auto b(ub.get());

        if (dimName == "Path" || dimName == "OriginId")
        {
            if (co != ComparisonType::eq)
            {
                throw std::runtime_error(
                        toString(co) + " not supported for dimension: " +
                        dimName);
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
        if (!val.is_array())
        {
            throw std::runtime_error("Invalid comparison list");
        }

        std::vector<double> vals;
        std::vector<Bounds> boundsList;

        for (const auto& single : val)
        {
            const double d(extractComparisonValue(metadata, dimName, single));

            vals.push_back(d);

            if (auto b = maybeExtractBounds(metadata, dimName, d, co))
            {
                boundsList.push_back(*b);
            }
        }

        if (dimName == "Path" || dimName == "OriginId")
        {
            if (co != ComparisonType::in)
            {
                throw std::runtime_error(
                        toString(co) + " not supported for dimension: " +
                        dimName);
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

} // namespace entwine


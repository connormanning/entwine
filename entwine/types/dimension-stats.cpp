/******************************************************************************
* Copyright (c) 2019, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <entwine/types/dimension-stats.hpp>

#include <entwine/types/defs.hpp>

namespace entwine
{
namespace dimension
{

Stats::Stats(const pdal::stats::Summary& s)
    : minimum(s.minimum())
    , maximum(s.maximum())
    , mean(s.average())
    , variance(s.populationVariance())
    , count(s.count())
{
    for (const auto& bucket : s.values()) values[bucket.first] = bucket.second;
}

Stats::Stats(const json& j)
    : minimum(j.value<double>("minimum", 0))
    , maximum(j.value<double>("maximum", 0))
    , mean(j.value<double>("mean", j.value<double>("average", 0)))
    , variance(j.value<double>("variance", 0))
    , count(j.value<uint64_t>("count", 0))
{
    if (j.count("counts"))
    {
        for (const auto& bucket : j.at("counts"))
        {
            if (j.is_object())
            {
                values[bucket.at("value").get<double>()] =
                    bucket.at("count").get<uint64_t>();
            }
            else if (j.is_string())
            {
                const StringList split =
                    pdal::Utils::split(j.get<std::string>(), '/');
                if (split.size() != 2)
                {
                    throw std::runtime_error("Invalid counts length");
                }
                values[std::stod(split[0])] = std::stoull(split[1]);
            }
            else throw std::runtime_error("Invalid dimension counts");
        }
    }
}

void to_json(json& j, const Stats& stats)
{
    j = {
        { "minimum", getTypedValue(stats.minimum) },
        { "maximum", getTypedValue(stats.maximum) },
        { "mean", getTypedValue(stats.mean) },
        { "variance", getTypedValue(stats.variance) },
        { "stddev", getTypedValue(std::sqrt(stats.variance)) },
        { "count", stats.count }
    };

    for (const auto& p : stats.values)
    {
        j["counts"].push_back({
            { "value", getTypedValue(p.first) },
            { "count", p.second }
        });
    }
}

void from_json(const json& j, Stats& stats) { stats = Stats(j); }

Stats combine(Stats agg, const Stats& cur)
{
    agg.minimum = std::min(agg.minimum, cur.minimum);
    agg.maximum = std::max(agg.maximum, cur.maximum);

    // Weighted variance formula from: https://math.stackexchange.com/a/2971563
    double n1 = agg.count;
    double n2 = cur.count;
    double m1 = agg.mean;
    double m2 = cur.mean;
    double v1 = agg.variance;
    double v2 = cur.variance;
    agg.variance =
        (((n1 - 1) * v1) + ((n2 - 1) * v2)) / (n1 + n2 - 1) +
        ((n1 * n2) * (m1 - m2) * (m1 - m2)) / ((n1 + n2) * (n1 + n2 - 1));

    agg.mean = ((agg.mean * agg.count) + (cur.mean * cur.count)) /
        (agg.count + cur.count);
    agg.count += cur.count;
    for (const auto& bucket : cur.values)
    {
        if (!agg.values.count(bucket.first)) agg.values[bucket.first] = 0;
        agg.values[bucket.first] += bucket.second;
    }
    return agg;
}

} // namespace dimension
} // namespace entwine

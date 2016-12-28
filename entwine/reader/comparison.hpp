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

#include <vector>

#include <entwine/reader/filterable.hpp>
#include <entwine/types/bounds.hpp>
#include <entwine/types/defs.hpp>
#include <entwine/util/unique.hpp>

namespace Json { class Value; }

namespace entwine
{

class Metadata;

enum class ComparisonType
{
    eq,
    gt,
    gte,
    lt,
    lte,
    ne,
    in,
    nin
};

inline bool isComparisonType(const std::string& s)
{
    return s.size() && s.front() == '$' && (
            s == "$eq" ||
            s == "$gt" ||
            s == "$gte" ||
            s == "$lt" ||
            s == "$lte" ||
            s == "$ne" ||
            s == "$in" ||
            s == "$nin");
}

inline ComparisonType toComparisonType(const std::string& s)
{
    if (s == "$eq")         return ComparisonType::eq;
    else if (s == "$gt")    return ComparisonType::gt;
    else if (s == "$gte")   return ComparisonType::gte;
    else if (s == "$lt")    return ComparisonType::lt;
    else if (s == "$lte")   return ComparisonType::lte;
    else if (s == "$ne")    return ComparisonType::ne;
    else if (s == "$in")    return ComparisonType::in;
    else if (s == "$nin")   return ComparisonType::nin;
    else throw std::runtime_error("Invalid comparison type: " + s);
}

inline std::string toString(ComparisonType c)
{
    switch (c)
    {
        case ComparisonType::eq: return "$eq";
        case ComparisonType::gt: return "$gt";
        case ComparisonType::gte: return "$gte";
        case ComparisonType::lt: return "$lt";
        case ComparisonType::lte: return "$lte";
        case ComparisonType::ne: return "$ne";
        case ComparisonType::in: return "$in";
        case ComparisonType::nin: return "$nin";
        default: throw std::runtime_error("Invalid comparison type enum");
    }
}

inline bool isSingle(ComparisonType co)
{
    return co != ComparisonType::in && co != ComparisonType::nin;
}

inline bool isMultiple(ComparisonType co)
{
    return !isSingle(co);
}

class ComparisonOperator
{
public:
    ComparisonOperator(ComparisonType type) : m_type(type) { }

    virtual ~ComparisonOperator() { }

    // Accepts a JSON value of the form:
    // { "$<op>": <val> }       // E.g. { "$eq": 42 }
    //
    // or the special case for the "$eq" operator:
    //      <val>               // Equivalent to the above.
    //
    //
    // Returns a pointer to a functor that performs the requested comparison.
    static std::unique_ptr<ComparisonOperator> create(
            const Metadata& metadata,
            const std::string& dimensionName,
            const Json::Value& json,
            const Delta* delta);

    virtual bool operator()(double in) const = 0;
    virtual bool operator()(const Bounds& bounds) const { return true; }
    virtual void log(const std::string& pre) const = 0;

    virtual std::vector<Origin> origins() const
    {
        return std::vector<Origin>();
    }

    ComparisonType type() const { return m_type; }

protected:
    ComparisonType m_type;
};

template<typename Op>
class ComparisonSingle : public ComparisonOperator
{
public:
    ComparisonSingle(ComparisonType type, Op op, double val, const Bounds* b)
        : ComparisonOperator(type)
        , m_op(op)
        , m_val(val)
        , m_bounds(maybeClone(b))
    { }

    virtual bool operator()(double in) const override
    {
        return m_op(in, m_val);
    }

    virtual bool operator()(const Bounds& bounds) const override
    {
        return !m_bounds || m_bounds->overlaps(bounds, true);
    }

    virtual void log(const std::string& pre) const override
    {
        std::cout << pre << toString(m_type) << " " << m_val;
        if (m_bounds) std::cout << " " << *m_bounds;
        std::cout << std::endl;
    }

    virtual std::vector<Origin> origins() const override
    {
        std::vector<Origin> o;
        o.push_back(m_val);
        return o;
    }

protected:
    Op m_op;
    double m_val;
    std::unique_ptr<Bounds> m_bounds;
};

class ComparisonMulti : public ComparisonOperator
{
public:
    ComparisonMulti(
            ComparisonType type,
            const std::vector<double>& vals,
            const std::vector<Bounds>& boundsList)
        : ComparisonOperator(type)
        , m_vals(vals)
        , m_boundsList(boundsList)
    { }

    virtual void log(const std::string& pre) const override
    {
        std::cout << pre << toString(m_type) << " ";
        for (const double d : m_vals) std::cout << d << " ";
        std::cout << std::endl;

        for (const auto& b : m_boundsList)
        {
            std::cout << pre << "  " << b << std::endl;
        }
    }

protected:
    std::vector<double> m_vals;
    std::vector<Bounds> m_boundsList;
};

class ComparisonAny : public ComparisonMulti
{
public:
    ComparisonAny(
            const std::vector<double>& vals,
            const std::vector<Bounds>& boundsList)
        : ComparisonMulti(ComparisonType::in, vals, boundsList)
    { }

    virtual bool operator()(double in) const override
    {
        return std::any_of(m_vals.begin(), m_vals.end(), [in](double val)
        {
            return in == val;
        });
    }

    virtual bool operator()(const Bounds& bounds) const override
    {
        if (m_boundsList.empty()) return true;
        else
        {
            for (const auto& b : m_boundsList)
            {
                if (b.overlaps(bounds, true)) return true;
            }

            return false;
        }
    }
};

class ComparisonNone : public ComparisonMulti
{
public:
    ComparisonNone(
            const std::vector<double>& vals,
            const std::vector<Bounds>& boundsList)
        : ComparisonMulti(ComparisonType::nin, vals, boundsList)
    { }

    virtual bool operator()(double in) const override
    {
        return std::none_of(m_vals.begin(), m_vals.end(), [in](double val)
        {
            return in == val;
        });
    }
};

template<typename O>
inline std::unique_ptr<ComparisonSingle<O>> createSingle(
        ComparisonType type,
        O op,
        double d,
        const Bounds* b = nullptr)
{
    return makeUnique<ComparisonSingle<O>>(type, op, d, b);
}

class Comparison : public Filterable
{
public:
    Comparison(
            pdal::Dimension::Id dim,
            const std::string& dimensionName,
            std::unique_ptr<ComparisonOperator> op)
        : m_dim(dim)
        , m_name(dimensionName)
        , m_op(std::move(op))
    { }

    static std::unique_ptr<Comparison> create(
            const Metadata& metadata,
            std::string dimName,
            const Json::Value& val,
            const Delta* delta);

    bool check(const pdal::PointRef& pointRef) const override
    {
        return (*m_op)(pointRef.getFieldAs<double>(m_dim));
    }

    bool check(const Bounds& bounds) const override
    {
        return (*m_op)(bounds);
    }

    virtual void log(const std::string& pre) const override
    {
        std::cout << pre << m_name << " ";
        m_op->log("");
    }

protected:
    pdal::Dimension::Id m_dim;
    std::string m_name;
    std::unique_ptr<ComparisonOperator> m_op;
};

} // namespace entwine


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

#include <entwine/new-reader/filterable.hpp>

namespace entwine
{

enum class LogicalOperator
{
    lAnd,
    lOr,
    lNor
};

inline bool isLogicalOperator(const std::string& s)
{
    return s == "$and" || s == "$or" || s == "$nor";
}

inline LogicalOperator toLogicalOperator(const std::string& s)
{
    if (s == "$and")        return LogicalOperator::lAnd;
    else if (s == "$or")    return LogicalOperator::lOr;
    else if (s == "$nor")   return LogicalOperator::lNor;
    else throw std::runtime_error("Invalid logical operator: " + s);
}

class LogicGate : public Filterable
{
public:
    virtual ~LogicGate() { }

    static std::unique_ptr<LogicGate> create(const std::string& s)
    {
        return create(toLogicalOperator(s));
    }

    static std::unique_ptr<LogicGate> create(LogicalOperator type);

    void push(std::unique_ptr<Filterable> f)
    {
        m_filters.push_back(std::move(f));
    }

protected:
    std::vector<std::unique_ptr<Filterable>> m_filters;
};

class LogicalAnd : public LogicGate
{
public:
    virtual bool check(const pdal::PointRef& pointRef) const override
    {
        for (const auto& f : m_filters)
        {
            if (!f->check(pointRef)) return false;
        }

        return true;
    }

    virtual bool check(const Bounds& bounds) const override
    {
        for (const auto& f : m_filters)
        {
            if (!f->check(bounds)) return false;
        }

        return true;
    }

    virtual void log(const std::string& pre) const override
    {
        if (m_filters.size()) std::cout << pre << "AND" << std::endl;
        for (const auto& c : m_filters) c->log(pre + "  ");
    }
};

class LogicalOr : public LogicGate
{
public:
    virtual bool check(const pdal::PointRef& pointRef) const override
    {
        for (const auto& f : m_filters)
        {
            if (f->check(pointRef)) return true;
        }

        return false;
    }

    virtual bool check(const Bounds& bounds) const override
    {
        for (const auto& f : m_filters)
        {
            if (f->check(bounds)) return true;
        }

        return false;
    }

    virtual void log(const std::string& pre) const override
    {
        std::cout << pre << "OR" << std::endl;
        for (const auto& c : m_filters) c->log(pre + "  ");
    }
};

class LogicalNor : public LogicalOr
{
public:
    using LogicalOr::check;
    virtual bool check(const pdal::PointRef& pointRef) const override
    {
        return !LogicalOr::check(pointRef);
    }

    virtual bool check(const Bounds& bounds) const override
    {
        return !LogicalOr::check(bounds);
    }

    virtual void log(const std::string& pre) const override
    {
        std::cout << pre << "NOR" << std::endl;
        for (const auto& c : m_filters) c->log(pre + "  ");
    }
};

} // namespace entwine


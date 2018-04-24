/******************************************************************************
* Copyright (c) 2016, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <entwine/new-reader/logic-gate.hpp>

#include <entwine/util/unique.hpp>

namespace entwine
{

std::unique_ptr<LogicGate> LogicGate::create(const LogicalOperator type)
{
    if (type == LogicalOperator::lAnd) return makeUnique<LogicalAnd>();
    else if (type == LogicalOperator::lOr) return makeUnique<LogicalOr>();
    else if (type == LogicalOperator::lNor) return makeUnique<LogicalNor>();
    else throw std::runtime_error("Invalid logic gate type");
}

} // namespace entwine


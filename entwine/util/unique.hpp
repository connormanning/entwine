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

#include <memory>

namespace entwine
{

template<typename T, typename... Args>
std::unique_ptr<T> makeUnique(Args&&... args)
{
    return std::unique_ptr<T>(new T(std::forward<Args>(args)...));
}

template<typename T>
std::unique_ptr<T> clone(const T& t)
{
    return makeUnique<T>(t);
}

template<typename T>
std::unique_ptr<T> maybeClone(const T* t)
{
    if (t) return makeUnique<T>(*t);
    else return std::unique_ptr<T>();
}

template<typename T>
inline std::unique_ptr<T> maybeCreate(const Json::Value& json)
{
    if (!json.isNull()) return makeUnique<T>(json);
    else return std::unique_ptr<T>();
}

template<typename T>
inline std::shared_ptr<T> maybeDefault(std::shared_ptr<T> v)
{
    return v ? v : std::make_shared<T>();
}

} // namespace entwine


/******************************************************************************
* Copyright (c) 2019, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#pragma once

#include <stdexcept>

#include <entwine/util/json.hpp>
#include <entwine/util/unique.hpp>

namespace entwine
{

constexpr struct in_place_t { } in_place { };

class bad_optional_access : public std::logic_error
{
public:
    explicit bad_optional_access(const std::string& s) : std::logic_error(s) { }
    explicit bad_optional_access(const char* s) : std::logic_error(s) { }
};

// This is a very poor implementation of optional since it uses the heap, but
// it's just a stopgap until the project upgrades to C++17 and matches the API
// of std::optional.  Functionally, this implementation acts as a unique_ptr
// but with a cloning copy constructor.
template <typename T>
class optional
{
public:
    optional() = default;
    optional(const T& v) : m_value(makeUnique<T>(v)) { }
    optional(T&& v) : m_value(makeUnique<T>(std::move(v))) { }

    template <typename ...Args>
    optional(in_place_t, Args&&... args)
        : m_value(makeUnique<T>(std::forward<Args>(args)...))
    { }

    optional(const optional& other)
        : optional()
    {
        if (other) m_value = makeUnique<T>(*other);
    }

    optional(optional&& other)
        : optional()
    {
        if (other) m_value = makeUnique<T>(std::move(*other));
    }

    optional(const json& j)
        : optional()
    {
        if (!j.is_null()) m_value = makeUnique<T>(j);
    }

    optional& operator=(const optional& other)
    {
        if (!other) reset();
        else m_value = makeUnique<T>(*other);
        return *this;
    }

    // Inspection.
    bool has_value() const noexcept { return static_cast<bool>(m_value); }
    explicit operator bool() const noexcept { return has_value(); }

    // Unchecked access.
    T* operator->() { return m_value.get(); }
    const T* operator->() const { return m_value.get(); }
    T& operator*() { return *m_value; }
    const T& operator*() const { return *m_value; }

    // Checked access.
    T& value()
    {
        if (!has_value()) throw bad_optional_access("Bad optional access");
        return *this;
    }

    const T& value() const
    {
        if (!has_value()) throw bad_optional_access("Bad optional access");
        return *this;
    }

    // Modifiers.
    void reset() { m_value.reset(); }

private:
    std::unique_ptr<T> m_value;
};

template <typename T>
inline optional<T> maybeCreate(const json& j)
{
    if (!j.is_null()) return j.get<T>();
    return { };
}

template <typename T>
inline void from_json(const json& j, optional<T>& v)
{
    if (!j.is_null()) v = j.get<T>();
}

} // namespace entwine

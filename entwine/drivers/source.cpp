/******************************************************************************
* Copyright (c) 2016, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <entwine/drivers/source.hpp>

#include <entwine/drivers/driver.hpp>

namespace entwine
{

namespace
{
    const std::string delimiter("://");

    std::string parseType(const std::string raw)
    {
        std::string type;
        const std::size_t pos(raw.find(delimiter));

        if (pos == std::string::npos)
        {
            type = "fs";
        }
        else
        {
            type = raw.substr(0, pos);
        }

        return type;
    }

    std::string postfixSlash(const std::string raw)
    {
        std::string result(raw);

        if (raw.size() && raw.back() != '/')
        {
            result.push_back('/');
        }

        return result;
    }
}

Source::Source(const std::string rawPath, Driver& driver)
    : m_type(parseType(rawPath))
    , m_path(stripType(rawPath))
    , m_driver(driver)
{ }

Source::~Source()
{ }

std::vector<char> Source::getRoot()
{
    return m_driver.get(m_path);
}

std::vector<char> Source::get(const std::string subpath)
{
    return m_driver.get(resolve(subpath));
}

std::string Source::getAsString(const std::string subpath)
{
    const std::vector<char> data(get(subpath));
    return std::string(data.begin(), data.end());
}

void Source::put(const std::string subpath, const std::vector<char>& data)
{
    return m_driver.put(resolve(subpath), data);
}

void Source::put(const std::string subpath, const std::string& data)
{
    return m_driver.put(resolve(subpath), data);
}

bool Source::isRemote() const
{
    return m_driver.isRemote();
}

std::string Source::resolve(const std::string subpath) const
{
    return postfixSlash(m_path) + subpath;
}

std::string Source::type() const
{
    return m_type;
}

std::string Source::path() const
{
    return m_path;
}

std::string Source::getType(std::string rawPath)
{
    return parseType(rawPath);
}

std::string Source::stripType(const std::string raw)
{
    std::string result(raw);
    const std::size_t pos(raw.find(delimiter));

    if (pos != std::string::npos)
    {
        result = raw.substr(pos + delimiter.size());
    }

    return result;
}

} // namespace entwine


/******************************************************************************
* Copyright (c) 2019, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <entwine/util/fs.hpp>

#include <stdexcept>

namespace entwine
{

bool isDirectory(std::string path)
{
    if (path.empty()) throw std::runtime_error("Cannot specify empty path");
    const char c = path.back();
    return c == '/' || c == '\\' || c == '*' ||
        arbiter::getExtension(path).empty();
}

StringList resolve(const StringList& input, const arbiter::Arbiter& a)
{
    StringList output;
    for (std::string item : input)
    {
        if (isDirectory(item))
        {
            const char last = item.back();
            if (last != '*')
            {
                if (last != '/') item.push_back('/');
                item.push_back('*');
            }

            const StringList directory(a.resolve(item));
            for (const auto& item : directory)
            {
                if (!isDirectory(item)) output.push_back(item);
            }
        }
        else output.push_back(arbiter::expandTilde(item));
    }
    return output;
}

} // namespace entwine

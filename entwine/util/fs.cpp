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

#include <pdal/util/Utils.hpp>

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

std::string getStem(const std::string path)
{
    return arbiter::stripExtension(arbiter::getBasename(path));
}

StringList resolve(const StringList& input, const arbiter::Arbiter& a)
{
    StringList output;
    for (std::string item : input)
    {
        if (isDirectory(item) || (item.find('*') != std::string::npos))
        {
            // arbiter::resolve() globs for * or ** only if they are at the end of
            // a path. To resolve these , we need to split the string
            // so it ends in *, then filter the results later.
            std::string postfix;
            const char last = item.back();
            if (last != '*')
            {
                auto pos = item.find_last_of('*');
                if (pos != std::string::npos)
                {
                    postfix = item.substr(pos + 1);
                    item = item.substr(0, pos);
                }
                if (last != '/') item.push_back('/');
                item.push_back('*');
			}
            const StringList directory(a.resolve(item));
            for (const auto& item : directory)
            {
                if (postfix.size() && !pdal::Utils::endsWith(item, postfix)) continue;
                if (!isDirectory(item)) output.push_back(item);
            }
        }
        else output.push_back(arbiter::expandTilde(item));
    }
    return output;
}

} // namespace entwine

/******************************************************************************
* Copyright (c) 2016, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <string>
#include <vector>

namespace arbiter
{
    class Endpoint;
}

namespace entwine
{

class Storage
{
public:
    // Functions from this class perform exit(1) if they cannot perform the
    // requested operation, as this indicates a fatal error with the underlying
    // storage mechanism.

    static void ensurePut(
            const arbiter::Endpoint& endpoint,
            const std::string& path,
            const std::vector<char>& data);

    static std::unique_ptr<std::vector<char>> ensureGet(
            const arbiter::Endpoint& endpoint,
            const std::string& path);
};

} // namespace entwine


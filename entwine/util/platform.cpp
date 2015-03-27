/******************************************************************************
* Copyright (c) 2016, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <entwine/util/platform.hpp>

#include <unistd.h>

namespace entwine
{
namespace platform
{

std::size_t pageSize()
{
#if defined _SC_PAGE_SIZE
    return sysconf(_SC_PAGE_SIZE);
#elif defined _SC_PAGESIZE
    return sysconf(_SC_PAGESIZE);
#endif
}

} // namespace platform
} // namespace entwine


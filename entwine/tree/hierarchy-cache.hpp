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

#include <cstddef>

class HierarchyCache
{
public:
    HierarchyCache() : m_nodes() { }

    void awaken(const Id& id, const BBox& bbox)
    {
        std::lock_guard<std::mutex> lock(m_mutex);
    }

private:
    struct Entry
    {
        Node* node;
        std::unordered_set<std::size_t> refs;

        std::mutex mutex;
        std::condition_variable cv;
        bool outstanding;
    };

    std::map<Id, Entry> m_nodes;
    mutable std::mutex m_mutex;
};


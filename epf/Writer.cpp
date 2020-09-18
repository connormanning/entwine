#include <vector>

#include <pdal/util/FileUtils.hpp>

#include "Writer.hpp"
#include "Epf.hpp"

using namespace pdal;

namespace epf
{

struct WriteData
{
    int index;
    DataVecPtr data;
};

Writer::Writer(const std::string& directory, int numThreads) :
    m_directory(directory), m_pool(numThreads), m_stop(false)
{
    if (FileUtils::fileExists(directory))
    {
        if (!FileUtils::isDirectory(directory))
            throw Error("Specified output directory '" + directory + "' is not a directory.");
    }
    else
        FileUtils::createDirectory(directory);

    std::function<void()> f = std::bind(&Writer::run, this);
    while (numThreads--)
        m_pool.add(f);
}

std::string Writer::path(int index)
{
    return m_directory + "/" + indexToString(index);
}

void Writer::enqueue(int index, DataVecPtr data)
{
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        m_queue.push_back({index, std::move(data)});
    }
    m_available.notify_one();
}

void Writer::stop()
{
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        m_stop = true;
    }
    m_available.notify_all();
    m_pool.join();
}

void Writer::run()
{
    while (true)
    {
        WriteData wd;

        while (true)
        {
            std::unique_lock<std::mutex> lock(m_mutex);
            if (m_queue.empty() && m_stop)
                return;

            auto li = m_queue.begin();
            for (; li != m_queue.end(); ++li)
                if (std::find(m_active.begin(), m_active.end(), li->index) == m_active.end())
                    break;

            // If there is no data to process, wait.
            if (li == m_queue.end())
                m_available.wait(lock);
            else
            {
                m_active.push_back(li->index);
                wd = std::move(*li);
                m_queue.erase(li);
                break;
            }
        }

        std::ofstream out(path(wd.index), std::ios::app | std::ios::binary);
        out.write(reinterpret_cast<const char *>(wd.data->data()), wd.data->size());
        m_bufferCache.replace(std::move(wd.data));

        std::lock_guard<std::mutex> lock(m_mutex);
        m_active.remove(wd.index); 
    }
}

} // namespace epf

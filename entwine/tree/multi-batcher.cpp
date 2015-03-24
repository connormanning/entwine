/******************************************************************************
* Copyright (c) 2016, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <entwine/tree/multi-batcher.hpp>

#include <thread>

#include <pdal/Filter.hpp>
#include <pdal/PipelineManager.hpp>
#include <pdal/PointView.hpp>
#include <pdal/Reader.hpp>
#include <pdal/StageFactory.hpp>
#include <pdal/StageWrapper.hpp>

#include <entwine/third/json/json.h>
#include <entwine/tree/sleepy-tree.hpp>
#include <entwine/types/schema.hpp>
#include <entwine/types/simple-point-table.hpp>

namespace
{
    const std::size_t httpAttempts(3);
}

namespace entwine
{

MultiBatcher::MultiBatcher(
        const S3Info& s3Info,
        SleepyTree& sleepyTree,
        const std::size_t numThreads,
        const std::size_t snapshot)
    : m_s3(
            s3Info.awsAccessKeyId,
            s3Info.awsSecretAccessKey,
            s3Info.baseAwsUrl,
            s3Info.bucketName)
    , m_threads(numThreads)
    , m_available(numThreads)
    , m_originList()
    , m_sleepyTree(sleepyTree)
    , m_snapshot(snapshot)
    , m_allowAdd(true)
    , m_mutex()
    , m_cv()
{
    for (std::size_t i(0); i < m_available.size(); ++i)
    {
        m_available[i] = i;
    }
}

MultiBatcher::~MultiBatcher()
{
    gather();
}

void MultiBatcher::add(const std::string& filename)
{
    std::unique_lock<std::mutex> lock(m_mutex);
    m_cv.wait(lock, [this]()->bool {
        return m_available.size() && m_allowAdd;
    });
    const std::size_t index(m_available.back());
    m_available.pop_back();

    Origin origin(m_originList.size());
    m_originList.push_back(filename);

    std::cout << "Adding " << filename << std::endl;
    lock.unlock();

    m_threads[index] = std::thread([this, index, &filename, origin]() {
        try
        {
            std::shared_ptr<SimplePointTable> pointTable(
                    new SimplePointTable(m_sleepyTree.schema()));
            SimplePointTable& pointTableRef(*pointTable.get());

            std::unique_ptr<pdal::StageFactory> stageFactory(
                    new pdal::StageFactory());

            std::unique_ptr<pdal::Options> readerOptions(new pdal::Options());
            std::unique_ptr<pdal::Options> reprojOptions(new pdal::Options());

            const std::string driver(stageFactory->inferReaderDriver(filename));
            if (driver.size())
            {
                // Fetch remote file and write locally.
                const std::string localPath("./tmp/" + std::to_string(origin));
                writeFile(localPath, filename);

                // Set up the file reader.
                std::unique_ptr<pdal::Reader> reader(
                        static_cast<pdal::Reader*>(
                            stageFactory->createStage(driver)));

                reader->setSpatialReference(
                        pdal::SpatialReference("EPSG:26915"));

                readerOptions->add(pdal::Option("filename", localPath));
                reader->setOptions(*readerOptions.get());

                // Set up the reprojection filter.
                std::shared_ptr<pdal::Filter> reproj(
                        static_cast<pdal::Filter*>(
                            stageFactory->createStage("filters.reprojection")));

                reprojOptions->add(
                        pdal::Option(
                            "in_srs",
                            pdal::SpatialReference("EPSG:26915")));
                reprojOptions->add(
                        pdal::Option(
                            "out_srs",
                            pdal::SpatialReference("EPSG:3857")));

                pdal::Filter& reprojRef(*reproj.get());

                pdal::FilterWrapper::initialize(reproj, pointTableRef);
                pdal::FilterWrapper::processOptions(
                        reprojRef,
                        *reprojOptions.get());
                pdal::FilterWrapper::ready(reprojRef, pointTableRef);

                // Set up our per-point data handler.
                reader->setReadCb(
                        [this, &pointTableRef, &reprojRef, origin]
                        (pdal::PointView& view, pdal::PointId index)
                {
                    pdal::PointViewPtr point(view.makeNew());
                    point->appendPoint(view, index);

                    pdal::FilterWrapper::filter(reprojRef, point);

                    m_sleepyTree.insert(*point.get(), origin);
                    pointTableRef.clear();
                });

                reader->prepare(pointTableRef);
                reader->execute(pointTableRef);

                if (remove(localPath.c_str()) != 0)
                {
                    std::cout << "Couldn't delete " << localPath << std::endl;
                    throw std::runtime_error("Couldn't delete tmp file");
                }
            }
            else
            {
                std::cout << "No driver found - " << filename << std::endl;
            }
        }
        catch (std::runtime_error& e)
        {
            std::cout << "Caught: " << e.what() << std::endl;
        }
        catch (...)
        {
            std::cout << "Exception in multi-batcher " << filename << std::endl;
        }

        std::unique_lock<std::mutex> lock(m_mutex);
        m_available.push_back(index);
        m_threads[index].detach();
        lock.unlock();
        m_cv.notify_all();
    });

    if (m_snapshot && (origin + 1) % m_snapshot == 0)
    {
        takeSnapshot();
    }
}

void MultiBatcher::gather()
{
    std::unique_lock<std::mutex> lock(m_mutex);
    m_cv.wait(lock, [this]()->bool {
        return m_available.size() == m_threads.size();
    });
}

void MultiBatcher::takeSnapshot()
{
    m_mutex.lock();
    m_allowAdd = false;
    m_mutex.unlock();

    try
    {
        std::cout << "Writing snapshot at " << m_originList.size() << std::endl;
        gather();

        std::ofstream dataStream;
        dataStream.open(
                m_sleepyTree.path() + "/manifest",
                std::ofstream::out | std::ofstream::trunc);
        Json::Value jsonManifest;
        jsonManifest["manifest"].resize(m_originList.size());
        for (std::size_t i(0); i < m_originList.size(); ++i)
        {
            jsonManifest["manifest"][static_cast<Json::ArrayIndex>(i)] =
                m_originList[i];
        }
        const std::string manifestString(jsonManifest.toStyledString());
        dataStream.write(manifestString.data(), manifestString.size());
        dataStream.close();

        m_sleepyTree.save();
    }
    catch (...)
    {
        std::cout << "Caught exception during snapshot" << std::endl;
    }

    m_mutex.lock();
    m_allowAdd = true;
    m_mutex.unlock();
    m_cv.notify_all();
}

void MultiBatcher::writeFile(
        const std::string& localPath,
        const std::string& remoteName)
{
    std::size_t tries(0);

    HttpResponse res;

    do
    {
        res = m_s3.get(remoteName);
    } while (res.code() != 200 && ++tries < httpAttempts);

    if (res.code() != 200)
    {
        std::cout << "Couldn't fetch " + remoteName <<
                " - Got: " << res.code() << std::endl;
        throw std::runtime_error("Couldn't fetch " + remoteName);
    }

    std::shared_ptr<std::vector<char>> fileData(res.data());
    std::ofstream writer(
            localPath,
            std::ofstream::binary |
                std::ofstream::out |
                std::ofstream::trunc);

    writer.write(fileData->data(), fileData->size());
    writer.close();
}

} // namespace entwine


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
#include <entwine/tree/branches/clipper.hpp>
#include <entwine/tree/sleepy-tree.hpp>
#include <entwine/types/schema.hpp>
#include <entwine/types/linking-point-view.hpp>
#include <entwine/types/simple-point-table.hpp>
#include <entwine/util/fs.hpp>

namespace
{
    const std::size_t httpAttempts(3);
    const std::size_t pointBatchSize(4096);

    // Insert points into the tree, and clear the PointTable.
    void insertPoints(
            entwine::SleepyTree& tree,
            pdal::Filter& filter,
            entwine::SimplePointTable& table,
            const entwine::Origin origin,
            entwine::Clipper* clipper)
    {
        if (table.size())
        {
            std::shared_ptr<entwine::LinkingPointView> link(
                    new entwine::LinkingPointView(table));
            pdal::FilterWrapper::filter(filter, link);

            tree.insert(*link.get(), origin, clipper);
            table.clear();
        }
    }
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

    std::cout << "Adding " << filename << std::endl;
    lock.unlock();

    Origin origin(m_sleepyTree.addOrigin(filename));

    std::unique_ptr<pdal::StageFactory> stageFactoryPtr(
            new pdal::StageFactory());

    pdal::StageFactory& stageFactory(*stageFactoryPtr.get());

    m_threads[index] = std::thread(
        [this, index, origin, filename, &stageFactory]() {
        try
        {
            std::shared_ptr<SimplePointTable> pointTablePtr(
                    new SimplePointTable(m_sleepyTree.schema()));
            SimplePointTable& pointTable(*pointTablePtr.get());

            std::unique_ptr<pdal::Options> readerOptions(new pdal::Options());
            std::unique_ptr<pdal::Options> reprojOptions(new pdal::Options());

            const std::string driver(stageFactory.inferReaderDriver(filename));
            if (driver.size())
            {
                // Fetch remote file and write locally.
                const std::string localPath(
                    "./tmp/" + m_sleepyTree.name() + "-" +
                            std::to_string(origin));

                {
                    const HttpResponse res(fetchFile(filename));

                    if (res.code() != 200)
                    {
                        std::cout << "Couldn't fetch " + filename <<
                                " - Got: " << res.code() << std::endl;
                        throw std::runtime_error("Couldn't fetch " + filename);
                    }

                    if (!fs::writeFile(
                                localPath,
                                *res.data().get(),
                                fs::binaryTruncMode))
                    {
                        throw std::runtime_error("Couldn't write " + localPath);
                    }
                }

                // Set up the file reader.
                std::unique_ptr<pdal::Reader> reader(
                        static_cast<pdal::Reader*>(
                            stageFactory.createStage(driver)));

                reader->setSpatialReference(
                        pdal::SpatialReference("EPSG:26915"));

                readerOptions->add(pdal::Option("filename", localPath));
                reader->setOptions(*readerOptions.get());

                // Set up the reprojection filter.
                std::shared_ptr<pdal::Filter> reproj(
                        static_cast<pdal::Filter*>(
                            stageFactory.createStage("filters.reprojection")));

                reprojOptions->add(
                        pdal::Option(
                            "in_srs",
                            pdal::SpatialReference("EPSG:26915")));
                reprojOptions->add(
                        pdal::Option(
                            "out_srs",
                            pdal::SpatialReference("EPSG:3857")));

                pdal::Filter& reprojRef(*reproj.get());

                pdal::FilterWrapper::initialize(reproj, pointTable);
                pdal::FilterWrapper::processOptions(
                        reprojRef,
                        *reprojOptions.get());
                pdal::FilterWrapper::ready(reprojRef, pointTable);

                std::unique_ptr<Clipper> clipper(new Clipper(m_sleepyTree));
                Clipper* clipperPtr(clipper.get());

                // Set up our per-point data handler.
                reader->setReadCb(
                        [this, &pointTable, &reprojRef, origin, clipperPtr]
                        (pdal::PointView& view, pdal::PointId index)
                {
                    if (pointTable.size() >= pointBatchSize)
                    {
                        insertPoints(
                                m_sleepyTree,
                                reprojRef,
                                pointTable,
                                origin,
                                clipperPtr);
                    }
                });

                reader->prepare(pointTable);
                reader->execute(pointTable);

                // Insert the leftover points that didn't reach the batch size.
                insertPoints(
                        m_sleepyTree,
                        reprojRef,
                        pointTable,
                        origin,
                        clipperPtr);

                std::cout << "\tDone " << filename << std::endl;
                if (!fs::removeFile(localPath))
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

        Json::Value jsonManifest;
        jsonManifest["manifest"].resize(m_originList.size());
        for (std::size_t i(0); i < m_originList.size(); ++i)
        {
            jsonManifest["manifest"][static_cast<Json::ArrayIndex>(i)] =
                m_originList[i];
        }
        const std::string manifestString(jsonManifest.toStyledString());

        if (!fs::writeFile(
                m_sleepyTree.path() + "/manifest",
                manifestString,
                std::ofstream::out | std::ofstream::trunc))
        {
            throw std::runtime_error("Could not write manifest");
        }

        m_sleepyTree.save();
    }
    catch (std::runtime_error& e)
    {
        std::cout << "Caught: " << e.what() << " during snapshot" << std::endl;
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

HttpResponse MultiBatcher::fetchFile(const std::string& remoteName)
{
    std::size_t tries(0);

    HttpResponse res;

    do
    {
        res = m_s3.get(remoteName);
    } while (res.code() != 200 && ++tries < httpAttempts);

    return res;
}

} // namespace entwine


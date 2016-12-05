#include "octree.hpp"

#include <pdal/Reader.hpp>

#include <entwine/util/unique.hpp>

using namespace entwine;

namespace test
{

namespace
{
    std::size_t md(0);
}

Traversal::Traversal(
        const Octree& octree,
        const std::size_t origin,
        pdal::PointView& view,
        const std::size_t index,
        const Point point)
    : m_octree(octree)
    , m_origin(origin)
    , m_view(view)
    , m_index(index)
    , m_point(point)
    , m_bounds(octree.bounds())
{ }

std::size_t Traversal::depthBegin() const { return m_octree.depthBegin(); }
std::size_t Traversal::depthEnd() const { return m_octree.depthEnd(); }

bool Node::insert(Slot slot)
{
    auto next([&]()
    {
        // Re-grab this here, it may have been swapped.
        Traversal& t(slot->front());

        const auto dir(t.dir());
        for (auto& t : *slot) t.next(dir);

        auto& child(m_children[dir]);
        if (!child) child = makeUnique<Node>();
        return child->insert(std::move(slot));
    });

    Traversal& t(slot->front());
    if (t.depthEnd() && t.depth() > t.depthEnd()) return false;

    if (t.depth() >= t.depthBegin())
    {
        if (empty())
        {
            m_slot = std::move(slot);
            md = std::max(t.depth(), md);
            return true;
        }
        else if (t.point() == point())
        {
            std::cout << "EQ!" << std::endl;
            for (auto& t : *slot)
            {
                m_slot->push_back(std::move(t));
            }
            md = std::max(t.depth(), md);
            return true;
        }
        else
        {
            const auto a(t.point().sqDist3d(t.bounds().mid()));
            const auto b(point().sqDist3d(t.bounds().mid()));

            if (a < b || (a == b && ltChained(t.point(), point())))
            {
                std::swap(m_slot, slot);
            }

            return next();
        }
    }
    else return next();
}

bool Node::insert(Traversal& t)
{
    Slot slot(makeUnique<Traversals>());
    slot->push_back(t);
    return insert(std::move(slot));
}

void Octree::insert(const std::string path)
{
    const std::size_t origin(m_data.size());
    m_data.push_back(Data());

    const auto driver(m_stageFactory.inferReaderDriver(path));
    if (driver.empty()) return;

    auto reader(static_cast<pdal::Reader*>(m_stageFactory.createStage(driver)));
    if (!reader) return;

    pdal::Options options;
    options.add(pdal::Option("filename", path));
    reader->setOptions(options);

    pdal::PointTable table;
    reader->prepare(table);
    auto views(reader->execute(table));

    if (views.size() != 1) throw std::runtime_error("Invalid number of views");
    auto view(*views.begin());
    m_data.back().view = view;

    Point p;
    pdal::PointRef pointRef(*view, 0);

    for (std::size_t i(0); i < view->size(); ++i)
    {
        pointRef.setPointId(i);
        p.x = pointRef.getFieldAs<double>(pdal::Dimension::Id::X);
        p.y = pointRef.getFieldAs<double>(pdal::Dimension::Id::Y);
        p.z = pointRef.getFieldAs<double>(pdal::Dimension::Id::Z);

        if (m_delta)
        {
            p = Point::scale(p, m_delta->scale(), m_delta->offset());
            pointRef.setField(pdal::Dimension::Id::X, p.x);
            pointRef.setField(pdal::Dimension::Id::Y, p.y);
            pointRef.setField(pdal::Dimension::Id::Z, p.z);
        }

        // std::cout << i << ": " << p << std::endl;

        if (!m_bounds.contains(p))
        {
            std::cout << " B " << m_bounds << " P " << p << std::endl;
            throw std::runtime_error("Need to handle out of bounds points");
        }

        Traversal t(*this, origin, *view, i, p);
        if (m_root.insert(t)) ++m_data.back().inserts;
        else ++m_data.back().outOfBounds;
    }
}

} // namespace test


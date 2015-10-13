#pragma once

#include <algorithm>
#include <cassert>
#include <chrono>
#include <deque>
#include <functional>
#include <iostream>
#include <mutex>
#include <thread>
#include <type_traits>
#include <vector>

namespace splicer
{

template<typename T> class Stack;
template<typename T> class SplicePool;
template<typename T> class UniqueStack;

template<typename T>
class Node
{
    friend class Stack<T>;

public:
    explicit Node(Node* next = nullptr) : m_val(), m_next(next) { }

    template<class... Args>
    void construct(Args&&... args)
    {
        new (&m_val) T(std::forward<Args>(args)...);
    }

    T& operator*() { return m_val; }
    const T& operator*() const { return m_val; }
    T& val() { return m_val; }
    const T& val() const { return m_val; }

    Node* next() { return m_next; }

private:
    void setNext(Node* node) { m_next = node; }

    T m_val;
    Node* m_next;
};

template<typename T>
class Stack
{
    friend class UniqueStack<T>;

public:
    Stack() : m_tail(nullptr), m_head(nullptr), m_size(0) { }

    void push(Node<T>* node)
    {
        assert(!m_tail || m_size);

        node->setNext(m_head);
        m_head = node;

        if (!m_size) m_tail = node;
        ++m_size;
    }

    void push(Stack& other)
    {
        if (!other.empty())
        {
            push(other.m_tail);
            m_head = other.head();
            m_size += other.size() - 1; // Tail has already been accounted for.
            other.clear();
        }
    }

    Node<T>* pop()
    {
        Node<T>* node(m_head);
        if (m_head)
        {
            m_head = m_head->next();
            if (!--m_size) m_tail = nullptr;
        }
        return node;
    }

    Stack popStack(std::size_t count)
    {
        Stack other;

        if (count >= size())
        {
            swap(other);
        }
        else if (count && !empty())
        {
            Node<T>* tail(m_head);
            for (std::size_t i(0); i < count - 1; ++i) tail = tail->next();

            other.m_head = m_head;
            m_head = tail->next();

            tail->setNext(nullptr);
            other.m_tail = tail;

            other.m_size = count;
            m_size -= count;
        }

        return other;
    }

    bool empty() const { return !m_head; }
    std::size_t size() const { return m_size; }

    void print(std::size_t maxElements) const
    {
        if (Node<T>* current = m_head)
        {
            std::size_t i(0);

            while (current && i++ < maxElements)
            {
                std::cout << current->val() << " ";
                current = current->next();
            }

            if (current) std::cout << "and more...";

            std::cout << std::endl;
        }
        else
        {
            std::cout << "(empty)" << std::endl;
        }
    }

    void swap(Stack& other)
    {
        std::swap(*this, other);
    }

    Node<T>* head() { return m_head; }

protected:
    void clear()
    {
        m_head = nullptr;
        m_tail = nullptr;
        m_size = 0;
    }

private:
    Node<T>* m_tail;
    Node<T>* m_head;
    std::size_t m_size;
};

template<typename T>
class UniqueStack
{
public:
    UniqueStack(SplicePool<T>& splicePool)
        : m_splicePool(splicePool)
        , m_nodeDelete(&splicePool)
        , m_stack()
    { }

    UniqueStack(SplicePool<T>& splicePool, Stack<T>&& stack)
        : m_splicePool(splicePool)
        , m_nodeDelete(&splicePool)
        , m_stack(stack)
    {
        stack.clear();
    }

    UniqueStack(UniqueStack&& other)
        : m_splicePool(other.m_splicePool)
        , m_nodeDelete(other.m_nodeDelete)
        , m_stack(other.release())
    { }

    UniqueStack& operator=(UniqueStack&& other)
    {
        reset(other.release());
        return *this;
    }

    ~UniqueStack() { m_splicePool.release(std::move(m_stack)); }

    Stack<T> release()
    {
        Stack<T> other(m_stack);
        m_stack.clear();
        return other;
    }

    void reset(Stack<T>&& other)
    {
        m_splicePool.release(std::move(m_stack));
        m_stack = other;
        other.clear();
    }

    void reset()
    {
        m_splicePool.release(std::move(m_stack));
    }

    void push(Node<T>* node) { m_stack.push(node); }
    void push(Stack<T>& other) { m_stack.push(other); }

    void push(typename SplicePool<T>::UniqueNodeType&& node)
    {
        Node<T>* pushing(node.release());
        m_stack.push(pushing);
    }

    void push(UniqueStack&& other)
    {
        Stack<T> pushing(other.release());
        m_stack.push(pushing);
    }

    typename SplicePool<T>::UniqueNodeType popOne()
    {
        return typename SplicePool<T>::UniqueNodeType(
                m_stack.pop(),
                m_nodeDelete);
    }

    UniqueStack pop(std::size_t count)
    {
        Stack<T> stack(m_stack.popStack(count));
        return UniqueStack(m_splicePool, std::move(stack));
    }

    bool empty() const { return m_stack.empty(); }
    std::size_t size() const { return m_stack.size(); }
    void print(std::size_t maxElements) const { m_stack.print(maxElements); }
    void swap(UniqueStack&& other) { m_stack.swap(other.m_stack); }
    Node<T>* head() { return m_stack.head(); }

private:
    UniqueStack(const UniqueStack&) = delete;
    UniqueStack& operator=(UniqueStack&) = delete;

    SplicePool<T>& m_splicePool;
    const typename SplicePool<T>::NodeDelete m_nodeDelete;
    Stack<T> m_stack;
};

template<typename T>
class SplicePool
{
public:
    typedef Node<T> NodeType;

    struct NodeDelete
    {
        explicit NodeDelete(SplicePool* splicePool = nullptr)
            : m_splicePool(splicePool)
        { }

        void operator()(NodeType* node)
        {
            if (m_splicePool) m_splicePool->release(node);
        }

    private:
        SplicePool* m_splicePool;
    };

    typedef std::unique_ptr<NodeType, NodeDelete> UniqueNodeType;

    typedef Stack<T> StackType;
    typedef UniqueStack<T> UniqueStackType;

    SplicePool(std::size_t blockSize)
        : m_blockSize(blockSize)
        , m_stack()
        , m_mutex()
        , m_allocated(0)
        , m_nodeDelete(this)
    { }

    virtual ~SplicePool() { }

    std::size_t allocated() const
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        return m_allocated;
    }

    std::size_t available() const
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        return m_stack.size();
    }

    void release(UniqueNodeType&& node) { node.reset(); }
    void release(UniqueStackType&& stack) { stack.reset(); }

    void release(Node<T>* node)
    {
        if (node)
        {
            reset(&node->val());

            // TODO - For these single node releases, we could put them into a
            // separate Stack to avoid blocking the entire pool, and only reach
            // into it when some threshold is reached or the main stack is
            // empty.
            std::lock_guard<std::mutex> lock(m_mutex);
            m_stack.push(node);
        }
    }

    void release(Stack<T>&& other)
    {
        if (Node<T>* node = other.head())
        {
            while (node)
            {
                reset(&node->val());
                node = node->next();
            }

            std::lock_guard<std::mutex> lock(m_mutex);
            m_stack.push(other);
        }
    }

    template<class... Args>
    UniqueNodeType acquireOne(Args&&... args)
    {
        UniqueNodeType node(nullptr, m_nodeDelete);

        {
            std::lock_guard<std::mutex> lock(m_mutex);
            node.reset(m_stack.pop());
        }

        if (!node)
        {
            Stack<T> newStack(doAllocate(1));
            node.reset(newStack.pop());

            std::lock_guard<std::mutex> lock(m_mutex);

            m_allocated += m_blockSize;
            m_stack.push(newStack);
        }

        if (!std::is_pointer<T>::value)
        {
            node->construct(std::forward<Args>(args)...);
        }

        return node;
    }

    UniqueStackType acquire(const std::size_t count)
    {
        UniqueStackType other(*this);

        std::unique_lock<std::mutex> lock(m_mutex);
        if (count >= m_stack.size())
        {
            other = UniqueStackType(*this, std::move(m_stack));

            lock.unlock();

            if (count > other.size())
            {
                const std::size_t numNodes(count - other.size());
                const std::size_t numBlocks(numNodes / m_blockSize + 1);

                Stack<T> alloc(doAllocate(numBlocks));

                assert(alloc.size() == numBlocks * m_blockSize);

                Stack<T> taken(alloc.popStack(numNodes));
                other.push(taken);

                lock.lock();
                m_stack.push(alloc);
                m_allocated += numBlocks * m_blockSize;
            }
        }
        else
        {
            other = UniqueStackType(*this, m_stack.popStack(count));
        }

        return other;
    }

protected:
    void reset(T* val)
    {
        destruct(val);
        construct(val);
    }

    virtual Stack<T> doAllocate(std::size_t blocks) = 0;
    virtual void construct(T*) const { }
    virtual void destruct(T*) const { }

    const std::size_t m_blockSize;

private:
    SplicePool(const SplicePool&) = delete;
    SplicePool& operator=(const SplicePool&) = delete;

    Stack<T> m_stack;
    mutable std::mutex m_mutex;

    std::size_t m_allocated;
    const NodeDelete m_nodeDelete;
};

template<typename T>
class ObjectPool : public SplicePool<T>
{
public:
    ObjectPool(std::size_t blockSize = 4096)
        : SplicePool<T>(blockSize)
        , m_blocks()
        , m_mutex()
    { }

private:
    virtual Stack<T> doAllocate(std::size_t blocks) override
    {
        Stack<T> stack;
        std::deque<std::unique_ptr<std::vector<Node<T>>>> newBlocks;

        for (std::size_t i(0); i < blocks; ++i)
        {
            std::unique_ptr<std::vector<Node<T>>> newBlock(
                    new std::vector<Node<T>>(this->m_blockSize));
            newBlocks.push_back(std::move(newBlock));
        }

        for (auto& block : newBlocks)
        {
            auto& vec(*block);

            for (std::size_t i(0); i < vec.size(); ++i)
            {
                stack.push(&vec[i]);
            }
        }

        std::lock_guard<std::mutex> lock(m_mutex);

        m_blocks.insert(
                m_blocks.end(),
                std::make_move_iterator(newBlocks.begin()),
                std::make_move_iterator(newBlocks.end()));

        return stack;
    }

    virtual void construct(T* val) const override
    {
        new (val) T();
    }

    virtual void destruct(T* val) const override
    {
        val->~T();
    }

    std::deque<std::unique_ptr<std::vector<Node<T>>>> m_blocks;
    mutable std::mutex m_mutex;
};

template<typename T>
class BufferPool : public SplicePool<T*>
{
public:
    BufferPool(std::size_t bufferSize, std::size_t blockSize = 4096)
        : SplicePool<T*>(blockSize)
        , m_bufferSize(bufferSize)
        , m_bytesPerBlock(m_bufferSize * this->m_blockSize)
        , m_bytes()
        , m_nodes()
        , m_mutex()
    { }

private:
    virtual Stack<T*> doAllocate(std::size_t blocks) override
    {
        Stack<T*> stack;

        std::deque<std::unique_ptr<std::vector<T>>> newBytes;
        std::deque<std::unique_ptr<std::vector<Node<T*>>>> newNodes;

        for (std::size_t i(0); i < blocks; ++i)
        {
            std::unique_ptr<std::vector<T>> newByteBlock(
                    new std::vector<T>(m_bytesPerBlock));

            std::unique_ptr<std::vector<Node<T*>>> newNodeBlock(
                    new std::vector<Node<T*>>(this->m_blockSize));

            newBytes.push_back(std::move(newByteBlock));
            newNodes.push_back(std::move(newNodeBlock));
        }

        for (std::size_t i(0); i < blocks; ++i)
        {
            std::vector<T>& bytes(*newBytes[i]);
            std::vector<Node<T*>>& nodes(*newNodes[i]);

            for (std::size_t i(0); i < this->m_blockSize; ++i)
            {
                Node<T*>& node(nodes[i]);
                node.val() = &bytes[m_bufferSize * i];
                stack.push(&node);
            }
        }

        std::lock_guard<std::mutex> lock(m_mutex);

        m_bytes.insert(
                m_bytes.end(),
                std::make_move_iterator(newBytes.begin()),
                std::make_move_iterator(newBytes.end()));

        m_nodes.insert(
                m_nodes.end(),
                std::make_move_iterator(newNodes.begin()),
                std::make_move_iterator(newNodes.end()));

        return stack;
    }

    virtual void construct(T** val) const override
    {
        std::fill(*val, *val + m_bufferSize, 0);
    }

    const std::size_t m_bufferSize;
    const std::size_t m_bytesPerBlock;

    std::deque<std::unique_ptr<std::vector<T>>> m_bytes;
    std::deque<std::unique_ptr<std::vector<Node<T*>>>> m_nodes;
    mutable std::mutex m_mutex;
};

} // namespace splicer


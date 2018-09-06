/******************************************************************************
    Copyright (c) 2018 Connor Manning

    Permission is hereby granted, free of charge, to any person obtaining a
    copy of this software and associated documentation files (the "Software"),
    to deal in the Software without restriction, including without limitation
    the rights to use, copy, modify, merge, publish, distribute, sublicense,
    and/or sell copies of the Software, and to permit persons to whom the
    Software is furnished to do so, subject to the following conditions:

    The above copyright notice and this permission notice shall be included in
    all copies or substantial portions of the Software.

    THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
    IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
    FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
    AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
    LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
    FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
    DEALINGS IN THE SOFTWARE.
******************************************************************************/
#pragma once

#include <algorithm>
#include <atomic>
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
    template<class... Args>
    void construct(Args&&... args)
    {
        new (&m_val) T(std::forward<Args>(args)...);
    }

    T& operator=(const T& val)
    {
        m_val = val;
        return m_val;
    }

    T& operator*() { return m_val; }
    const T& operator*() const { return m_val; }

    T& get() { return m_val; }
    const T& get() const { return m_val; }

    T* operator->() { return &m_val; }
    const T* operator->() const { return &m_val; }

    Node* prev() { return m_prev; }
    const Node* prev() const { return m_prev; }

    Node* next() { return m_next; }
    const Node* next() const { return m_next; }

private:
    void setPrev(Node* node) { m_prev = node; }
    void setNext(Node* node) { m_next = node; }

    T m_val;
    Node* m_prev = nullptr;
    Node* m_next = nullptr;
};

template<typename T>
class Stack
{
    friend class UniqueStack<T>;

public:
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
        if (other.empty()) return;

        push(other.m_tail);
        m_head = other.head();
        m_size += other.size() - 1; // Tail has already been accounted for.
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

    void print(std::size_t maxElements = 20) const
    {
        if (Node<T>* current = m_head)
        {
            std::size_t i(0);

            while (current && i++ < maxElements)
            {
                std::cout << current->get() << " ";
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
    const Node<T>* head() const { return m_head; }

    class Iterator;

    class ConstIterator
    {
        friend class Iterator;

    public:
        explicit ConstIterator(const Node<T>* node) : m_node(node) { }

        ConstIterator& operator++()
        {
            m_node = m_node->next();
            return *this;
        }

        ConstIterator operator++(int)
        {
            Iterator it(m_node);
            m_node = m_node->next();
            return it;
        }

        const T& operator*() const { return **m_node; }

        bool operator!=(const ConstIterator& other) const
        {
            return m_node != other.m_node;
        }

        bool operator!=(const Iterator& other) const
        {
            return m_node != other.m_node;
        }

    private:
        const Node<T>* m_node;
    };

    class Iterator
    {
        friend class ConstIterator;

    public:
        explicit Iterator(Node<T>* node) : m_node(node) { }

        Iterator& operator++()
        {
            m_node = m_node->next();
            return *this;
        }

        Iterator operator++(int)
        {
            Iterator it(m_node);
            m_node = m_node->next();
            return it;
        }

        T& operator*() { return **m_node; }
        const T& operator*() const { return **m_node; }

        bool operator!=(const ConstIterator& other) const
        {
            return m_node != other.m_node;
        }

        bool operator!=(const Iterator& other) const
        {
            return m_node != other.m_node;
        }

    private:
        Node<T>* m_node;
    };

    Iterator begin() { return Iterator(head()); }
    ConstIterator begin() const { return ConstIterator(head()); }
    ConstIterator cbegin() const { return begin(); }

    Iterator end() { return Iterator(nullptr); }
    ConstIterator end() const { return ConstIterator(nullptr); }
    ConstIterator cend() const { return end(); }

    void clear()
    {
        m_head = nullptr;
        m_tail = nullptr;
        m_size = 0;
    }

private:
    Node<T>* tail() { return m_tail; }
    const Node<T>* tail() const { return m_tail; }

    Node<T>* m_tail = nullptr;
    Node<T>* m_head = nullptr;
    std::size_t m_size = 0;
};

template<typename T>
class UniqueNode
{
public:
    explicit UniqueNode(SplicePool<T>& splicePool) noexcept
        : m_splicePool(splicePool)
        , m_node(nullptr)
    { }

    UniqueNode(SplicePool<T>& splicePool, Node<T>* node) noexcept
        : m_splicePool(splicePool)
        , m_node(node)
    { }

    UniqueNode(UniqueNode&& other)
        : m_splicePool(other.m_splicePool)
        , m_node(other.release())
    { }

    UniqueNode& operator=(UniqueNode&& other)
    {
        m_node = other.release();
        return *this;
    }

    UniqueNode(const UniqueNode&) = delete;
    UniqueNode& operator=(const UniqueNode&) = delete;

    ~UniqueNode() { reset(); }

    bool empty() const { return m_node == nullptr; }

    Node<T>* release()
    {
        Node<T>* result(m_node);
        m_node = nullptr;
        return result;
    }

    void reset(Node<T>* node = nullptr)
    {
        if (m_node) m_splicePool.release(m_node);
        m_node = node;
    }

    void swap(UniqueNode& other)
    {
        Node<T>* copy(m_node);
        m_node = other.m_node;
        other.m_node = copy;
    }

    Node<T>* get() const { return m_node; }
    explicit operator bool() const { return m_node != nullptr; }

    T& operator*() { return **m_node; }
    const T& operator*() const { return **m_node; }

    T* operator->() { return &m_node->get(); }
    const T* operator->() const { return &m_node->get(); }

    SplicePool<T>& pool() { return m_splicePool; }
    const SplicePool<T>& pool() const { return m_splicePool; }

private:
    SplicePool<T>& m_splicePool;
    Node<T>* m_node;
};

template<typename T>
class UniqueStack
{
public:
    using Iterator = typename Stack<T>::Iterator;
    using ConstIterator = typename Stack<T>::ConstIterator;
    using NodeType = typename SplicePool<T>::NodeType;
    using UniqueNodeType = typename SplicePool<T>::UniqueNodeType;

    explicit UniqueStack(SplicePool<T>& splicePool)
        : m_splicePool(splicePool)
        , m_stack()
    { }

    UniqueStack(SplicePool<T>& splicePool, Stack<T>&& stack)
        : m_splicePool(splicePool)
        , m_stack(stack)
    {
        stack.clear();
    }

    explicit UniqueStack(UniqueNodeType&& node)
        : m_splicePool(node.pool())
        , m_stack()
    {
        push(std::move(node));
    }

    UniqueStack(UniqueStack&& other)
        : m_splicePool(other.m_splicePool)
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

    // Push to front.
    void push(Node<T>* node) { m_stack.push(node); }
    void push(Stack<T>& other) { m_stack.push(other); }
    void push(Stack<T>&& other) { m_stack.push(other); other.clear(); }

    void push(UniqueNodeType&& node)
    {
        Node<T>* pushing(node.release());
        m_stack.push(pushing);
    }

    void push(UniqueStack&& other)
    {
        Stack<T> pushing(other.release());
        m_stack.push(pushing);
    }

    // Push to back.
    void pushBack(Node<T>* node) { m_stack.pushBack(node); }
    void pushBack(Stack<T>& other) { m_stack.pushBack(other); }
    void pushBack(Stack<T>&& other) { m_stack.pushBack(other); other.clear(); }

    void pushBack(UniqueNodeType&& node)
    {
        Node<T>* pushing(node.release());
        m_stack.pushBack(pushing);
    }

    void pushBack(UniqueStack&& other)
    {
        Stack<T> pushing(other.release());
        m_stack.pushBack(pushing);
    }

    UniqueNodeType popOne()
    {
        return UniqueNodeType(m_splicePool, m_stack.pop());
    }

    template<class... Args>
    UniqueNodeType popOne(Args&&... args)
    {
        UniqueNodeType node(m_splicePool, m_stack.pop());

        if (!std::is_pointer<T>::value)
        {
            node.get()->construct(std::forward<Args>(args)...);
        }

        return node;
    }

    UniqueStack pop(std::size_t count)
    {
        Stack<T> stack(m_stack.popStack(count));
        return UniqueStack(m_splicePool, std::move(stack));
    }

    bool empty() const { return m_stack.empty(); }
    std::size_t size() const { return m_stack.size(); }
    void swap(UniqueStack&& other) { m_stack.swap(other.m_stack); }

    void print(std::size_t maxElements = 20) const
    {
        m_stack.print(maxElements);
    }

    Node<T>* head() { return m_stack.head(); }
    const Node<T>* head() const { return m_stack.head(); }

    Iterator begin() { return Iterator(head()); }
    ConstIterator begin() const { return ConstIterator(head()); }
    ConstIterator cbegin() const { return begin(); }

    Iterator end() { return Iterator(nullptr); }
    ConstIterator end() const { return ConstIterator(nullptr); }
    ConstIterator cend() const { return end(); }

    SplicePool<T>& pool() { return m_splicePool; }

    Stack<T>& stack() { return m_stack; }
    const Stack<T>& stack() const { return m_stack; }

    Stack<T>& get() { return m_stack; }
    const Stack<T>& get() const { return m_stack; }

private:
    UniqueStack(const UniqueStack&) = delete;
    UniqueStack& operator=(UniqueStack&) = delete;

    SplicePool<T>& m_splicePool;
    Stack<T> m_stack;
};

class SpinLock
{
public:
    SpinLock() = default;

    void lock() { while (m_flag.test_and_set()) ; }
    void unlock() { m_flag.clear(); }

private:
    std::atomic_flag m_flag = ATOMIC_FLAG_INIT;

    SpinLock(const SpinLock& other) = delete;
};

using SpinGuard = std::lock_guard<SpinLock>;
using UniqueSpin = std::unique_lock<SpinLock>;

template<typename T>
class SplicePool
{
public:
    using NodeType = Node<T>;
    using UniqueNodeType = UniqueNode<T>;

    using StackType = Stack<T>;
    using UniqueStackType = UniqueStack<T>;

    SplicePool(std::size_t blockSize) : m_blockSize(blockSize) { }
    virtual ~SplicePool() { }

    void clear()
    {
        assert(!used());

        doClear();
        m_stack.clear();
        m_allocated = 0;
    }

    std::size_t allocated() const
    {
        SpinGuard lock(m_spin);
        return m_allocated;
    }

    std::size_t used() const
    {
        return allocated() - available();
    }

    std::size_t available() const
    {
        SpinGuard lock(m_spin);
        return m_stack.size();
    }

    void release(UniqueNodeType&& node) { node.reset(); }
    void release(UniqueStackType&& stack) { stack.reset(); }

    void release(Node<T>* node)
    {
        if (node)
        {
            reset(&node->get());

            // TODO - For these single node releases, we could put them into a
            // separate Stack to avoid blocking the entire pool, and only reach
            // into it when some threshold is reached or the main stack is
            // empty.
            SpinGuard lock(m_spin);
            m_stack.push(node);
        }
    }

    void release(Stack<T>&& other)
    {
        if (Node<T>* node = other.head())
        {
            while (node)
            {
                reset(&node->get());
                node = node->next();
            }

            SpinGuard lock(m_spin);
            m_stack.push(other);
            other.clear();
        }
    }

    template<class... Args>
    UniqueNodeType acquireOne(Args&&... args)
    {
        UniqueNodeType node(*this);

        {
            SpinGuard lock(m_spin);
            node.reset(m_stack.pop());
        }

        if (!node)
        {
            Stack<T> newStack(doAllocate(1));
            node.reset(newStack.pop());

            SpinGuard lock(m_spin);

            m_allocated += m_blockSize;
            m_stack.push(newStack);
        }

        if (!std::is_pointer<T>::value)
        {
            node.get()->construct(std::forward<Args>(args)...);
        }

        return node;
    }

    UniqueStackType acquire(const std::size_t count)
    {
        UniqueStackType other(*this);

        UniqueSpin lock(m_spin);
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
    virtual void doClear() = 0;
    virtual void construct(T*) const { }
    virtual void destruct(T*) const { }

    const std::size_t m_blockSize;

private:
    SplicePool(const SplicePool&) = delete;
    SplicePool& operator=(const SplicePool&) = delete;

    Stack<T> m_stack;
    mutable SpinLock m_spin;

    std::size_t m_allocated = 0;
};

template<typename T>
class ObjectPool : public SplicePool<T>
{
public:
    ObjectPool(std::size_t blockSize = 4096) : SplicePool<T>(blockSize) { }

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

        SpinGuard lock(m_spin);

        m_blocks.insert(
                m_blocks.end(),
                std::make_move_iterator(newBlocks.begin()),
                std::make_move_iterator(newBlocks.end()));

        return stack;
    }

    virtual void doClear() override
    {
        m_blocks.clear();
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
    mutable SpinLock m_spin;
};

template<typename T>
class BufferPool : public SplicePool<T*>
{
public:
    BufferPool(std::size_t bufferSize, std::size_t blockSize = 4096)
        : SplicePool<T*>(blockSize)
        , m_bufferSize(bufferSize)
        , m_bytesPerBlock(m_bufferSize * this->m_blockSize)
    { }

    std::size_t bufferSize() const { return m_bufferSize; }

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
                node.get() = &bytes[m_bufferSize * i];
                stack.push(&node);
            }
        }

        SpinGuard lock(m_spin);

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

    virtual void doClear() override
    {
        m_bytes.clear();
        m_nodes.clear();
    }

    virtual void construct(T** val) const override
    {
        std::fill(*val, *val + m_bufferSize, 0);
    }

    const std::size_t m_bufferSize;
    const std::size_t m_bytesPerBlock;

    std::deque<std::unique_ptr<std::vector<T>>> m_bytes;
    std::deque<std::unique_ptr<std::vector<Node<T*>>>> m_nodes;
    mutable SpinLock m_spin;
};

} // namespace splicer


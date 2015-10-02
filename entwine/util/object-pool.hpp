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

#include <atomic>
#include <chrono>
#include <deque>
#include <mutex>
#include <thread>
#include <vector>

#include <iostream>

namespace entwine
{

template<typename T>
class ObjectPool
{
public:
    class Stack;

    class Node
    {
        friend class ObjectPool;
        friend class Stack;

    public:
        Node(Node* next = nullptr) : m_val(), m_next(next) { }

        T& val() { return m_val; }

    private:
        Node* next() { return m_next; }
        void next(Node* node) { m_next = node; }

        T m_val;
        Node* m_next;
    };

    class Stack
    {
        friend class ObjectPool;

    public:
        Stack() : m_tail(nullptr), m_head(nullptr) { }

        void push(Node* node)
        {
            node->next(m_head);
            m_head = node;

            if (!m_tail) m_tail = node;
        }

        void push(Stack& other)
        {
            if (!other.empty())
            {
                push(other.m_tail);
                m_head = other.m_head;
            }
        }

        Node* pop()
        {
            Node* node(m_head);
            if (m_head)
            {
                m_head = m_head->next();
                if (!m_head) m_tail = nullptr;
            }
            return node;
        }

        bool empty() const { return !m_head; }

    private:
        Node* head() { return m_head; }

        Node* m_tail;
        Node* m_head;
    };

    ObjectPool(std::size_t blockSize = 4096)
        : m_blocks()
        , m_stack()
        , m_blockSize(blockSize)
        , m_count(0)
        , m_mutex()
        , m_adding()
    {
        m_adding.clear();
        allocate();
    }

    std::size_t count() const { return m_count.load(); }

    void release(Node* node)
    {
        node->val().~T();
        new (&node->val()) T();
        std::lock_guard<std::mutex> lock(m_mutex);
        m_stack.push(node);
    }

    void release(Stack& other)
    {
        Node* node(other.head());
        while (node)
        {
            node->val().~T();
            new (&node->val()) T();
            node = node->next();
        }

        std::lock_guard<std::mutex> lock(m_mutex);
        m_stack.push(other);
    }

    template <class... Args>
    Node* acquire(Args&&... args)
    {
        std::unique_lock<std::mutex> lock(m_mutex);
        Node* node(m_stack.pop());

        if (node)
        {
            new (&node->val()) T(std::forward<Args>(args)...);
            return node;
        }
        else
        {
            lock.unlock();
            allocate();
            return acquire(std::forward<Args>(args)...);
        }
    }

    void print() { m_stack.print(); }

private:
    void allocate()
    {
        TryLocker locker(m_adding);

        if (locker.tryLock())
        {
            std::unique_ptr<std::vector<Node>> newBlock(
                    new std::vector<Node>(m_blockSize));

            Stack newStack;

            for (std::size_t i(0); i < newBlock->size(); ++i)
            {
                newStack.push(&newBlock->operator[](i));
            }

            m_count += m_blockSize;

            {
                std::lock_guard<std::mutex> lock(m_mutex);
                m_blocks.emplace_back(nullptr);
                m_blocks[m_blocks.size() - 1].reset(newBlock.release());
                m_stack.push(newStack);
            }
        }
        else
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
    }

    class TryLocker
    {
    public:
        explicit TryLocker(std::atomic_flag& flag) : m_flag(flag) { }
        ~TryLocker() { m_flag.clear(); }

        bool tryLock() { return !m_flag.test_and_set(); }

    private:
        std::atomic_flag& m_flag;
    };

    std::deque<std::unique_ptr<std::vector<Node>>> m_blocks;
    Stack m_stack;

    const std::size_t m_blockSize;
    std::atomic_size_t m_count;

    std::mutex m_mutex;
    std::atomic_flag m_adding;
};

} // namespace entwine


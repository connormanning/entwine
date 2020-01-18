/******************************************************************************
* Copyright (c) 2020, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

namespace entwine
{

template<class ForwardIt>
ForwardIt min_element(ForwardIt curr, ForwardIt last)
{
    if (curr == last) return last;

    ForwardIt smallest = curr;
    ++curr;
    for ( ; curr != last; ++curr)
    {
        if (*curr < *smallest) smallest = curr;
    }
    return smallest;
}

template<class ForwardIt, class Compare>
ForwardIt min_element(ForwardIt curr, ForwardIt last, Compare comp)
{
    if (curr == last) return last;

    ForwardIt smallest = curr;
    ++curr;
    for ( ; curr != last; ++curr)
    {
        if (comp(*curr, *smallest)) smallest = curr;
    }
    return smallest;
}

template<class ForwardIt>
ForwardIt max_element(ForwardIt curr, ForwardIt last)
{
    if (curr == last) return last;

    ForwardIt largest = curr;
    ++curr;
    for ( ; curr != last; ++curr)
    {
        if (*largest < *curr) largest = curr;
    }
    return largest;
}

template<class ForwardIt, class Compare>
ForwardIt max_element(ForwardIt curr, ForwardIt last, Compare comp)
{
    if (curr == last) return last;

    ForwardIt largest = curr;
    ++curr;
    for ( ; curr != last; ++curr)
    {
        if (comp(*largest, *curr)) largest = curr;
    }
    return largest;
}

} // namespace entwine

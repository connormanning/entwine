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

#include <cmath>
#include <iomanip>
#include <iostream>
#include <vector>

namespace entwine
{
namespace matrix
{

inline const std::vector<double>& identity()
{
    static std::vector<double> v
    {
        1, 0, 0, 0,
        0, 1, 0, 0,
        0, 0, 1, 0,
        0, 0, 0, 1
    };

    return v;
}

inline double determinant(const std::vector<double>& v)
{
    if (v.size() == 4)
    {
        return v[0] * v[3] - v[1] * v[2];
    }
    else
    {
        double agg(0);
        const std::size_t n(std::sqrt(v.size()));

        for (std::size_t i(0); i < n; ++i)
        {
            std::vector<double> minor;
            for (std::size_t j(n); j < v.size(); ++j)
            {
                if (j % n != i % n) minor.push_back(v[j]);
            }

            // Addition of 0.0 is to avoid possible negative-zero values.
            agg += v[i] * determinant(minor) * ((i % 2) ? -1 : 1) + 0.0;
        }

        return agg;
    }
}

inline std::vector<double> cofactor(const std::vector<double>& v)
{
    std::vector<double> r;
    const std::size_t n(std::sqrt(v.size()));

    for (std::size_t i(0); i < v.size(); ++i)
    {
        std::vector<double> minor;
        for (std::size_t j(0); j < v.size(); ++j)
        {
            if (j % n != i % n && j / n != i / n) minor.push_back(v[j]);
        }

        // Addition of 0.0 is to avoid possible negative-zero values.
        r.push_back(
                determinant(minor) * (((i / n + i % n) % 2) ? -1 : 1) + 0.0);
    }

    return r;
}

inline std::vector<double> adjoint(const std::vector<double>& v)
{
    std::vector<double> r(v.size(), 0);
    const std::size_t n(std::sqrt(v.size()));

    for (std::size_t i(0); i < v.size(); ++i)
    {
        r[i / n  * n + i % n] = v[i % n * n + i / n];
    }

    return r;
}

inline std::vector<double> inverse(const std::vector<double>& v)
{
    const double det(determinant(v));
    const auto adj(adjoint(cofactor(v)));

    auto r(adj);
    std::transform(r.begin(), r.end(), r.begin(), [&det](double d)
    {
        return d / det;
    });

    return r;
}

inline std::vector<double> multiply(
        const std::vector<double>& a,
        const std::vector<double>& b)
{
    std::vector<double> r(16, 0);

    for (std::size_t i(0); i < 4; ++i)
    {
        for (std::size_t j(0); j < 4; ++j)
        {
            for (std::size_t k(0); k < 4; ++k)
            {
                r[i * 4 + j] += a[i * 4 + k] * b[k * 4 + j];
            }
        }
    }

    return r;
}

inline void print(
        const std::vector<double>& v,
        const std::size_t precision = 0)
{
    std::size_t i(0);
    const std::size_t n(std::sqrt(v.size()));

    if (precision)
    {
        std::cout << std::setprecision(precision);
    }

    std::cout << "[\n\t";
    for (const auto d : v)
    {
        std::cout << d;
        if (i < v.size() - 1) std::cout << ", ";
        if (++i % n == 0) std::cout << "\n";
        if (i != v.size()) std::cout << "\t";
    }
    std::cout << "]" << std::endl;
}

inline std::vector<double> flip(const std::vector<double>& v)
{
    std::vector<double> r;
    const std::size_t n(std::sqrt(v.size()));

    for (std::size_t i(0); i < n; ++i)
    {
        for (std::size_t j(0); j < v.size(); j += n)
        {
            r.push_back(v.at(i + j));
        }
    }

    return r;
}

} // namespace matrix
} // namespace entwine


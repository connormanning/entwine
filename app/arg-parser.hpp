/******************************************************************************
* Copyright (c) 2018, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#pragma once

#include <functional>
#include <string>
#include <vector>

#include <entwine/util/json.hpp>

namespace entwine
{

using Args = std::vector<std::string>;
using Handler = std::function<void(json)>;

class ArgParser
{
public:
    void setUsage(std::string usage)
    {
        m_usage = usage;
    }

    void logUsage() const
    {
        std::cout << "\nUsage: " << m_usage << "\n" << std::endl;
        for (const std::string d : m_descriptions)
        {
            std::cout << d << std::endl;
        }
    }

    bool handle(Args args) const
    {
        if (args.empty())
        {
            logUsage();
            return false;
        }
        if (args.size() == 1)
        {
            const auto arg(args.front());
            if (arg == "help" || arg == "--help" || arg == "-h")
            {
                logUsage();
                return false;
            }
        }

        uint64_t i(0);
        while (i < args.size())
        {
            std::string flag = args.at(i);

            if (!i && flag.front() != '-')
            {
                if (!m_handlers.count(""))
                {
                    throw std::runtime_error("Invalid argument: " + flag);
                }

                flag = "";
                --i;
            }

            if (!m_handlers.count(flag))
            {
                throw std::runtime_error("Invalid argument: " + flag);
            }

            json val;
            while (++i < args.size() && args.at(i).front() != '-')
            {
                val.push_back(args.at(i));
            }

            if (!val.is_null() && val.size() == 1) val = val[0];

            try
            {
                m_handlers.at(flag)(val);
            }
            catch (std::exception& e)
            {
                throw std::runtime_error(
                        "Error handling argument '" + flag + "' with value " +
                        val.dump(2) + ": " + e.what());
            }
        }

        return true;
    }

    void addDescription(std::string a, std::string description)
    {
        m_descriptions.push_back(
                tab(1) + a + "\n" + buildDescription(description));
    }

    void addDescription(std::string a, std::string b, std::string description)
    {
        m_descriptions.push_back(
                tab(1) + a + ", " + b + "\n" + buildDescription(description));
    }

    void add(std::string flag, std::string description, Handler h)
    {
        m_handlers[flag] = h;
        addDescription(flag, description);
    }

    void add(std::string a, std::string b, std::string description, Handler h)
    {
        m_handlers[a] = h;
        m_handlers[b] = h;
        addDescription(a, b, description);
    }

    void addDefault(
            std::string a,
            std::string b,
            std::string description,
            Handler h)
    {
        add(a, b, description, h);
        m_handlers[""] = h;
    }

private:
    std::string tab(uint64_t n) const { return std::string(n * 4, ' '); }

    std::vector<std::string> split(std::string in, char delim) const
    {
        std::vector<std::string> tokens;
        std::size_t pos(0);
        while (pos != std::string::npos)
        {
            std::size_t end(in.find_first_of(delim, pos));

            if (end == std::string::npos)
            {
                tokens.push_back(in.substr(pos));
                pos = end;
            }
            else
            {
                tokens.push_back(in.substr(pos, end - pos));
                pos = end + 1;
                if (pos >= in.size()) pos = std::string::npos;
            }
        }
        return tokens;
    }

    std::string buildDescription(std::string in) const
    {
        std::string out;
        std::vector<std::string> lines(split(in, '\n'));
        for (uint64_t i(0); i < lines.size(); ++i)
        {
            out += formatLine(lines[i]);
            if (i != lines.size() - 1) out += '\n';
        }
        return out;
    }

    std::string formatLine(std::string in) const
    {
        std::vector<std::string> words(split(in, ' '));
        const auto pre(tab(2));

        std::string out;
        std::string line;
        for (std::size_t i(0); i < words.size(); ++i)
        {
            const auto word(words[i]);

            if (line.size() + word.size() > 80)
            {
                out += line + '\n';
                line.clear();
            }

            line += (line.empty() ? pre : " ") + word;
        }

        if (line.size()) out += line;
        out += '\n';

        return out;
    }

    std::string m_usage;
    std::map<std::string, Handler> m_handlers;
    std::vector<std::string> m_descriptions;
};

} // namespace entwine


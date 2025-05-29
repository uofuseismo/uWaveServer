#ifndef PRIVATE_TO_JSON_HPP
#define PRIVATE_TO_JSON_HPP
#include <vector>
#include <cmath>
#include <chrono>
#include <set>
#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>
#include "uWaveServer/packet.hpp"
#include "private/toName.hpp"
namespace
{

nlohmann::json toJSON(const UWaveServer::Packet &packet)
{
    nlohmann::json result;
    result["samplingRate"] = packet.getSamplingRate();
    result["startTimeMuSec"] = packet.getStartTime().count();
    auto dataType = packet.getDataType(); 
    if (!packet.empty())
    {
        if (dataType == UWaveServer::Packet::DataType::Integer32)
        {
            result["samples"] = packet.getData<int> ();
        }
        else if (dataType == UWaveServer::Packet::DataType::Integer64)
        {
            result["samples"] = packet.getData<int64_t> ();
        }
        else if (dataType == UWaveServer::Packet::DataType::Double)
        {
            result["samples"] = packet.getData<double> ();
        }
        else if (dataType == UWaveServer::Packet::DataType::Float)
        {
            result["samples"] = packet.getData<float> ();
        }
        else
        {
            spdlog::warn("Undefined data type");
            result["samples"] = nullptr;
        }
    }
    else
    {
        result["samples"] = nullptr;
    }
    return result;
}

nlohmann::json toJSON(const std::vector<UWaveServer::Packet> &packets)
{
    nlohmann::json result;
    if (packets.empty()){return result;}
    // Get the unique sensor names
    std::set<std::string> sensors;
    for (const auto &packet : packets)
    {
        auto name = ::toName(packet);
        if (!sensors.contains(name))
        {
            sensors.insert(name);
        }
    }
    // Package each packet
    for (const auto &sensor : sensors)
    {
        std::vector<std::pair<std::chrono::microseconds, int>> workSpace;
        workSpace.reserve(packets.size());
        std::string network;
        std::string station;
        std::string channel;
        std::string locationCode;
        for (int i = 0; i < static_cast<int> (packets.size()); ++i)
        {
            auto name = ::toName(packets[i]);
            if (name == sensor)
            {
                if (network.empty())
                {
                    network = packets[i].getNetwork();
                    station = packets[i].getStation();
                    channel = packets[i].getChannel();
                    locationCode = packets[i].getLocationCode();
                }
                workSpace.push_back(std::pair {packets[i].getStartTime(), i});
                //::toJSON(packet); 
            }
        }
        if (!workSpace.empty())
        {
            nlohmann::json sensorData;
            sensorData["network"] = network;
            sensorData["station"] = station;
            sensorData["channel"] = channel;
            if (locationCode.empty()){locationCode = "--";}
            sensorData["locationCode"] = locationCode;
            // Argsort 
            std::sort(workSpace.begin(), workSpace.end(),
                      [](const auto &lhs, const auto &rhs)
                      {
                          return lhs.first < rhs.first;
                      });
            // Pack data in order
            nlohmann::json packetsJSON;
            for (const auto &index : workSpace)
            {
                try
                {
                     packetsJSON.push_back(::toJSON(packets.at(index.second)));
                }
                catch (const std::exception &e)
                {
                     spdlog::warn(e.what());
                }
            }
            sensorData["packets"] = packetsJSON;
            result.push_back(sensorData);
        }
    }
    return result;
}

}
#endif

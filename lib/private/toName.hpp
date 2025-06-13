#ifndef PRIVATE_TO_NAME_HPP
#define PRIVATE_TO_NAME_HPP
#include <string>
#include "uWaveServer/packet.hpp"
namespace
{
[[nodiscard]] std::string toName(const std::string &network,
                                 const std::string &station,
                                 const std::string &channel,
                                 const std::string &locationCode)
{
    auto name = network + "." + station + "." + channel;
    if (!locationCode.empty())
    {    
        name = name + "." + locationCode;
    }    
    return name;
}

[[nodiscard]] std::string toName(const UWaveServer::Packet &packet)
{
    auto network = packet.getNetwork();
    auto station = packet.getStation(); 
    auto channel = packet.getChannel();
    std::string locationCode;
    try 
    {   
        locationCode = packet.getLocationCode();
    }
    catch (...)
    {   
    }
    return ::toName(network, station, channel, locationCode);
    /*
    auto name = network + "."  + station + "." + channel;
    if (!locationCode.empty())
    {   
        name = name + "." + locationCode;
    }
    return name;
    */
}
}
#endif

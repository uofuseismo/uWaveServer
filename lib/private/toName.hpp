#ifndef PRIVATE_TO_NAME_HPP
#define PRIVATE_TO_NAME_HPP
#include <string>
#include "uWaveServer/packet.hpp"
namespace
{
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
    auto name = network + "."  + station + "." + channel;
    if (!locationCode.empty())
    {   
        name = name + "." + locationCode;
    }
    return name;

}
}
#endif

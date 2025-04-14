#include "uWaveServer/dataClient/seedLink.hpp"
#include "uWaveServer/packet.hpp"

using namespace UWaveServer::DataClient;

IDataClient::Type SEEDLink::getType() const noexcept
{
    return Type::SEEDLink;
}

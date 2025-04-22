#include "uWaveServer/dataClient/dataClient.hpp"
#include "uWaveServer/packet.hpp"

using namespace UWaveServer::DataClient;

class IDataClient::IDataClientImpl
{
public:
    void addPackets(Packet &&packet)
    {
        std::vector<Packet> packets{std::move(packet)};
    }
    void addPackets(std::vector<Packet> &&packets)
    {
        mCallback(std::move(packets));
    }
    std::function<void (std::vector<Packet> &&packets)> mCallback;
    bool mHaveCallback{false};
};

/// Constructor
IDataClient::IDataClient() :
    pImpl(std::make_unique<IDataClientImpl> ())
{
}

/// Add packets
void IDataClient::addPacket(Packet &&packet)
{
    std::vector<Packet> packets{std::move(packet)};    
    addPackets(std::move(packets));
}

void IDataClient::addPackets(std::vector<Packet> &&packets)
{
    if (!pImpl->mHaveCallback)
    {
        throw std::runtime_error("Packet adding callback not set");
    }
    if (packets.empty()){return;}
    pImpl->mCallback(std::move(packets));
}

/// Destructor
IDataClient::~IDataClient() = default;

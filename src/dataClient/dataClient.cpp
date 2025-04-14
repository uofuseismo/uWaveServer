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

/// Destructor
IDataClient::~IDataClient() = default;

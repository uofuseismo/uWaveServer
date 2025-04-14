#ifndef UWAVE_SERVER_DATA_CLIENT_SEED_LINK_HPP
#define UWAVE_SERVER_DATA_CLIENT_SEED_LINK_HPP
#include <uWaveServer/dataClient/dataClient.hpp>
namespace UWaveServer::DataClient
{
class SEEDLink : public IDataClient
{
public:
    void stop() override;
private:
    class SEEDLinkImpl;
    std::unique_ptr<SEEDLinkImpl> pImpl;
};
}
#endif

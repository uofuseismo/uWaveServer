#include <grpc/grpc.h>
#include <grpcpp/grpcpp.h>
#include <spdlog/spdlog.h>
#include "uWaveServer/dataClient/grpc.hpp"
#include "uWaveServer/dataClient/grpcOptions.hpp"

using namespace UWaveServer::DataClient;

namespace
{

}

class GRPC::GRPCImpl
{
public:
    GRPCOptions mOptions; 
};

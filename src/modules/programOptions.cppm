module;

#include <boost/program_options.hpp>
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/ini_parser.hpp>
#include "uWaveServer/packetSanitizerOptions.hpp"
#include "uWaveServer/dataClient/seedLinkOptions.hpp"
#include "uWaveServer/dataClient/grpcOptions.hpp"
//#include "getEnvironmentVariable.hpp"

export module ProgramOptions;
import GetEnvironmentVariable;
import OTelOptions;

namespace UWaveServer
{

#define APPLICATION_NAME "uwsDataLoader"

export
struct ProgramOptions
{
    UWaveServer::PacketSanitizerOptions mPacketSanitizerOptions;
    std::vector<UWaveServer::DataClient::SEEDLinkOptions> seedLinkOptions;
    std::vector<UWaveServer::DataClient::GRPCOptions> grpcOptions;
    std::string applicationName{APPLICATION_NAME};
    OTelHTTPMetricsOptions otelHTTPMetricsOptions;
    OTelHTTPLogOptions otelHTTPLogOptions;
    OTelGRPCMetricsOptions otelGRPCMetricsOptions;
    OTelGRPCLogOptions otelGRPCLogOptions;
    std::chrono::seconds printSummaryInterval{std::chrono::minutes {15}};
    //std::string prometheusURL{"localhost:9020"};
    std::string databaseUser{getEnvironmentVariable("UWAVE_SERVER_DATABASE_READ_WRITE_USER")};
    std::string databasePassword{getEnvironmentVariable("UWAVE_SERVER_DATABASE_READ_WRITE_PASSWORD")};
    std::string databaseName{getEnvironmentVariable("UWAVE_SERVER_DATABASE_NAME")};
    std::string databaseHost{getEnvironmentVariable("UWAVE_SERVER_DATABASE_HOST", "localhost")};
    std::string databaseSchema{getEnvironmentVariable("UWAVE_SERVER_DATABASE_SCHEMA", "")};
    int databasePort{getIntegerEnvironmentVariable("UWAVE_SERVER_DATABASE_PORT", 5432)};
    int mQueueCapacity{8092}; // Want this big enough but not too big
    int nDatabaseWriterThreads{1};
    int verbosity{3};
    bool exportLogs{false};
    bool exportMetrics{false};
    bool exportHTTPLogs{true};
    bool exportHTTPMetrics{true};
};

}

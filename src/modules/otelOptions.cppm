module;
#include <cstdint>
#include <string>
#include <chrono>
#include <filesystem>

export module OTelOptions;

namespace UWaveServer
{

export struct OTelGRPCMetricsOptions
{
    std::string url{"localhost:4317"};
    std::chrono::milliseconds exportInterval{std::chrono::seconds {15}};
    std::chrono::milliseconds exportTimeOut{500};
    std::filesystem::path certificatePath; // Path to the cert file
};

export struct OTelGRPCLogOptions
{
    std::string url{"localhost:4317"};
    std::chrono::milliseconds exportTimeOut{500};
    std::filesystem::path certificatePath; // Path to the cert file
};


export struct OTelHTTPMetricsOptions
{
    std::string url{"localhost:4318"};
    std::chrono::milliseconds exportInterval{std::chrono::seconds {15}};
    std::chrono::milliseconds exportTimeOut{500};
    std::string suffix{"/v1/metrics"};
};

export struct OTelHTTPLogOptions
{
    std::string url{"localhost:4318"};
    std::filesystem::path certificatePath;
    std::string suffix{"/v1/logs"};
};

}

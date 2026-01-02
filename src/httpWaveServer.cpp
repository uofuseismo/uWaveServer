#include <iostream>
#include <iomanip>
#include <string>
#include <vector>
#include <chrono>
#include <spdlog/spdlog.h>
#include <boost/program_options.hpp>
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/ini_parser.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string.hpp>
#include <crow.h>
#include "uWaveServer/database/readOnlyClient.hpp"
#include "uWaveServer/database/credentials.hpp"
#include "uWaveServer/packet.hpp"
#include "lib/private/toMiniSEED.hpp"
#include "lib/private/toJSON.hpp"
#include "getEnvironmentVariable.hpp"
#include "metricsExporter.hpp"
#include "serverMetrics.hpp"

#define APPLICATION_NAME "uHTTPWaveServer"

namespace 
{

struct ProgramOptions
{
    std::string applicationName{APPLICATION_NAME};
    std::string prometheusURL{"localhost:9020"}; 

    std::string crowServerName{"localhost"};
    std::string crowBindAddress{"127.0.0.1"};

    std::string databaseUser{
        ::getEnvironmentVariable("UWAVE_SERVER_DATABASE_READ_ONLY_USER")
    };
    std::string databasePassword{
        ::getEnvironmentVariable("UWAVE_SERVER_DATABASE_READ_ONLY_PASSWORD")
    };  
    std::string databaseName{
        ::getEnvironmentVariable("UWAVE_SERVER_DATABASE_NAME")
    };  
    std::string databaseHost{
        ::getEnvironmentVariable("UWAVE_SERVER_DATABASE_HOST", "localhost")
    };  
    std::string databaseSchema{
        ::getEnvironmentVariable("UWAVE_SERVER_DATABASE_SCHEMA", "") 
    };  
    uint16_t databasePort{
        ::getIntegerEnvironmentVariable("UWAVE_SERVER_DATABASE_PORT", 5432)
    };
    std::set<std::string> databaseSchemas;

    int verbosity{3};
    uint16_t crowPort{8000}; 
    uint16_t nThreads{2}; // Min is 2 for crow
};

std::pair<std::string, bool> parseCommandLineOptions(int, char *[]);
void setVerbosityForSPDLOG(const int );
ProgramOptions parseIniFile(const std::filesystem::path &);

}

namespace
{

std::string getOriginalKey(
    const std::map<std::string, std::string> &lowerCaseToOriginalKeys,
    const std::vector<std::string> &candidateKeys)
{
    for (const auto &candidateKey : candidateKeys)
    {
        auto idx = lowerCaseToOriginalKeys.find(candidateKey);
        if (idx != lowerCaseToOriginalKeys.end())
        {
            return idx->second;
        }
    }
    return std::string {""};
}

}

/// Converts YYYY-MM-DDTHH:MM:SS or YYYY-MM-DDTHH:MM:SS.XXXXXX
/// to seconds since the epoch.
[[nodiscard]] double toTimeStamp(const std::string &timeStringIn)
{
    auto timeString = timeStringIn;
    if (timeString.back() == 'Z'){timeString.pop_back();}
    // In case we have something like 2025-04-22T12:13:22.32
    /*
    if (timeString.size() > 19)
    {
        while (timeString.size() < 26)
        {
            timeString.push_back('0');
        }
    }
    */
    int year{1900};
    unsigned int month{1};
    unsigned int dom{1};
    int hour{0};
    int minute{0};
    int second{0};
    int microSecond{0};
    if (timeString.size() == 26)
    {
        sscanf(timeString.c_str(),
               "%04d-%02d-%02dT%02d:%02d:%02d.%06d",
               &year, &month, &dom, &hour, &minute, &second, &microSecond);
    }
    else if (timeString.size() == 19)
    {
        sscanf(timeString.c_str(),
               "%04d-%02d-%02dT%02d:%02d:%02d",
               &year, &month, &dom, &hour, &minute, &second);
    }
    else
    {
        throw std::invalid_argument(timeString + " is an invalid time string");
    } 
    auto yearMonthDay = std::chrono::year_month_day(std::chrono::year {year},
                                                    std::chrono::month {month},
                                                    std::chrono::day {dom} );
    std::chrono::sys_days elapsedDays{yearMonthDay};
    auto hourMinuteSecond
        = std::chrono::hh_mm_ss<std::chrono::seconds> {
             std::chrono::hours {hour}
           + std::chrono::minutes (minute)
           + std::chrono::seconds (second) };
    auto timeStamp = elapsedDays.time_since_epoch() + hourMinuteSecond.to_duration();
    auto time = timeStamp.count() + microSecond*1.e-6;
    return time;
}


class CustomLogger : public crow::ILogHandler
{
public:
    CustomLogger()
    {
    }
    void log(const std::string &message, 
             crow::LogLevel level)
    {
        // Most common
        if (level == crow::LogLevel::Info)
        {
            spdlog::info(message);
        }
        else if (level == crow::LogLevel::Warning)
        {
            spdlog::warn(message);
        }
        else if (level == crow::LogLevel::Critical)
        {
            spdlog::critical(message);
        }
        else if (level == crow::LogLevel::Error)
        {
            spdlog::error(message);
        }
        else if (level == crow::LogLevel::Debug)
        {
            spdlog::debug(message);
        }
        else
        {
            spdlog::warn("Unhandled log level - logging " + message);
        }
    }
};

int main(int argc, char *argv[])
{
    // Get the ini file from the command line
    std::string iniFile;
    try 
    {   
        auto [iniFileName, isHelp] = ::parseCommandLineOptions(argc, argv);
        if (isHelp){return EXIT_SUCCESS;}
        iniFile = iniFileName;
    }   
    catch (const std::exception &e) 
    {    
        std::cerr << e.what() << std::endl;
        return EXIT_FAILURE;
    }

    // Read the program properties
    ::ProgramOptions programOptions;
    try 
    {   
        programOptions = ::parseIniFile(iniFile);
    }   
    catch (const std::exception &e) 
    {   
        spdlog::error(e.what());
        return EXIT_FAILURE;
    }   
    ::setVerbosityForSPDLOG(programOptions.verbosity);

    try
    {
        ::initializeMetrics(programOptions.prometheusURL);
    }
    catch (const std::exception &e)
    {
        spdlog::critical("Failed to initalize metrics with "
                       + std::string {e.what()});
        return EXIT_FAILURE;
    }

    mObservableSuccessResponses["stream-query"] = 0;
    mObservableServerErrorResponses["stream-query"] = 0;
    mObservableClientErrorResponses["stream-query"] = 0;

    UWaveServer::Database::Credentials databaseCredentials;
    databaseCredentials.setUser(programOptions.databaseUser);
    databaseCredentials.setPassword(programOptions.databasePassword);
    databaseCredentials.setHost(programOptions.databaseHost);
    databaseCredentials.setPort(programOptions.databasePort);
    databaseCredentials.setDatabaseName(programOptions.databaseName);
    databaseCredentials.setApplication(programOptions.applicationName);
    databaseCredentials.setSchema("ynp");

    std::vector<std::unique_ptr<UWaveServer::Database::ReadOnlyClient>> clients;
    auto client = std::make_unique<UWaveServer::Database::ReadOnlyClient> (databaseCredentials);
    clients.push_back(std::move(client));

    CustomLogger customLogger;
    crow::logger::setHandler(&customLogger);
    crow::SimpleApp app;

    CROW_ROUTE(app, "/")
    ([]()
    {
        spdlog::debug("Default route");
        return crow::response(200);
    });

    // Unpack something like:
    // host/query?network=UU&station=BGU&channel=HHZ&location=01&starttime=1235&endtime=678910&nodata=404
    CROW_ROUTE(app, "/stream-query")
    ([&](const crow::request &request)
    {
        // Make the keys lowercase
        auto originalKeys = request.url_params.keys();
        if (originalKeys.empty())
        {
            mObservableClientErrorResponses.add_or_assign("stream-query", 1);
            crow::response response;
            response.code = 400;
            response.body = "No query parameters specified - try something like network=UU&station=BGU&channel=HHZ&location=01&startime=1234&endtime=5678&nodata=204";
            return response;
        }
        std::map<std::string, std::string> lowerCaseToOriginalKeys;
        for (const auto &originalKey : originalKeys)
        {
            std::string lowerCaseKey{originalKey};
            std::transform(lowerCaseKey.begin(), lowerCaseKey.end(),
                           lowerCaseKey.begin(), ::tolower);
            lowerCaseToOriginalKeys.insert_or_assign(lowerCaseKey, originalKey);
        }
        // Unpack
        std::string network;
        std::string station;
        std::string channel;
        std::string locationCode;
        // Network
        auto networkKey
             = ::getOriginalKey(lowerCaseToOriginalKeys, {"net", "network"});
        if (!networkKey.empty())
        {
            if (request.url_params.get(networkKey) != nullptr)
            {
                network = request.url_params.get(networkKey);
            }
        }
        if (network.empty())
        {
            mObservableClientErrorResponses.add_or_assign("stream-query", 1);
            crow::response response;
            response.code = 400;
            response.body = "Network code must be supplied in net= or network= parameters - e.g., net=UU";
            return response;
        }
        // Station
        auto stationKey
            = ::getOriginalKey(lowerCaseToOriginalKeys, {"sta", "station"});
        if (!stationKey.empty())
        {
            if (request.url_params.get(stationKey) != nullptr)
            {
                station = request.url_params.get(stationKey);
            }
        }
        if (station.empty())
        {
            mObservableClientErrorResponses.add_or_assign("stream-query", 1);
            crow::response response;
            response.code = 400;
            response.body = "Station name must be supplied in sta= or station= parameters  e.g., sta=CTU";
            return response;
        }
        // Channel
        auto channelKey
            = ::getOriginalKey(lowerCaseToOriginalKeys, {"cha", "channel"});
        if (!channelKey.empty())
        {
            if (request.url_params.get(channelKey) != nullptr)
            {
                channel = request.url_params.get(channelKey);
            }
        }
        // Location code
        auto locationKey
            = ::getOriginalKey(lowerCaseToOriginalKeys, {"loc", "location"});
        if (!locationKey.empty())
        {
            if (request.url_params.get(locationKey) != nullptr)
            {
                locationCode = request.url_params.get(locationKey);
            }   
        }
        // Fix the names
        std::transform(network.begin(), network.end(),
                       network.begin(), ::toupper);
        std::transform(station.begin(), station.end(),
                       station.begin(), ::toupper);
        std::transform(channel.begin(), channel.end(),
                       channel.begin(), ::toupper);
        std::transform(locationCode.begin(), locationCode.end(),
                       locationCode.begin(), ::toupper);
        // Start time
        double startTime{0};
        auto startTimeKey
             = ::getOriginalKey(lowerCaseToOriginalKeys, {"start", "starttime"}); 
        if (!startTimeKey.empty())
        {
            if (request.url_params.get(startTimeKey))
            {
                try
                {
                    auto stringTime = request.url_params.get(startTimeKey);
                    startTime = ::toTimeStamp(stringTime);
                }
                catch (...)
                {
                    try
                    {
                        auto epochalTime
                            = crow::utility::lexical_cast<double>
                              (request.url_params.get(startTimeKey));
                        startTime = epochalTime; 
                    }
                    catch (...)
                    {
                        mObservableClientErrorResponses.add_or_assign(
                            "stream-query", 1);
                        crow::response response;
                        response.code = 400;
                        response.body = startTimeKey + "=" 
                                      + request.url_params.get(startTimeKey)
                                      + " must be valid epochal time or UTC date";
                        return response;
                    }
                }
            }
        }
        double endTime{0};
        auto endTimeKey
             = ::getOriginalKey(lowerCaseToOriginalKeys, {"end", "endtime"}); 
        if (!endTimeKey.empty())
        {
            if (request.url_params.get(endTimeKey))
            {
                try
                {
                    auto stringTime = request.url_params.get(endTimeKey);
                    endTime = ::toTimeStamp(stringTime);
                }
                catch (...)
                {
                    try
                    {
                        auto epochalTime = crow::utility::lexical_cast<double>
                                           (request.url_params.get(endTimeKey));
                        endTime = epochalTime; 
                    }
                    catch (...)
                    {
                        mObservableClientErrorResponses.add_or_assign(
                           "stream-query", 1);
                        crow::response response;
                        response.code = 400;
                        response.body = endTimeKey + "=" + 
                                      + request.url_params.get(endTimeKey)
                                      + " must be valid epochal time or UTC date";
                        return response;
                    }
                }
            }
        }
//std::cout << std::setprecision(16) << startTime << " " << endTime << " " << endTime - startTime << std::endl;
        if (startTime > endTime)
        {
            mObservableClientErrorResponses.add_or_assign("stream-query", 1);
            crow::response response;
            response.code = 400;
            response.body = "startime cannot exceed endtime";
            return response;
        }

        std::string format{"miniseed3"};
        auto formatKey
             = ::getOriginalKey(lowerCaseToOriginalKeys, {"format"}); 
        bool wantMiniSEED3{true};
        if (request.url_params.get(formatKey) != nullptr)
        {
            format = request.url_params.get(formatKey);
            std::transform(format.begin(), format.end(), format.begin(),
                            ::tolower);
            if (format != "mseed2" &&
                format != "mseed3" &&
                format != "miniseed2" &&
                format != "miniseed3" &&
                format != "json")
            {
                mObservableClientErrorResponses.add_or_assign(
                    "stream-query", 1);
                crow::response response;
                response.code = 400;
                response.body
                    = "format='" + format
                    + "' must be miniseed2, miniseed3, or json";
                return response;
            }
            if (format != "json")
            {
                if (format == "mseed2" ||
                    format == "miniseed2")
                {
                    format = "mseed2";
                    wantMiniSEED3 = false;
                }
                else if (format == "mseed3" ||
                         format == "miniseed3")
                {
                    format = "mseed3";
                    wantMiniSEED3 = true;
                }
                else
                {
                    spdlog::critical("Unhandled format " + format);
                    mObservableServerErrorResponses.add_or_assign(
                        "stream-query", 1);
                    crow::response response;
                    response.code = 500;
                    response.body = "server could not properly process format";
                    return response;
                }
            }
        } 
        // What to do when no data found data
        int noData{204};
        auto noDataKey
             = ::getOriginalKey(lowerCaseToOriginalKeys, {"nodata"});
        if (request.url_params.get(noDataKey) != nullptr)
        {
            spdlog::debug("No data found");
            noData
                = crow::utility::lexical_cast<int>
                  (request.url_params.get(noDataKey));
            if (noData != 204 && noData != 404)
            {
                crow::response response;
                response.code = 400;
                response.body = "nodata = " + std::to_string(noData)
                              + " can only be 204 or 404 - default is 204";
                return response;
            }
        }

        try
        {
            // TODO histogram metrics
            spdlog::debug("Unpacking data");
            auto now = std::chrono::high_resolution_clock::now();
            auto nowMuSeconds
                = std::chrono::time_point_cast<std::chrono::microseconds>
                  (now).time_since_epoch();
            std::vector<UWaveServer::Packet> packets;
            // Two-pass loop - first check our caches
            for (int strategy = 0; strategy < 2; ++strategy)
            {
                for (const auto &client : clients)
                {
                    if (strategy == 0)
                    {
                        constexpr bool checkCacheOnly{true};
                        if (client->contains(network, station,
                                             channel, locationCode,
                                             checkCacheOnly))
                        {
                            packets
                               = client->query(network, station,
                                               channel, locationCode,
                                               startTime, endTime);
                        }
                    }
                    else
                    {
                        // This will actually search the sensors table and
                        // potentially add the sensor to the cache
                        packets
                             = client->query(network, station,
                                             channel, locationCode,
                                             startTime, endTime);
                    }
                }
                if (!packets.empty()){break;}
            }
            if (packets.empty())
            {
                // I did my job right
                mObservableSuccessResponses.add_or_assign("stream-query", 1);
                spdlog::debug("No data packets found in query");
                crow::response response;
                response.code = noData;
                response.body = "No data found";
                return response;
            }
            constexpr int recordLength{512}; 
            if (format == "json")
            {
                mObservableSuccessResponses.add_or_assign("stream-query", 1);
                auto payload = ::packetsToCrowJSON(packets);
                crow::response response;
                response.set_header("Content-Type", "application/json");
                response.code = 200;
                response.body = payload.dump();
                return response;
            }
            else
            {
                constexpr int recordLength{512};
                mObservableSuccessResponses.add_or_assign("stream-query", 1);
                auto payload = ::toMiniSEED(packets, recordLength, wantMiniSEED3);
spdlog::info("returning : " +  std::to_string (packets.size()) + " with payload size " + std::to_string (payload.size()));
                crow::response response;
                response.set_header("Content-Type", "application/octet-stream");
                response.code = 200;
                response.body = payload;
                return response;
            }
            //std::cout << "hey packets; " << packets.size() << std::endl;
        }
        catch (const std::exception &e)
        {
            mObservableServerErrorResponses.add_or_assign("stream-query", 1);
            spdlog::warn(e.what());
            crow::response response;
            response.code = 500;
            response.body = "Server error";
            return response;
        }

        mObservableServerErrorResponses.add_or_assign("stream-query", 1);
        crow::response response;
        response.code = 500;
        response.body = "Unhandled server route";
        return response;
    });

    try
    {
        app.bindaddr(programOptions.crowBindAddress)
           .port(programOptions.crowPort)
           .server_name(programOptions.crowServerName)
           .concurrency(programOptions.nThreads)
//#ifdef ENABLE_COMPRESSION
//           .use_compression(crow::compression::algorithm::ZLIB)
//#endif
           //.multithreaded()
           .run();
        cleanupMetrics();
        return EXIT_SUCCESS;
    }
    catch (const std::exception &e)
    {
        spdlog::critical("uHTTPWebServer failed with "
                       + std::string {e.what()});
        cleanupMetrics();
        return EXIT_FAILURE;
    }
}

///--------------------------------------------------------------------------///
///                            Utility Functions                             ///
///--------------------------------------------------------------------------///
namespace
{

void setVerbosityForSPDLOG(const int verbosity)
{
    if (verbosity <= 1)
    {   
        spdlog::set_level(spdlog::level::critical);
    }   
    if (verbosity == 2){spdlog::set_level(spdlog::level::warn);}
    if (verbosity == 3){spdlog::set_level(spdlog::level::info);}
    if (verbosity >= 4){spdlog::set_level(spdlog::level::debug);}
}   

/// Read the program options from the command line
std::pair<std::string, bool> parseCommandLineOptions(int argc, char *argv[])
{
    std::string iniFile;
    boost::program_options::options_description desc(R"""(
The uHTTPWaveServer responds to GET requests and serves waveforms to clients.

Example usage is

    uHTTPWaveServer --ini=httpServer.ini

Allowed options)""");
    desc.add_options()
        ("help", "Produces this help message")
        ("ini",  boost::program_options::value<std::string> (),
                 "The initialization file for this executable");
    boost::program_options::variables_map vm;
    boost::program_options::store(
        boost::program_options::parse_command_line(argc, argv, desc), vm);
    boost::program_options::notify(vm);
    if (vm.count("help"))
    {
        std::cout << desc << std::endl;
        return {iniFile, true};
    }
    if (vm.count("ini"))
    {
        iniFile = vm["ini"].as<std::string>();
        if (!std::filesystem::exists(iniFile))
        {
            throw std::runtime_error("Initialization file: " + iniFile
                                   + " does not exist");
        }
    }
    return {iniFile, false};
}

ProgramOptions parseIniFile(const std::filesystem::path &iniFile)
{
    ::ProgramOptions options;
    if (!std::filesystem::exists(iniFile)){return options;}
    // Parse the initialization file
    boost::property_tree::ptree propertyTree;
    boost::property_tree::ini_parser::read_ini(iniFile, propertyTree);

    // Application name
    options.applicationName
        = propertyTree.get<std::string> ("General.applicationName",
                                         options.applicationName);
    if (options.applicationName.empty())
    {   
        options.applicationName = APPLICATION_NAME;
    }   
    options.verbosity
        = propertyTree.get<int> ("General.verbosity", options.verbosity);

    // Prometheus
    uint16_t prometheusPort
        = propertyTree.get<uint16_t> ("Prometheus.port", 9200);
    std::string prometheusHost
        = propertyTree.get<std::string> ("Prometheus.host", "localhost");
    if (!prometheusHost.empty())
    {   
        options.prometheusURL = prometheusHost + ":" 
                              + std::to_string(prometheusPort);
    }

    // Crow options

    // Database
    options.databaseUser
        = propertyTree.get<std::string> ("Database.user", 
                                         options.databaseUser);
    if (options.databaseUser.empty())
    {
        throw std::invalid_argument("Must specify database user as UWAVE_SERVER_DATABASE_READ_ONLY_USER or as Database.user in ini file");
    }
    options.databasePassword
        = propertyTree.get<std::string> ("Database.password",
                                         options.databasePassword);
    if (options.databasePassword.empty())
    {   
        throw std::invalid_argument("Must specify database password as UWAVE_SERVER_DATABASE_READ_ONLY_PASSWORD or as Database.password in ini file");
    }
    options.databaseName
        = propertyTree.get<std::string> ("Database.name",
                                         options.databaseName);
    if (options.databaseName.empty())
    {
        throw std::invalid_argument("Must specify database name as UWAVE_SERVER_DATABASE_NAME or as Database.name in ini file");
    }
    options.databaseHost
        = propertyTree.get<std::string> ("Database.host",
                                         options.databaseHost);
    if (options.databaseHost.empty())
    {
        throw std::invalid_argument("Must specify database host as UWAVE_SERVER_DATABASE_HOST or as Database.host in ini file");
    }
    options.databasePort
        = propertyTree.get<uint16_t> ("Database.port", options.databasePort);

    return options;
}

}

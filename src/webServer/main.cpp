#include <iostream>
#include <set>
#include <thread>
#include <vector>
#include <string>
#include <filesystem>
#include <spdlog/spdlog.h>
#include <boost/program_options.hpp>
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/ini_parser.hpp>
#include "uWaveServer/packet.hpp"
#include "uWaveServer/database/connection/postgresql.hpp"
#include "uWaveServer/database/client.hpp"
#include "callback.hpp"
#include "listener.hpp"

#define DEFAULT_ADDRESS "0.0.0.0" // 127.0.0.1 is good for local dev but this works better for containers

struct ProgramOptions
{
    boost::asio::ip::address address{boost::asio::ip::make_address(DEFAULT_ADDRESS)};
    std::filesystem::path documentRoot{std::filesystem::current_path()}; 
    std::set<std::string> schemas{"ynp", "utah"};
    int nThreads{1};
    unsigned short port{80}; //51};
    bool helpOnly{false};
};
[[nodiscard]] ::ProgramOptions parseCommandLineOptions(int argc, char *argv[]);

int main(int argc, char *argv[])
{
    ProgramOptions programOptions;
    try 
    {
        programOptions = parseCommandLineOptions(argc, argv);
        if (programOptions.helpOnly){return EXIT_SUCCESS;}
    }   
    catch (const std::exception &e) 
    {   
        spdlog::error(e.what());
        return EXIT_FAILURE;
    }   

    std::vector<std::unique_ptr<UWaveServer::Database::Client>> postgresClients;
    for (auto &schema : programOptions.schemas)
    {
        UWaveServer::Database::Connection::PostgreSQL databaseConnection;
 
        try
        {
            spdlog::info(
               "Creating TimeSeriesDB PostgreSQL database read-only connection...");
            auto user = std::string {std::getenv("UWAVE_SERVER_DATABASE_READ_ONLY_USER")};
            auto password = std::string {std::getenv("UWAVE_SERVER_DATABASE_READ_ONLY_PASSWORD")};
            auto databaseName = std::string {std::getenv("UWAVE_SERVER_DATABASE_NAME")};
            std::string host="localhost";
            try
            {
                host = std::string {std::getenv("UWAVE_SERVER_DATABASE_HOST")};
            }
            catch (...)
            {
            }
            int port = 5432;
            try
            {
                port = std::stoi(std::getenv("UWAVE_SERVER_DATABASE_PORT"));
            }
            catch (...)
            {
            }
            databaseConnection.setUser(user);
            databaseConnection.setPassword(password);
            databaseConnection.setAddress(host);
            databaseConnection.setPort(port);
            databaseConnection.setDatabaseName(databaseName);
            if (!schema.empty()){databaseConnection.setSchema(schema);}
            databaseConnection.setApplication("uwsWebServer");
            databaseConnection.connect();
            if (!schema.empty())
            {
                spdlog::info("Connected to " + databaseName
                           + " postgresql database with schema " + schema);
            }
            else
            {
                spdlog::info("Connected to " + databaseName
                           + " postgresql database");
            }
        }
        catch (const std::exception &e)
        {
            spdlog::error(
                "Failed to create PostgreSQL database connection; failed with "
              + std::string {e.what()});
            return EXIT_FAILURE;
        }
        try
        {
            auto postgresClient
                = std::make_unique<UWaveServer::Database::Client>
                   (std::move(databaseConnection));
            postgresClients.push_back(std::move(postgresClient));
        }
        catch (const std::exception &e)
        {
            spdlog::error("Failed to initialize client; failed with "
                        + std::string {e.what()});
            return EXIT_FAILURE;
        }
    }
/*
try
{
 //const double t1{1746460373 + 360}; //1745949629 + 1100 };
const double t1{1748295749 - 86400};
 const double t2{t1 + 320};
 auto result = postgresClient->query("WY", "YMV", "EHZ", "01", t1, t2);
if (!result.empty())
{
 spdlog::info("t1, obs t1" + std::to_string(t1) + " " + std::to_string(result.front().getStartTime().count()*1.e-6));
 spdlog::info("obs t2, t2" + std::to_string(result.back().getEndTime().count()*1.e-6) + " "  + std::to_string(t2));
}

 spdlog::info(std::to_string(result.size()));
}
catch (const std::exception &e)
{
    spdlog::error(e.what());
    return EXIT_FAILURE;
}
return EXIT_SUCCESS;
*/
    // The IO context is required for all I/O
    boost::asio::io_context ioContext{programOptions.nThreads};
    // The SSL context is required, and holds certificates
    boost::asio::ssl::context context{boost::asio::ssl::context::tlsv12};


    UWaveServer::WebServer::Callback callback{std::move(postgresClients)};

    // Create and launch a listening port
    spdlog::info("Launching HTTP listeners...");
    const auto documentRoot
        = std::make_shared<std::string> (programOptions.documentRoot);
    std::make_shared<UWaveServer::WebServer::Listener>(
        ioContext,
        context,
        boost::asio::ip::tcp::endpoint{programOptions.address, programOptions.port},
        documentRoot,
        callback.getCallbackFunction())->run();

    // Run the I/O service on the requested number of threads
    std::vector<std::thread> instances;
    instances.reserve(programOptions.nThreads - 1); 
    for (int i = programOptions.nThreads - 1; i > 0; --i)
    {
        instances.emplace_back([&ioContext]
                               {
                                   ioContext.run();
                               });
    }
    ioContext.run();

    return EXIT_SUCCESS;
}

/// @brief Parses the command line options.
::ProgramOptions parseCommandLineOptions(int argc, char *argv[])
{
    ::ProgramOptions result;
    boost::program_options::options_description desc(
R"""(
The uWaveServer responds to waveform queries.

Example usage:
    uWaveServer --address=127.0.0.1 --port=8080 --document_root=./ --n_threads=1

Additionally, you should specify the following environment variables:
    UWAVE_SERVER_DATABASE_HOST
    UWAVE_SERVER_DATABASE_NAME
    UWAVE_SERVER_DATABASE_PORT
    UWAVE_SERVER_DATABASE_READ_ONLY_USER
    UWAVE_SERVER_DATABASE_READ_ONLY_PASSWORD

Allowed options)""");
    desc.add_options()
        ("help",    "Produces this help message")
        ("address", boost::program_options::value<std::string> ()->default_value(DEFAULT_ADDRESS),
                    "The address at which to bind")
        ("port",    boost::program_options::value<uint16_t> ()->default_value(result.port),
                    "The port on which to bind")
        ("document_root", boost::program_options::value<std::string> ()->default_value(result.documentRoot),
                    "The document root in case files are served");
    //    ("n_threads", boost::program_options::value<int> ()->default_value(1),
    //                 "The number of threads");
    boost::program_options::variables_map vm; 
    boost::program_options::store(
        boost::program_options::parse_command_line(argc, argv, desc), vm); 
    boost::program_options::notify(vm);
    if (vm.count("help"))
    {   
        std::cout << desc << std::endl;
        result.helpOnly = true;
        return result;
    }
    if (vm.count("address"))
    {   
        auto address = vm["address"].as<std::string>();
        if (address.empty()){throw std::invalid_argument("Address is empty");}
        result.address = boost::asio::ip::make_address(address); 
    }   
    if (vm.count("port"))
    {   
        auto port = vm["port"].as<uint16_t> (); 
        result.port = port;
    }   
    if (vm.count("document_root"))
    {   
        auto documentRoot = vm["document_root"].as<std::string>();
        if (documentRoot.empty()){documentRoot = "./";}
        if (!std::filesystem::exists(documentRoot))
        {
            // We can attempt to make the directory
            if (!std::filesystem::create_directories(documentRoot))
            {
                throw std::runtime_error(
                   "Failed to make document root directory " + documentRoot);
            }
            if (!std::filesystem::exists(documentRoot))
            {
                throw std::runtime_error("Document root: " + documentRoot
                                       + " does not exist");
            }
        }
        result.documentRoot = documentRoot;
    }
    if (vm.count("n_threads"))
    {   
        auto nThreads = vm["n_threads"].as<int> (); 
        if (nThreads < 1){throw std::invalid_argument("Number of threads must be positive");}
        result.nThreads = nThreads;
    }   
    return result;
}

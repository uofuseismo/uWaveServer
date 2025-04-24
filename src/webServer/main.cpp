#include <thread>
#include <vector>
#include <string>
#include <filesystem>
#include <spdlog/spdlog.h>
#include "uWaveServer/packet.hpp"
#include "uWaveServer/database/connection/postgresql.hpp"
#include "uWaveServer/database/client.hpp"
#include "callback.hpp"
#include "listener.hpp"

struct ProgramOptions
{
    boost::asio::ip::address address{boost::asio::ip::make_address("127.0.0.1")};
    std::filesystem::path documentRoot{"./"}; 
    int nThreads{1};
    unsigned short port{8051};
};

int main()
{
    ProgramOptions programOptions;

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
        databaseConnection.setSchema("ynp");
        databaseConnection.setApplication("uwsWebServer");
        databaseConnection.connect();
        spdlog::info("Connected to " + databaseName + " postgresql database!");
    }
    catch (const std::exception &e)
    {
        spdlog::error(
            "Failed to create PostgreSQL database connection; failed with "
          + std::string {e.what()});
        return EXIT_FAILURE;
    }
    std::shared_ptr<UWaveServer::Database::Client> postgresClient{nullptr};
    try
    {
        postgresClient = std::make_shared<UWaveServer::Database::Client> (std::move(databaseConnection));
    }
    catch (const std::exception &e)
    {
        spdlog::error("Failed to initialize client; failed with" + std::string {e.what()});
        return EXIT_FAILURE;
    }

    // The IO context is required for all I/O
    boost::asio::io_context ioContext{programOptions.nThreads};
    // The SSL context is required, and holds certificates
    boost::asio::ssl::context context{boost::asio::ssl::context::tlsv12};


    UWaveServer::WebServer::Callback callback{postgresClient}; /*
                                              aqmsClients,
                                              ldapAuthenticator}; */
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

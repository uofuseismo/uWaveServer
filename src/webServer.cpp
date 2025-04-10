#include <spdlog/spdlog.h>
#include "uWaveServer/packet.hpp"
#include "uWaveServer/database/connection/postgresql.hpp"
#include "uWaveServer/database/client.hpp"

int main()
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
    try
    {
        UWaveServer::Database::Client client(std::move(databaseConnection));
    }
    catch (const std::exception &e)
    {
        spdlog::error("Failed to initialize client; failed with" + std::string {e.what()});
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}

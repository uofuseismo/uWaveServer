#include <string>
#include <soci/soci.h>
#include <soci/postgresql/soci-postgresql.h>
#include "uWaveServer/database/connection/postgresql.hpp"

using namespace UWaveServer::Database::Connection;

class PostgreSQL::PostgreSQLImpl
{
public:
    PostgreSQLImpl()
    {
        mSessionPtr = &mSession;
    } 
    soci::session mSession;
    void *mSessionPtr{nullptr};
    std::string mConnectionString;
    std::string mUser;
    std::string mPassword;
    std::string mDatabaseName;
    std::string mAddress;
    std::string mSchema;
    std::string mApplication{"uWaveServer"};
    int mPort{5432};
};

/// Constructor
PostgreSQL::PostgreSQL() :
    pImpl(std::make_unique<PostgreSQLImpl> ())
{
}

/// Move constructor
PostgreSQL::PostgreSQL(PostgreSQL &&pg) noexcept
{
    *this = std::move(pg);
}

/// Move assignment
PostgreSQL& PostgreSQL::operator=(PostgreSQL &&pg) noexcept
{
    if (&pg == this){return *this;}
    pImpl = std::move(pg.pImpl);
    return *this;
}

/// Destructor
PostgreSQL::~PostgreSQL() = default; 

/// User
void PostgreSQL::setUser(const std::string &user)
{
    if (user.empty())
    {
        throw std::invalid_argument("User is empty");
    }
    pImpl->mConnectionString.clear();
    pImpl->mUser = user;
}

std::string PostgreSQL::getUser() const
{
    if (!haveUser()){throw std::runtime_error("User not set");}
    return pImpl->mUser;
}

bool PostgreSQL::haveUser() const noexcept
{
    return !pImpl->mUser.empty();
}

/// Password
void PostgreSQL::setPassword(const std::string &password)
{
    if (password.empty())
    {
        throw std::invalid_argument("Password is empty");
    }
    pImpl->mConnectionString.clear();
    pImpl->mPassword = password;
}

std::string PostgreSQL::getPassword() const
{
    if (!havePassword()){throw std::runtime_error("Password not set");}
    return pImpl->mPassword;
}

bool PostgreSQL::havePassword() const noexcept
{
    return !pImpl->mPassword.empty();
}

/// Address
void PostgreSQL::setAddress(const std::string &address)
{
    if (address.empty())
    {
        throw std::invalid_argument("Address is empty");
    }
    pImpl->mConnectionString.clear();
    pImpl->mAddress = address;
}

std::string PostgreSQL::getAddress() const noexcept
{
    return pImpl->mAddress;
}

/// DB name
void PostgreSQL::setDatabaseName(const std::string &name)
{
    if (name.empty())
    {
        throw std::invalid_argument("Name is empty");
    }
    pImpl->mConnectionString.clear();
    pImpl->mDatabaseName = name;
}

std::string PostgreSQL::getDatabaseName() const
{
    if (!haveDatabaseName()){throw std::runtime_error("Database name not set");}
    return pImpl->mDatabaseName;
}

bool PostgreSQL::haveDatabaseName() const noexcept
{
    return !pImpl->mDatabaseName.empty();
}

/// Port
void PostgreSQL::setPort(const int port)
{
    if (port < 0){throw std::invalid_argument("Port cannot be negative");}
    pImpl->mConnectionString.clear();
    pImpl->mPort = port;
}

int PostgreSQL::getPort() const noexcept
{
    return pImpl->mPort;
}

/// Application
void PostgreSQL::setApplication(const std::string &application)
{
    if (application.empty())
    {
        throw std::invalid_argument("Application is empty");
    }
    pImpl->mConnectionString.clear();
    pImpl->mApplication = application;
}

std::string PostgreSQL::getApplication() const noexcept
{
    return pImpl->mApplication;
}

/// Schema
void PostgreSQL::setSchema(const std::string &schema)
{
    pImpl->mSchema = schema;
}

std::string PostgreSQL::getSchema() const noexcept
{
    return pImpl->mSchema;
} 

/// Drivername
std::string PostgreSQL::getDriver() noexcept
{
   return "postgresql";
}

/// Generate a connection string
std::string PostgreSQL::getConnectionString() const
{
    if (!pImpl->mConnectionString.empty()){return pImpl->mConnectionString;}
    if (!haveUser()){throw std::runtime_error("User not set");}
    if (!havePassword()){throw std::runtime_error("Password not set");}
    auto driver = getDriver();
    auto user = getUser();
    auto password = getPassword();
    auto address = getAddress();
    auto cPort = std::to_string(getPort()); 
    auto dbname = getDatabaseName();
    auto appName = getApplication();
    pImpl->mConnectionString = driver
                             + "://" + user
                             + ":" + password
                             + "@" + address
                             + ":" + cPort
                             + "/" + dbname
                             + "?connect_timeout=10"
                             + "&application_name=" + appName;
    return pImpl->mConnectionString;
}

/// Connect
void PostgreSQL::connect()
{
    auto connectionString = getConnectionString(); // Throws
    if (pImpl->mSession.is_connected()){pImpl->mSession.close();}
    try
    {
        pImpl->mSession.open(soci::postgresql, connectionString);
        auto schema = getSchema();
        if (!schema.empty())
        {
            auto query = "SET SCHEMA '" + schema + "'";
            pImpl->mSession.once << query;
        }
    }
    catch (const std::exception &e)
    {
        throw std::runtime_error("Failed to connect to postgresql with error:\n"
                               + std::string {e.what()});
    }
}

void PostgreSQL::reconnect()
{
    try
    {
        pImpl->mSession.reconnect();
    }
    catch (const std::exception  &e)
    {
        throw std::runtime_error("Reconnect to postgres failed with error:\n"
                               + std::string {e.what()});
    }
    try
    {
        auto schema = getSchema();
        if (!schema.empty())
        {
            auto query = "SET SCHEMA '" + schema + "'";
            pImpl->mSession.once << query;
        }
    }
    catch (const std::exception &e)
    {
        throw std::runtime_error("Failed to set schema to " + getSchema() 
                               + " during reconnect.  Failed with "
                               + std::string {e.what()});
    } 
}

bool PostgreSQL::isConnected() const noexcept
{
    return pImpl->mSession.is_connected();
}

/// Disconnect
void PostgreSQL::disconnect()
{
    if (pImpl->mSession.is_connected()){pImpl->mSession.close();}
}

std::uintptr_t PostgreSQL::getSession() const
{
    return reinterpret_cast<std::uintptr_t> (pImpl->mSessionPtr);
}


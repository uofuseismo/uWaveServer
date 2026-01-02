#include <string>
#include <chrono>
#include "uWaveServer/database/credentials.hpp"

using namespace UWaveServer::Database;

class Credentials::CredentialsImpl
{
public:
    std::string mConnectionString;
    std::string mUser;
    std::string mPassword;
    std::string mDatabaseName;
    std::string mHost;
    std::string mSchema;
    std::string mApplication{"uWaveServer"};
    std::chrono::seconds mTimeOut{5};
    int mPort{5432};
    bool mReadOnly{false};
};

/// Constructor
Credentials::Credentials() :
    pImpl(std::make_unique<CredentialsImpl> ())
{
}

/// Copy constructor
Credentials::Credentials(const Credentials &credentials)
{
    *this = credentials;
}

/// Move constructor
Credentials::Credentials(Credentials &&credentials) noexcept
{
    *this = std::move(credentials);
}

/// Move assignment
Credentials& Credentials::operator=(Credentials &&credentials) noexcept
{
    if (&credentials == this){return *this;}
    pImpl = std::move(credentials.pImpl);
    return *this;
}

/// Copy assignment
Credentials& Credentials::operator=(const Credentials &credentials)
{
    if (&credentials == this){return *this;}
    pImpl = std::make_unique<CredentialsImpl> (*credentials.pImpl);
    return *this;
}

/// Destructor
Credentials::~Credentials() = default; 

/// User
void Credentials::setUser(const std::string &user)
{
    if (user.empty())
    {
        throw std::invalid_argument("User is empty");
    }
    pImpl->mConnectionString.clear();
    pImpl->mUser = user;
}

std::string Credentials::getUser() const
{
    if (!haveUser()){throw std::runtime_error("User not set");}
    return pImpl->mUser;
}

bool Credentials::haveUser() const noexcept
{
    return !pImpl->mUser.empty();
}

/// Password
void Credentials::setPassword(const std::string &password)
{
    if (password.empty())
    {
        throw std::invalid_argument("Password is empty");
    }
    pImpl->mConnectionString.clear();
    pImpl->mPassword = password;
}

std::string Credentials::getPassword() const
{
    if (!havePassword()){throw std::runtime_error("Password not set");}
    return pImpl->mPassword;
}

bool Credentials::havePassword() const noexcept
{
    return !pImpl->mPassword.empty();
}

/// Host
void Credentials::setHost(const std::string &host)
{
    if (host.empty())
    {
        throw std::invalid_argument("Host is empty");
    }
    pImpl->mConnectionString.clear();
    pImpl->mHost = host;
}

std::string Credentials::getHost() const noexcept
{
    return pImpl->mHost;
}

/// DB name
void Credentials::setDatabaseName(const std::string &name)
{
    if (name.empty())
    {
        throw std::invalid_argument("Name is empty");
    }
    pImpl->mConnectionString.clear();
    pImpl->mDatabaseName = name;
}

std::string Credentials::getDatabaseName() const
{
    if (!haveDatabaseName()){throw std::runtime_error("Database name not set");}
    return pImpl->mDatabaseName;
}

bool Credentials::haveDatabaseName() const noexcept
{
    return !pImpl->mDatabaseName.empty();
}

/// Port
void Credentials::setPort(const int port)
{
    if (port < 0){throw std::invalid_argument("Port cannot be negative");}
    pImpl->mConnectionString.clear();
    pImpl->mPort = port;
}

int Credentials::getPort() const noexcept
{
    return pImpl->mPort;
}

/// Application
void Credentials::setApplication(const std::string &application)
{
    if (application.empty())
    {
        throw std::invalid_argument("Application is empty");
    }
    pImpl->mConnectionString.clear();
    pImpl->mApplication = application;
}

std::string Credentials::getApplication() const noexcept
{
    return pImpl->mApplication;
}

/// Schema
void Credentials::setSchema(const std::string &schema)
{
    pImpl->mSchema = schema;
}

std::string Credentials::getSchema() const noexcept
{
    return pImpl->mSchema;
} 

/// Drivername
std::string Credentials::getDriver() noexcept
{
   return "postgresql";
}

/// Generate a connection string
const char *Credentials::getConnectionString() const
{
    if (!haveUser()){throw std::runtime_error("User not set");}
    if (!havePassword()){throw std::runtime_error("Password not set");}
    auto driver = getDriver();
    auto user = getUser();
    auto password = getPassword();
    auto host = getHost();
    auto cPort = std::to_string(getPort()); 
    auto dbname = getDatabaseName();
    auto applicationName = getApplication();
    std::string cTimeOut;
    if (pImpl->mTimeOut.count() > 0)
    {
        cTimeOut = std::to_string(pImpl->mTimeOut.count());
    }
    pImpl->mConnectionString = "user=" + user
                             + " password=" + password
                             + " host=" + host 
                             + " dbname=" + dbname
                             + " port=" + cPort;
   
    if (!cTimeOut.empty())
    {
        pImpl->mConnectionString = pImpl->mConnectionString
                                 + " connect_timeout=" + cTimeOut;
    }
    if (!applicationName.empty())
    {
        pImpl->mConnectionString = pImpl->mConnectionString
                                 + " application_name=" + applicationName;
    }
    return pImpl->mConnectionString.c_str();
}

/// Read-only?
void Credentials::enableReadOnly() noexcept
{
    pImpl->mReadOnly = true;
}

void Credentials::enableReadWrite() noexcept
{
    pImpl->mReadOnly = false;
}

bool Credentials::isReadOnly() const noexcept
{
    return pImpl->mReadOnly;
}

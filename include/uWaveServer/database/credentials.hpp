#ifndef UWAVE_SERVER_DATABASE_CREDENTIALS_HPP
#define UWAVE_SERVER_DATABASE_CREDENTIALS_HPP
#include <cstdint>
#include <memory>
namespace UWaveServer::Database
{
/// @name Credentials "credentials.hpp"
/// @brief Encapsulates the parameters for a libpqxx PostgreSQL connection.
/// @copyright Ben Baker (University of Utah) distributed under the MIT license. 
class Credentials
{
public:
    /// @name Constructors
    /// @{

    /// @brief Constructor.
    Credentials();
    /// @brief Copy constructor.
    /// @param[in] credentials  The credentials from which to initialize
    ///                         this class.
    Credentials(const Credentials &credentials);
    /// @brief Move constructor.
    /// @param[in,out] credentials  The credentials from which to initialize
    ///                             this class.
    Credentials(Credentials &&credentials) noexcept;
    /// @}

    /// @name User Name
    /// @{

    /// @brief Sets the user name.
    /// @param[in] user  The user name.
    /// @throws std::invalid_argument if user is empty.
    void setUser(const std::string &user);
    /// @result The user name.
    /// @throws std::runtime_error if \c haveUser() is false.
    [[nodiscard]] std::string getUser() const;
    /// @result True indicates the user name was set.
    [[nodiscard]] bool haveUser() const noexcept;
    /// @}

    /// @name Password
    /// @{

    /// @brief Sets the user's password.
    /// @param[in] password  The user's password.
    /// @throws std::invalid_argument if password is empty.
    void setPassword(const std::string &password);
    /// @result The user's password.
    /// @throws std::runtime_error if \c havePassword() is false.
    [[nodiscard]] std::string getPassword() const;
    /// @result True indicates the user's password was set.
    [[nodiscard]] bool havePassword() const noexcept;
    /// @}

    /// @name Database Host Address
    /// @{

    /// @brief Sets the host's address.
    /// @param[in] host  The hosts's address e.g.,
    ///                  localhost or machine.domain.com
    /// @throws std::invalid_argument if the host is empty.
    void setHost(const std::string &address);
    /// @result The host address.
    /// @note By default this is 127.0.0.1
    [[nodiscard]] std::string getHost() const noexcept;
    /// @}

    /// @name Database Name
    /// @{

    /// @brief Sets the name of the database.
    /// @throws std::invalid_argument if name is empty.
    void setDatabaseName(const std::string &name);
    /// @result The name of the database to which to connect.
    /// @throw std::runtime_error if \c haveDatabasename() is false.
    [[nodiscard]] std::string getDatabaseName() const;
    /// @result True indicates the database name was set.
    [[nodiscard]] bool haveDatabaseName() const noexcept;
    /// @}

    /// @name Port Number
    /// @{

    /// @brief Sets the port number.
    void setPort(int port);
    /// @result The port number.  By default this is 5432.
    [[nodiscard]] int getPort() const noexcept;
    /// @}

    /// @name Schema
    /// @{

    /// @brief Sets the schema name.
    void setSchema(const std::string &schema);
    /// @result The schema name.
    [[nodiscard]] std::string getSchema() const noexcept;

    /// @name Application Name
    /// @{

    /// @brief Sets the name of the application.
    /// @param[in] application  The name of the application.
    /// @throws std::invalid_argument if the application is empty.
    void setApplication(const std::string &application);
    /// @result The application name.
    /// @note By default this is qurts.
    [[nodiscard]] std::string getApplication() const noexcept;

    /// @brief Enables the session as read-only.
    void enableReadOnly() noexcept;
    /// @brief Enables the session as read-write.
    void enableReadWrite() noexcept;
    /// @result True indicates the session is read-only. 
    [[nodiscard]] bool isReadOnly() const noexcept;
    /// @}

    /// @name Driver
    /// @{

    /// @result The driver (e.g., postgresql).
    [[nodiscard]] static std::string getDriver() noexcept;
    /// @}

    /// @result A connection string for pqxx.
    /// @throws std::runtime_error if any required parameter is not set.
    [[nodiscard]] const char *getConnectionString() const;

    /// @name Destructors
    /// @{

    /// @brief Destructor.
    ~Credentials();
    /// @}

    /// @name Operators
    /// @{

    /// @brief Copy assignment.
    /// @param[in] credentials  The credentials to copy to this.
    /// @result A deep copy of the credentials.
    Credentials& operator=(const Credentials &credentials); 
    /// @brief Move assignment.
    /// @param[in,out] credentials  The credentials whose memory will be moved
    ///                             to this.  On exit, credential's behavior is
    ///                             is undefined.
    /// @result The memory from the credentials moved to this.
    Credentials& operator=(Credentials &&credentials) noexcept;
    /// @}
private:
    class CredentialsImpl;
    std::unique_ptr<CredentialsImpl> pImpl;
};
}
#endif

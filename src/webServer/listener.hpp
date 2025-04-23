#ifndef LISTENER_HPP
#define LISTENER_HPP 
#include <string>
#include <memory>
#include <functional>
#include <boost/asio.hpp>
#include <boost/beast/ssl.hpp>
#include <boost/beast/http/message.hpp>
#include <boost/beast/http/verb.hpp>
namespace UWaveServer::WebServer
{
/// @class Listener "listener.hpp"
/// @brief The listener runs the IO service.  This container allows
///        multiple threads to listen on an endpoint.
/// @copyright Ben Baker (University of Utah) distributed under the MIT license.
class Listener : public std::enable_shared_from_this<Listener>
{
public:
    /// @brief Constructor
    /// @param[in] ioContext     The ASIO input/output context.
    /// @param[in] sslContext    If creating an https session then this will
    ///                          handle the encrypted IO. 
    /// @param[in] endpoint      The URI and port on which we're listening,
    ///                          e.g., 127.0.0.1 8080.
    /// @param[in] documentRoot  The directory with the document root,
    ///                          e.g., ./
    /// @param[in] callback      The callback function to process requests.
    Listener(boost::asio::io_context& ioContext,
             boost::asio::ssl::context &sslContext,
             boost::asio::ip::tcp::endpoint endpoint,
             const std::shared_ptr<const std::string> &documentRoot,
             const std::function
             <
                std::string (const boost::beast::http::header
                             <
                                 true,
                                 boost::beast::http::basic_fields<std::allocator<char>>
                             > &,
                             const std::string &,
                             const boost::beast::http::verb)
             > &callback);
    /// @brief Begin accepting incoming connections.
    void run();
private:
    void doAccept();
    void onAccept(boost::beast::error_code errorCode,
                  boost::asio::ip::tcp::socket socket);

    boost::asio::io_context &mIOContext;
    boost::asio::ssl::context &mSSLContext;
    boost::asio::ip::tcp::acceptor mAcceptor;
    std::shared_ptr<const std::string> mDocumentRoot;
    std::function<std::string (const boost::beast::http::header
                               <
                                  true,
                                  boost::beast::http::basic_fields<std::allocator<char>>
                               > &,
                               const std::string &,
                               const boost::beast::http::verb)> mCallback;
};
}
#endif

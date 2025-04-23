#ifndef UWAVE_SERVER_PACKET_SANITIZER_HPP
#define UWAVE_SERVER_PACKET_SANITIZER_HPP
#include <memory>
namespace UWaveServer
{
 class Packet;
 class PacketSanitizerOptions;
}
namespace UWaveServer
{

class PacketSanitizer
{
public:
    /// @brief Constructor.
    PacketSanitizer() = delete;
    /// @brief Constructs from options.
    explicit PacketSanitizer(const PacketSanitizerOptions &options);
    /// @brief Copy constructor.
    PacketSanitizer(const PacketSanitizer &sanitizer);
    /// @brief Move constructor.
    PacketSanitizer(PacketSanitizer &&sanitizer) noexcept;

    /// @result True indicates that this packet does not appear to be 
    //          a duplicate, extremely late, from the future, etc.
    bool allow(const Packet &packet);
    /// @brief Releases memory and resets the class.
    void clear() noexcept;
    /// @brief Destructor.
    ~PacketSanitizer();

    /// @brief Copy assignment.
    /// @result A deep copy of the this class. 
    PacketSanitizer& operator=(const PacketSanitizer &sanitizer);
    /// @brief Move assignment.
    /// @result The memory from sanitizer moved to this.
    PacketSanitizer& operator=(PacketSanitizer &&sanitizer) noexcept;
private:
    class PacketSanitizerImpl;
    std::unique_ptr<PacketSanitizerImpl> pImpl;
};

}
#endif

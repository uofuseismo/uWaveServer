cmake_minimum_required(VERSION 3.28)
project(uWaveServer VERSION 0.1.0 LANGUAGES CXX)
enable_testing()

option(ENABLE_SSL "Enable SSL connections for the web server backend" ON) 
option(WITH_CORS "Compile with CORS * for the web server backend" OFF)
option(BUILD_SHARED_LIBS "Build using shared libraries" ON)

include(FetchContent)

list(APPEND CMAKE_MODULE_PATH ${CMAKE_SOURCE_DIR}/cmake)

# Boost
if(POLICY CMP0167)
  cmake_policy(SET CMP0167 NEW)
endif()

find_package(spdlog REQUIRED)
find_package(SOCI COMPONENTS Core PostgreSQL REQUIRED)
find_package(PostgreSQL REQUIRED)
find_package(nlohmann_json 3.8.0 REQUIRED)
find_package(Boost COMPONENTS program_options url REQUIRED)
find_package(MiniSEED REQUIRED)
find_package(SEEDLink REQUIRED)
find_package(Catch2 3)
find_package(Earthworm)
if (${ENABLE_SSL})
   find_package(OpenSSL COMPONENTS SSL Crypto REQUIRED)
else()
   find_package(OpenSSL COMPONENTS SSL Crypto)
endif()
if (${OPENSSL_FOUND})
   message("Found SSL version ${OPENSSL_VERSION}")
endif()


configure_file(${CMAKE_SOURCE_DIR}/lib/private/version.hpp.in
               ${CMAKE_SOURCE_DIR}/include/uWaveServer/version.hpp)

set(LIBRARY_SRC
    lib/dataClient/dataClient.cpp
    lib/dataClient/streamSelector.cpp
    lib/testFuturePacket.cpp
    lib/packet.cpp
    lib/packetSanitizer.cpp
    lib/packetSanitizerOptions.cpp
    lib/version.cpp
    lib/database/client.cpp
    lib/database/connection/postgresql.cpp)
if (${SEEDLINK_FOUND})
   message("Using SEEDLink")
   set(LIBRARY_SRC ${LIBRARY_SRC}
       lib/dataClient/seedLink.cpp
       lib/dataClient/seedLinkOptions.cpp)
endif()
#if (${Earthworm_FOUND})
#   message("Using Earthworm")
#   target_compile_definitions(uWaveServer WITH_EARTHWORM)
#endif()

if (BUILD_SHARED_LIBS)
   message("Will build shared library")
   add_library(uWaveServer SHARED ${LIBRARY_SRC})
   set_target_properties(uWaveServer PROPERTIES
                         CXX_STANDARD 20
                         CXX_STANDARD_REQUIRED YES 
                         CXX_EXTENSIONS NO) 
else()
   message("Will build static library")
   add_library(uWaveServer STATIC ${LIBRARY_SRC})
   set_target_properties(uWaveServer PROPERTIES
                         CXX_STANDARD 20
                         CXX_STANDARD_REQUIRED YES
                         CXX_EXTENSIONS NO
                         OUTPUT_NAME uWaveServer-static)
endif()
target_link_libraries(uWaveServer
                      PUBLIC nlohmann_json::nlohmann_json SOCI::Core SOCI::PostgreSQL
                      PRIVATE spdlog::spdlog ${MINISEED_LIBRARY})
target_include_directories(uWaveServer
                           PRIVATE ${MINISEED_INCLUDE_DIR} ${PostgreSQL_INCLUDE_DIRS}
                           PRIVATE $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/lib>
                           PRIVATE $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
                           PUBLIC $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include/uWaveServer>
                           PUBLIC $<INSTALL_INTERFACE:include>)
if (${SEEDLINK_FOUND})
   target_link_libraries(uWaveServer PRIVATE ${SEEDLINK_LIBRARY})
   target_include_directories(uWaveServer PRIVATE ${SEEDLINK_INCLUDE_DIR})
   target_compile_definitions(uWaveServer PRIVATE WITH_SEEDLINK)
endif()
string(COMPARE GREATER_EQUAL ${MINISEED_VERSION} "3.1.5" USE_MS_VERSION_315)
if (USE_MS_VERSION_315)
   message("Using ms_sid2nslc_n")
   target_compile_definitions(uWaveServer PRIVATE USE_MS_VERSION_315)
endif()
if (${Earthworm_FOUND})
#   target_link_libraries(uWaveServer PRIVATE ${EARTHWORM_UTILITY_LIBRARY} ${EARTHWORM_MT_LIBRARY})
#   target_include_directories(uWaveServer PRIVATE ${EARTHWORM_INCLUDE_DIR})
    target_compile_definitions(uWaveServer PRIVATE WITH_EARTHWORM)
endif()

add_executable(uwsDataLoader
               src/main.cpp)
set_target_properties(uwsDataLoader PROPERTIES
                      CXX_STANDARD 20
                      CXX_STANDARD_REQUIRED YES 
                      CXX_EXTENSIONS NO) 
target_include_directories(uwsDataLoader
                           PRIVATE $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/lib>
                           PRIVATE $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>)
target_link_libraries(uwsDataLoader
                      PRIVATE uWaveServer spdlog::spdlog Boost::program_options)# SOCI::Core SOCI::PostgreSQL)

add_executable(uwsWebServer
               src/webServer/main.cpp
               src/webServer/callback.cpp
               src/webServer/listener.cpp)
if (${ENABLE_SSL})
   target_compile_definitions(uwsWebServer PUBLIC WITH_OPENSSL)
   target_link_libraries(uwsWebServer PRIVATE OpenSSL::SSL OpenSSL::Crypto)
endif()
set_target_properties(uwsWebServer PROPERTIES
                      CXX_STANDARD 20
                      CXX_STANDARD_REQUIRED YES 
                      CXX_EXTENSIONS NO) 
target_include_directories(uwsWebServer
                           PRIVATE $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/lib>
                           PRIVATE $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>)
target_link_libraries(uwsWebServer
                      PRIVATE uWaveServer spdlog::spdlog Boost::program_options Boost::url)# SOCI::Core SOCI::PostgreSQL)

if (${Catch2_FOUND})
   message("Found catch")
   add_executable(unitTests
                  testing/pack.cpp
                  testing/packet.cpp)
   set_target_properties(unitTests PROPERTIES
                         CXX_STANDARD 20
                         CXX_STANDARD_REQUIRED YES
                         CXX_EXTENSIONS NO)
   target_link_libraries(unitTests PRIVATE
                         Catch2::Catch2 Catch2::Catch2WithMain
                         uWaveServer ${MINISEED_LIBRARY})
   target_include_directories(unitTests
                              PRIVATE $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
                              PRIVATE $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/lib>
                              PRIVATE $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/src>
                              PRIVATE $<BUILD_INTERFACE:${UMPS_INCLUDE_DIR}>)
   add_test(NAME unitTests 
            COMMAND unitTests)
endif()
#add_executable(binaryTest test/binary.cpp)
#set_target_properties(binaryTest PROPERTIES
#                      CXX_STANDARD 20
#                      CXX_STANDARD_REQUIRED YES
#                      CXX_EXTENSIONS NO)
#target_include_directories(binaryTest
#                           PRIVATE $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/lib>
#                           PRIVATE $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>)
#target_link_libraries(binaryTest PRIVATE uWaveServer spdlog::spdlog Boost::program_options Boost::url SOCI::Core SOCI::PostgreSQL)

##########################################################################################
#                                      Installation                                      #
##########################################################################################
include(GNUInstallDirs)
include(CMakePackageConfigHelpers)
configure_package_config_file(
    cmake/${PROJECT_NAME}Config.cmake.in "${CMAKE_CURRENT_BINARY_DIR}/${PROJECT_NAME}Config.cmake"
    INSTALL_DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/${PROJECT_NAME}")
write_basic_package_version_file(
    "${CMAKE_CURRENT_BINARY_DIR}/${PROJECT_NAME}ConfigVersion.cmake"
    VERSION "${version}"
    COMPATIBILITY AnyNewerVersion
)
#install(TARGETS uWaveServer uwsWebServer uwsDataLoader
#        EXPORT ${PROJECT_NAME}-targets
#        RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
#        LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
#        ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
#        PUBLIC_HEADER DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
#        COMPONENT Runtime)
install(TARGETS uWaveServer
        EXPORT ${PROJECT_NAME}-targets
        ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
        COMPONENT libraries)
install(TARGETS uwsWebServer uwsDataLoader
        RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
        COMPONENT applications)
install(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/include/uWaveServer
        DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
        COMPONENT headers)
export(EXPORT ${PROJECT_NAME}-targets
       FILE "${CMAKE_CURRENT_BINARY_DIR}/${PROJECT_NAME}Targets.cmake")
install(EXPORT ${PROJECT_NAME}-targets
        FILE ${PROJECT_NAME}Targets.cmake
        DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/${PROJECT_NAME})
install(FILES
        ${CMAKE_CURRENT_BINARY_DIR}/${PROJECT_NAME}Config.cmake
        ${CMAKE_CURRENT_BINARY_DIR}/${PROJECT_NAME}ConfigVersion.cmake
        DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/${PROJECT_NAME})


##########################################################################################
#                                     CPACK Packaging                                    #
##########################################################################################
include(CPackComponent)
set(CPACK_PACKAGE_NAME ${PROJECT_NAME})
set(CPACK_PACKAGE_VENDOR "UUSS")
set(CPACK_PACKAGE_CONTACT "ben.baker@utah.edu")
set(CPACK_PACKAGE_LICENSE "MIT")
set(CPACK_PACKAGE_DESCRIPTION_SUMMARY "A set of services for interacting with a TimeScaleDB-enabled Postgres database for storing seismic data at UUSS.")
set(CPACK_PACKAGE_INSTALL_DIRECTORY ${CPACK_PACKAGE_NAME})
set(CPACK_VERBATIM_VARIABLES YES)
set(CPACK_PACKAGE_VERSION ${PROJECT_VERSION})
set(CPACK_PACKAGE_VERSION_MAJOR ${PROJECT_VERSION_MAJOR})
set(CPACK_PACKAGE_VERSION_MINOR ${PROJECT_VERSION_MINOR})
set(CPACK_PACKAGE_VERSION_PATCH ${PROJECT_VERSION_PATCH})
set(CPACK_RESOURCE_FILE_LICENSE "${CMAKE_SOURCE_DIR}/LICENSE")
cpack_add_component(libraries
                    DISPLAY_NAME "Application common library"
                    DEPENDS ${MINISEED_LIBRARY})
cpack_add_component(headers
                    DISPLAY_NAME "libuWaveServer header files"
                    DEPENDS libraries)
cpack_add_component(applications
                    DISPLAY_NAME "uWaveServer Applications"
                    DEPENDS libraries)
if (WIN32)
    set(CPACK_GENERATOR ZIP WIX)
elseif (APPLE)
    set(CPACK_GENERATOR TGZ productbuild)
elseif(CMAKE_SYSTEM_NAME STREQUAL "Linux")
    #set(CPACK_GENERATOR TGZ RPM)
    set(CPACK_GENERATOR TGZ)
else()
    set(CPACK_GENERATOR TGZ)
endif()
set(CPACK_SOURCE_IGNORE_FILES
  /\\.git/
  \\.swp
  \\.orig
  /CMakeLists\\.txt\\.user
  /private/
)
include(CPack) # Put this last!


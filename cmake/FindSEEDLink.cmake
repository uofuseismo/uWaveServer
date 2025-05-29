# Already in cache, be silent
if (SEEDLINK_INCLUDE_DIR AND SEEDLINK_LIBRARY)
    set(SEEDLINK_FIND_QUIETLY TRUE)
endif()

# Find the include directory
find_path(SEEDLINK_INCLUDE_DIR
          NAMES libslink.h
          HINTS $ENV{SEEDLINK_ROOT}/include
                $ENV{SEEDLINK_ROOT}/
                /usr/local/include
          )
# Find the library components
if (BUILD_SHARED_LIBS)
   message("Looking for libslink shared")
   find_library(SEEDLINK_LIBRARY
                NAMES libslink.so
                PATHS $ENV{SEEDLINK_ROOT}/lib/
                      $ENV{SEEDLINK_ROOT}/
                      /usr/local/lib64
                      /usr/local/lib
                )
else()
   message("Looking for libslink static")
   find_library(SEEDLINK_LIBRARY
                NAME libslink.a
                PATHS $ENV{SEEDLINK_ROOT}/lib/
                      $ENV{SEEDLINK_ROOT}/
                      /usr/local/lib64
                      /usr/local/lib
                ) 
endif()
# Handle the QUIETLY and REQUIRED arguments and set MKL_FOUND to TRUE if
# all listed variables are TRUE.
include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(SEEDLink
                                  DEFAULT_MSG SEEDLINK_INCLUDE_DIR SEEDLINK_LIBRARY)
mark_as_advanced(SEEDLINK_INCLUDE_DIR SEEDLINK_LIBRARY)

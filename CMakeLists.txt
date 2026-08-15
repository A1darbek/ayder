cmake_minimum_required(VERSION 3.18)

project(Ayder VERSION 1.0.0 LANGUAGES C)

set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)
set(CMAKE_C_EXTENSIONS ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

option(AYDER_ENABLE_NATIVE_OPTIMIZATION
       "Enable host-specific compiler instructions (non-portable artifact)" OFF)
option(AYDER_WARNINGS_AS_ERRORS "Treat compiler diagnostics as errors" OFF)

find_package(PkgConfig REQUIRED)
find_package(Threads REQUIRED)
pkg_check_modules(LIBUV REQUIRED IMPORTED_TARGET libuv)
pkg_check_modules(LIBURING REQUIRED IMPORTED_TARGET liburing)
pkg_check_modules(LIBCURL REQUIRED IMPORTED_TARGET libcurl)
pkg_check_modules(OPENSSL REQUIRED IMPORTED_TARGET openssl)
pkg_check_modules(ZLIB REQUIRED IMPORTED_TARGET zlib)

file(GLOB AYDER_SOURCES CONFIGURE_DEPENDS "${CMAKE_CURRENT_SOURCE_DIR}/src/*.c")

add_executable(ayder
    ${AYDER_SOURCES}
    deps/picohttpparser/picohttpparser.c
)

target_include_directories(ayder PRIVATE
    "${CMAKE_CURRENT_SOURCE_DIR}/include"
    "${CMAKE_CURRENT_SOURCE_DIR}/src"
    "${CMAKE_CURRENT_SOURCE_DIR}/deps/picohttpparser"
)

target_compile_definitions(ayder PRIVATE HTTP_SERVER_FAST NDEBUG)
target_compile_options(ayder PRIVATE
    -Wall
    -Wextra
    -Wshadow
    -Wdouble-promotion
    $<$<CONFIG:Release>:-O3>
)

if(AYDER_WARNINGS_AS_ERRORS)
    target_compile_options(ayder PRIVATE -Werror)
endif()

if(AYDER_ENABLE_NATIVE_OPTIMIZATION)
    if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang")
        target_compile_options(ayder PRIVATE -march=native)
    else()
        message(FATAL_ERROR "AYDER_ENABLE_NATIVE_OPTIMIZATION is unsupported by this compiler")
    endif()
endif()

target_link_libraries(ayder PRIVATE
    Threads::Threads
    PkgConfig::LIBUV
    PkgConfig::LIBURING
    PkgConfig::LIBCURL
    PkgConfig::OPENSSL
    PkgConfig::ZLIB
    m
)

install(TARGETS ayder RUNTIME DESTINATION bin)

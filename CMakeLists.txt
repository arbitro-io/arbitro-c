cmake_minimum_required(VERSION 3.20)

project(arbitro-c
    VERSION 0.1.1
    LANGUAGES C
    DESCRIPTION "Single-file C client for the Arbitro message broker"
)

set(CMAKE_C_STANDARD 99)
set(CMAKE_C_STANDARD_REQUIRED ON)
set(CMAKE_C_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

option(ARBITRO_BUILD_TESTS "Build unit and integration tests" ON)
option(ARBITRO_BUILD_EXAMPLES "Build examples" ON)
option(ARBITRO_BUILD_BENCHMARKS "Build benchmarks" ON)
option(ARBITRO_SANITIZERS "Enable ASan + UBSan" OFF)
option(ARBITRO_TLS "Enable TLS support (requires OpenSSL)" OFF)

# --- Library ---

add_library(arbitro STATIC src/arbitro.c)
target_include_directories(arbitro PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)

if(WIN32)
    target_link_libraries(arbitro PRIVATE ws2_32)
else()
    target_compile_definitions(arbitro PRIVATE _POSIX_C_SOURCE=200112L)
endif()

if(ARBITRO_TLS)
    find_package(OpenSSL REQUIRED)
    target_link_libraries(arbitro PRIVATE OpenSSL::SSL OpenSSL::Crypto)
    target_compile_definitions(arbitro PUBLIC ARBITRO_TLS=1)
endif()

target_compile_options(arbitro PRIVATE
    $<$<C_COMPILER_ID:GNU,Clang,AppleClang>:-Wall -Wextra -Wpedantic -Werror -Wno-unused-parameter>
    $<$<C_COMPILER_ID:MSVC>:/W4 /WX>
)

if(ARBITRO_SANITIZERS)
    target_compile_options(arbitro PUBLIC -fsanitize=address,undefined -fno-omit-frame-pointer)
    target_link_options(arbitro PUBLIC -fsanitize=address,undefined)
endif()

# --- Shared library (optional) ---

add_library(arbitro_shared SHARED src/arbitro.c)
target_include_directories(arbitro_shared PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)
set_target_properties(arbitro_shared PROPERTIES
    OUTPUT_NAME arbitro
    VERSION ${PROJECT_VERSION}
    SOVERSION 0
)
if(WIN32)
    target_link_libraries(arbitro_shared PRIVATE ws2_32)
endif()

# --- Tests ---

if(ARBITRO_BUILD_TESTS)
    enable_testing()

    add_executable(unit_tests tests/unit_tests.c)
    target_link_libraries(unit_tests PRIVATE arbitro)
    add_test(NAME unit_tests COMMAND unit_tests)

    add_executable(integration tests/integration.c)
    target_link_libraries(integration PRIVATE arbitro)

    add_executable(queue_integration tests/queue_integration.c)
    target_link_libraries(queue_integration PRIVATE arbitro)

    if(NOT WIN32)
        find_package(Threads REQUIRED)
        add_executable(service_integration tests/service_integration.c)
        target_link_libraries(service_integration PRIVATE arbitro Threads::Threads)
    endif()

    # tests/integration/ — one executable per file, each with its own main.
    # These existed but were never referenced here, so ~85 test cases had
    # never been compiled, let alone run. They all still build clean against
    # the current API; the only thing missing was the wiring.
    file(GLOB ARBITRO_PHASE_TESTS ${CMAKE_CURRENT_SOURCE_DIR}/tests/integration/*.c)
    foreach(phase_src ${ARBITRO_PHASE_TESTS})
        get_filename_component(phase_name ${phase_src} NAME_WE)
        add_executable(${phase_name} ${phase_src})
        target_include_directories(${phase_name} PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/tests)
        target_link_libraries(${phase_name} PRIVATE arbitro)
        add_test(NAME ${phase_name} COMMAND ${phase_name})
    endforeach()

    # C only warns when a function pointer of the wrong shape is passed, so a
    # handler written against an older signature compiles and then reads one
    # struct at another's offsets at runtime. That is exactly how the SEGV in
    # arb__replyto_decode got in. Make it a hard error everywhere tests are
    # built — a callback mismatch must never reach a broker again.
    # The library builds with -Wall -Wextra -Wpedantic -Werror; tests get the
    # same bar. A warning tolerated here is a bug the suite cannot see.
    set(ARBITRO_TEST_WARNINGS
        -Wall -Wextra -Werror -Wno-unused-parameter
        -Werror=incompatible-pointer-types
        -Werror=implicit-function-declaration)

    foreach(t IN LISTS ARBITRO_PHASE_TESTS)
        get_filename_component(tn ${t} NAME_WE)
        target_compile_options(${tn} PRIVATE ${ARBITRO_TEST_WARNINGS})
        # These tests call usleep(), which glibc hides under the strict C99
        # the library itself is built with.
        target_compile_definitions(${tn} PRIVATE _DEFAULT_SOURCE)
    endforeach()
    foreach(tn unit_tests integration queue_integration)
        target_compile_options(${tn} PRIVATE ${ARBITRO_TEST_WARNINGS})
    endforeach()
endif()

# --- Examples ---

if(ARBITRO_BUILD_EXAMPLES)
    add_executable(example_pubsub examples/pubsub.c)
    target_link_libraries(example_pubsub PRIVATE arbitro)

    add_executable(example_service examples/service.c)
    target_link_libraries(example_service PRIVATE arbitro)
endif()

# --- Benchmarks ---

if(ARBITRO_BUILD_BENCHMARKS)
    add_executable(bench_publish benchmarks/bench_publish.c)
    target_link_libraries(bench_publish PRIVATE arbitro)

    add_executable(bench_roundtrip benchmarks/bench_roundtrip.c)
    target_link_libraries(bench_roundtrip PRIVATE arbitro)

    add_executable(bench_limits benchmarks/bench_limits.c)
    target_link_libraries(bench_limits PRIVATE arbitro)
endif()

# --- Install ---

include(GNUInstallDirs)

install(TARGETS arbitro arbitro_shared
    EXPORT arbitro-targets
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
)

install(DIRECTORY include/arbitro
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
)

install(EXPORT arbitro-targets
    FILE arbitro-config.cmake
    NAMESPACE arbitro::
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/arbitro
)

configure_file(arbitro.pc.in arbitro.pc @ONLY)
install(FILES ${CMAKE_CURRENT_BINARY_DIR}/arbitro.pc
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/pkgconfig
)

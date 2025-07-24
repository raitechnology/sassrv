# Rai Sass RV

[![Copr build status](https://copr.fedorainfracloud.org/coprs/injinj/rel/package/sassrv/status_image/last_build.png)](https://copr.fedorainfracloud.org/coprs/injinj/rel/package/sassrv/)

A high-performance C++ implementation of SASS/RV messaging middleware with
enhanced features for market data distribution and real-time messaging. This
implementation provides API compatibility while offering improved performance,
and additional message formats.

## Features

- **SASS/RV Compatibility**: Drop-in replacement for SASS/RV APIs
- **Multiple Message Formats**: Support for RV, TibMsg, SASS, RWF, Mktfeed, and JSON
- **High Performance**: Optimized C++ implementation with low-latency message processing
- **Fault Tolerance**: Two FT (Fault Tolerance) mechanisms for high availability
- **Advanced Monitoring**: Subscription tracking using the RV subscription management
- **Market Data Support**: Specialized features for financial market data distribution
- **Dictionary Integration**: Support for SASS and CFile dictionary formats
- **Intraprocess Communication**: High-speed local messaging via process transport

## Libraries

### Core Libraries

#### libsassrv.a/.so - Core SASS RV Library
Main library providing the core RV implementation and event-driven architecture.

**Key Components:**
- `EvRvListen` - TCP listener for accepting RV connections
- `EvRvClient` - Client connection management and messaging
- `RvHost` / `RvHostDB` - Host database and network management
- `SubMgr` - Subscription management and routing
- `FtMember` / `FtMonitor` - Fault tolerance group management

#### librv5lib.a/.so - RV5 API Library
Legacy RV5 API compatibility layer.

**Key Components:**
- `rv_Session` - RV session management
- `rv_Listener` - Message listener interface
- `rv_Timer` / `rv_Signal` - Timer and signal handling
- Message creation and manipulation functions

#### librv7lib.a/.so - RV7/TibRV API Library
RV7 compatible API implementation.

**Key Components:**
- `tibrvMsg` - TibRV message handling
- `tibrvEvent` - Event and listener management
- `tibrvTransport` - Transport creation and management
- `tibrvQueue` - Message queue and dispatching
- Full TibRV field type support (strings, arrays, dates, etc.)

#### librv7ftlib.a/.so - RV7 Fault Tolerance Library
Fault tolerance extensions for RV7 API.

**Key Components:**
- `tibrvftMember` - FT group member management
- `tibrvftMonitor` - FT group monitoring
- Automatic failover and weight-based priority

## Programs

### Core Services

#### rv_server
**Purpose**: RV daemon server/listener  
**Usage**: `rv_server -r <port>`  
**Description**: Acts as the central RV daemon, listening for client connections and managing RV network topology. Handles host registration, service discovery, and message routing between clients.  This does not contain a multicast transport, that is a part of the Rai MS (Multicast Services) daemon.

#### rv_client  
**Purpose**: Advanced RV data subscriber/client  
**Usage**: `rv_client -d daemon -s service subject1 subject2...`  
**Description**: High-performance RV client for subscribing to subjects and receiving market data. Features include dictionary loading, latency tracking, rate monitoring, and statistical analysis.

**Key Features:**
- Multiple dictionary formats (SASS, CFile)
- Snapshot and live subscription modes
- Message latency and timing statistics
- Quiet mode and hex dump capabilities

#### rv_pub
**Purpose**: RV data publisher  
**Usage**: `rv_pub -d daemon -m rate -k count -p times subject1 subject2...`  
**Description**: Publishes test market data messages to RV subjects with configurable rates and formats.

**Key Features:**
- Multiple message formats (JSON, RV, TibMsg, SASS)
- Rate limiting and burst control
- Custom payload and record type support
- Random subject selection patterns

### API Test Programs

#### api_client
**Purpose**: RV7 C API demonstration client  
**Usage**: `api_client [-service service] [-network network] [-daemon daemon] subject_list`  
**Description**: Demonstrates TibRV-compatible API usage for subscribing to subjects with automatic dictionary handling and snapshot requests.

#### rv5_api_test
**Purpose**: RV5 C API test program  
**Usage**: `rv5_api_test [-service service] [-network network] [-daemon daemon] [-subject subjects]`  
**Description**: Simple test demonstrating RV5 C API functionality including session creation, message listening, and callback handling.

#### rv5_cpp_test
**Purpose**: RV5 C++ API test program  
**Usage**: `rv5_cpp_test [same options as rv5_api_test]`  
**Description**: C++ wrapper test for RV5 API using object-oriented callback handling.

#### rv7_test
**Purpose**: RV7 message format test  
**Usage**: `rv7_test`  
**Description**: Test of RV message creation, manipulation, and serialization. Tests all field types, message arrays, and format validation.

#### subrv7test
**Purpose**: Advanced subscription test with Rai MD integration  
**Usage**: `subrv7test [-service service] [-network network] [-daemon daemon] subject_list`  
**Description**: Advanced subscription client that integrates RV7 API with MD (Market Data) library for complex message format handling.

#### intra_test (intraprocess)
**Purpose**: Intraprocess communication test  
**Usage**: `intra_test`  
**Description**: Tests high-speed local messaging using `TIBRV_PROCESS_TRANSPORT` with multi-threaded publisher/subscriber pattern.

### Monitoring and Management

#### rv_subtop
**Purpose**: Subscription topology monitor  
**Usage**: `rv_subtop -d daemon -t subject_wildcards...`  
**Description**: Real-time monitoring of RV subscription topology, showing active hosts, sessions, and subscription reference counts.

**Key Features:**
- Live topology display
- Session state tracking
- Subscription reference counting
- "Top"-style continuous updates

#### rv_ftmon
**Purpose**: Fault tolerance monitor  
**Usage**: `rv_ftmon -d daemon -c cluster -w weight`  
**Description**: Monitors and participates in RV fault tolerance groups, displaying real-time FT status and handling failover events.

**Key Features:**
- FT group participation
- Weight-based priority management
- Primary/secondary/join state monitoring
- Automatic failover handling

## Go Language Support

The `golang/` directory contains a Go implementation that provides CGO bindings to the C libraries:

- **subrv7test.go**: Go equivalent of the C subrv7test program
- **Full API Compatibility**: Uses CGO to link with existing C libraries
- **Native Go Interface**: Command-line parsing with Go's flag package
- **Message Callbacks**: Go implementations of C callback functions

See [`golang/README.md`](golang/README.md) for detailed Go usage instructions.

## Building

### Dependencies
- C++11 compatible compiler
- [raimd](https://github.com/raitechnology/raimd) - Message decoding library
- [raikv](https://github.com/raitechnology/raikv) - Key-value and networking library  
- [libdecnumber](https://github.com/raitechnology/libdecnumber) - Decimal number library
- pcre2-8, libcares, pthread, rt libraries

### Build Commands
```bash
# Build everything (libraries and programs)
make

# Debug build
make port_extra=-g

# Clean build
make clean
```
# Make rpm
make dist_rpm
```

### Running Tests
```bash
# Run the C RV7 test
./<os>_x86_64/bin/rv7_test

# Run subscription test
./<os>_x86_64/bin/subrv7test SUBJECT.TEST

# Run Go version
cd golang && ./subrv7test SUBJECT.TEST
```

## API Overview

### RV7/TibRV API (librv7lib)
Compatible with RV7 API:
```c
// Transport and session management
tibrvTransport_Create(&transport, service, network, daemon);
tibrvEvent_CreateListener(&event, queue, callback, transport, subject, closure);

// Message handling
tibrvMsg_Create(&msg);
tibrvMsg_SetSendSubject(msg, subject);
tibrvMsg_AddString(msg, "field", value);
tibrvTransport_Send(transport, msg);
```

### Native C++ API (libsassrv)
High-performance native C++ interface:
```cpp
// Event-driven client
EvRvClient client(poll, params);
client.connect();
client.subscribe(subject, callback);

// Publishing
client.publish(subject, msg_data, msg_len);
```

### RV5 Legacy API (librv5lib)
Legacy RV5 compatibility:
```c
rv_Session session;
rv_Init(&session, network, daemon);
rv_Listen(&session, subject, callback, closure);
```

## Configuration

### Network Parameters
- **Service**: Port number (default: 7500)
- **Network**: Multicast address (default: system-dependent)
- **Daemon**: Daemon address (default: tcp:7500)

### Message Formats
- **RV**: Native RV binary format
- **TibMsg**: TibMsg market data format
- **SASS**: SASS market data format
- **JSON**: Human-readable JSON format
- **RWF**: Reuters Wire Format
- **Marketfeed**: Reuters marktetfeed format

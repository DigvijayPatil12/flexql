# FlexQL: A Flexible SQL-like Database Driver

FlexQL is a custom-built, high-concurrency relational database engine written in C++14. It is designed to handle massive datasets (up to 20M+ rows) with a focus on low-latency insertion and efficient memory management.

## 🚀 Key Features
- **Async Disk I/O**: Decoupled RAM updates from physical disk writes using a background worker thread with backpressure support.
- **Multithreaded Architecture**: Fixed-size Thread Pool for connection handling and `shared_timed_mutex` for concurrent Read/Write operations.
- **Advanced Caching**: LRU (Least Recently Used) cache to accelerate frequent queries.
- **O(1) Indexing**: Primary Hash Index using `std::unordered_map` for instant record lookups.
- **Memory Purging**: Background maintenance thread for automatic purging of expired TTL records.

## 💻 Supported SQL Capabilities
FlexQL enforces strict typing (`INT`, `DECIMAL`, `VARCHAR`, `DATETIME`) and focuses on a streamlined, high-performance subset of relational operations:

* **Data Definition (DDL):** Dynamic table creation with strongly typed schemas.
* **Data Manipulation (DML):** High-throughput row insertions with support for mandatory expiration timestamps (TTL).
* **Querying & Projection:** Fetch entire tables or project specific columns to minimize memory overhead.
* **Filtering:** Single-condition `WHERE` clause evaluation.
* **Relational Joins:** `INNER JOIN` operations with dynamic `ON` clause parsing and optional post-join filtering.

## 🛠️ System Requirements
- **OS**: Ubuntu 20.04+ (or any modern Linux distro)
- **Compiler**: `g++` (supporting C++14)
- **Libraries**: POSIX Threads (`pthread`), Standard Template Library (STL)

## 📥 Installation
Clone the repository and navigate into the project directory:
```bash
git clone https://github.com/DigvijayPatil12/flexql.git
cd flexql
```

## 🏗️ Compilation Instructions
From the root directory, run the following commands to build the system:

### 1. Compile Server
```bash
g++ -std=c++14 -pthread -O3 -I./include src/server/server.cpp -o bin/flexql-server
```

### 2. Compile Interactive Client (REPL)
```bash
g++ -std=c++14 -I./include src/client/flexql_client.cpp src/client/main.cpp -o bin/flexql-client
```

### 3. Compile Scalability Benchmark
```bash
g++ -std=c++14 -O3 -I./include src/client/flexql_client.cpp tests/benchmark_flexql.cpp -o bin/run-benchmark
```

## 🏃 Execution Guide

Open multiple terminal windows to run the server and clients concurrently.

**1. Start the Server:** 
```bash
./bin/flexql-server
```

**2. Run Interactive SQL (in a new terminal):** 
```bash
./bin/flexql-client
```

**3. Run the Benchmark (in a new terminal):**
You can specify the exact number of rows to insert (e.g., 10 Million):
```bash
./bin/run-benchmark 10000000
```

**4. Run Unit Tests:**
To verify the internal data structures and SQL operations without running the massive benchmark:
```bash
./bin/run-benchmark --unit-test
```

## 📊 Performance
FlexQL achieves a sustained throughput of **~500,000 rows/second** on standard hardware by utilizing 5000-row batching and asynchronous persistence. Detailed performance metrics, cache hit ratios, and scalability plots are available in the project design document.

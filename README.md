# Curvine

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Rust](https://img.shields.io/badge/Rust-1.80%2B-orange)](https://www.rust-lang.org)

**Curvine** is a high-performance, concurrent distributed cache system written in Rust, designed for low-latency and high-throughput workloads.

## 📜 License
Curvine is licensed under the ​**​[Apache License 2.0](LICENSE)​**.

## 📚 Documentation Resources

For more detailed information, please refer to:

- [Official Documentation](https://curvineio.github.io/docs/Overview/instroduction)
- [Quick Start](https://curvineio.github.io/docs/Deploy/quick-start)
- [User Manuals](https://curvineio.github.io/docs/category/user-manuals)
- [Benchmark](https://curvineio.github.io/docs/category/benchmark)


## 🧩 Modular Architecture
Curvine adopts a modular design and is mainly composed of the following core components：

- **orpc**: A high-performance network communication framework that supports asynchronous RPC calls.
- **curvine-common**: A shared library containing protocol definitions, error handling, and common utilities.
- **curvine-server**: A server component that includes Master and Worker implementations.
- **curvine-client**: A client library that provides APIs for interacting with the server.
- **curvine-fuse**: A FUSE file system interface that allows Curvine to be mounted as a local file system.
- **curvine-libsdk**: An SDK library that supports multi - language access.
- **curvine-web**: A web management interface and API.
- **curvine-tests**: A testing framework and performance benchmarking tool.


## 🚀 Core Features

- **High-performance RPC Framework**: An asynchronous communication framework based on Tokio, supporting high-concurrency request processing.
- **Distributed Architecture**: A Master - Worker architecture design that supports horizontal scaling.
- **Multi-level Cache**: Supports multi - level cache strategies for memory, SSD, and HDD.
- **FUSE Interface**: Provides a FUSE file system interface for seamless integration into existing systems.
- **Underlying Storage Integration**: Supports integration with multiple underlying storage systems.
- **Raft Consensus**: Uses the Raft algorithm to ensure data consistency and high availability.
- **Monitoring and Metrics**: Built - in monitoring and performance metric collection.
- **Web Interface**: Provides a web management interface for convenient system monitoring and management.

## 📦 System Requirements

- Rust 1.80+
- Linux or macOS (Limited support on Windows)
- FUSE library (for file system functionality)

## ⚙️ Installation and Configuration

Build from source:
```bash
# Compiled files are in build/dist
sh build/build.sh
```

Start a single - node cluster:
```bash
cd build/dist

# Start the master node
bin/curvine-master.sh start

# Start the worker node
bin/curvine-worker.sh start
```

Mount the file system:
```bash
# The default mount point is /curvine-fuse
bin/curvine-fuse.sh start
```

View the cluster overview:
```bash
bin/curvine report
```

Access the file system using compatible HDFS commands:
```bash
bin/curvine fs -mkdir /a
bin/curvine fs -ls /
```

Curvine uses TOML - formatted configuration files. An example configuration is located at conf/curvine-cluster.toml. The main configuration items include:

- Network settings (ports, addresses, etc.)
- Storage policies (cache size, storage type)
- Cluster configuration (number of nodes, replication factor)
- Performance tuning parameters

## 🏗️ Architecture Design

Curvine adopts a master-slave architecture:

- **Master Node**: Responsible for metadata management, worker node coordination, and load balancing.
- **Worker Node**: Responsible for data storage and processing.
- **Client**: Communicates with the Master and Worker nodes via RPC.

The system uses the Raft consensus algorithm to ensure metadata consistency and supports multiple storage strategies (memory, SSD, HDD) to optimize performance and cost.

## 📈 Performance

Curvine performs excellently in high-concurrency scenarios and supports:

- High-throughput data read and write
- Low-latency operations
- Large-scale concurrent connections
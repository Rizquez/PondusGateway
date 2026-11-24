# Pondus Gateway - v0.7.0

Pondus Gateway is an experimental engine for the admission, scheduling and concurrent execution of operations.

It allows researchers to study how different scheduling strategies manage workloads with limited capacity, variable costs and multiple clients.

The project is being developed according to three priorities, in this order:

1. **Overload protection:** respecting capacity, concurrency and queue limits.
2. **Resource utilization:** using available capacity efficiently.
3. **Starvation reduction:** preventing pending operations from being indefinitely preempted.

## 🚀 Quick start

### Direct execution

```bash
go run ./cmd/pondus -concurrency=4 -capacity=10 -operations=20 -duration=100ms
```

This mode generates operations with a unit cost using FIFO.

### Execution with YAML

```bash
go run ./cmd/pondus -client clients/basic.yaml
```

Allows you to configure admission limits, the executor, the strategy and a workload consisting of operations or multiple clients.

### Strategy comparison

```bash
go run ./cmd/pondus -client clients/multiple.yaml -compare
```

Runs the same workload using FIFO, Capacity-Fit and Capacity-Fit with starvation protection, using separate controllers.

### TCP server and client

Start the server in one terminal:

```bash
go run ./cmd/pondus-server -addr=127.0.0.1:9000 -concurrency=4 -capacity=10 -queue=20 -scheduler=capacity-fit -duration=100ms
```

Then send a workload from another terminal:

```bash
go run ./cmd/pondus-client -addr=127.0.0.1:9000 -file=clients/request.json
```

The server applies the configured admission limits and scheduler and the client receives the results of the submitted workload.

### Continuous TCP server and client

Start the persistent TCP server in one terminal:

```bash
go run ./cmd/pondus-stream-server -addr=127.0.0.1:9001 -concurrency=4 -capacity=10 -queue=20 -scheduler=capacity-fit -duration=100ms
```

In a second terminal, send multiple operations over one connection:

```bash
go run ./cmd/pondus-stream-client -addr=127.0.0.1:9001 -file=clients/stream-requests.jsonl -window=4 -timeout=30s
```

The input file uses **JSONL / NDJSON**: each line must contain one complete `submit` JSON object. The client receives admission decisions and terminal results, which may be interleaved across requests. The `-window` flag limits client requests without terminal responses; it is separate from the server's execution concurrency limit.

The v0.6.0 TCP commands above remain available for finite batches. The v0.7.0 streaming service uses a shared Controller and budget across connections.

### Makefile

| Command | Description |
| --- | --- |
| `make test` | Runs all tests 20 times, race tests 5 times and `go vet`. |
| `make build` | Builds all Go packages. |
| `make run-flags` | Direct execution with command-line flags. |
| `make run-basic` | Runs the `clients/basic.yaml` scenario. |
| `make run-multiple` | Runs the `clients/multiple.yaml` scenario. |
| `make run-compare` | Compares all strategies using `clients/multiple.yaml`. |
| `make run-server` | Starts the v0.6.0 batch TCP server on `127.0.0.1:9000`. |
| `make run-client` | Sends `clients/request.json` to the batch server. |
| `make run-stream-server` | Starts the v0.7.0 continuous TCP server on `127.0.0.1:9001`. |
| `make run-stream-client` | Sends `clients/stream-requests.jsonl` to the continuous server. |

Run `make run-stream-server` and `make run-stream-client` in separate terminals.

## ⚙️ Features

The engine provides:

- **Admission control:** configurable limits for capacity, concurrency and pending operations.
- **Scheduling:** FIFO, Capacity-Fit and Capacity-Fit with starvation protection.
- **Concurrent execution:** Runner with result management, cooperative cancellation and error recovery.
- **Isolation:** exclusive sessions to protect the Controller during batch execution.
- **Multi-client simulation:** operations with different costs, defined using YAML.
- **Observability:** metrics for results, duration and estimated capacity utilization.
- **Batch TCP transport:** one finite workload per connection.
- **Continuous TCP service:** dynamic admission from multiple connections with globally shared limits and independent results.
- **Owner cancellation:** disconnected clients cancel their own work; active reservations remain held until execution returns.

### Available strategies

| Strategy | Behavior |
| --- | --- |
| FIFO | Respects the order of pending operations. |
| Capacity-Fit | Selects the first pending operation that fits within the available capacity. |
| Capacity-Fit + StarvationGuard | Limits the jumping ahead of pending operations to reduce the risk of starvation. |

> [!IMPORTANT]
> The cost of an operation is measured in logical capacity units, not actual CPU or memory usage.

## 📊 Metrics

The simulations report the number of completed, failed, canceled and rejected operations, as well as the total duration of the batch.

They also provide the observed peak reserved capacity and its estimated average utilization.

These metrics are calculated through periodic sampling. They may omit brief fluctuations in capacity and do not directly represent the actual use of system resources.

## 🚧 Roadmap

| Version | Phase | Objective |
| --- | --- | --- |
| v0.1.0 | Fundamentals | Define the engine’s basic operations, states and contracts. |
| v0.2.0 | Capacity control | Implement concurrency, capacity and queue limits to protect against overload. |
| v0.3.0 | Scheduling | Implement FIFO and Capacity-Fit selection strategies and starvation protection. |
| v0.4.0 | Execution | Execute operations concurrently and manage their lifecycles. |
| v0.5.0 | MVP | Integrate the engine, simulate multi-client workloads, compare strategies, measure results and validate its behavior. |
| v0.6.0 | TCP communication | Implement a TCP server and a test client to send workloads and receive results. |
| v0.7.0 | Concurrent service | Receive and execute operations continuously from multiple TCP clients with a shared global capacity budget. 📍 |
| v0.8.0 | Advanced overload control | Incorporate appropriate limits and admission policies for concurrent clients, saturation and disconnections. |
| v0.9.0 | Cost estimation | Incorporate mechanisms to estimate the cost of operations and use those estimates in admission. |
| v0.10.0 | Advanced scheduling | Evaluate and improve fairness, starvation protection and capacity utilization under heterogeneous loads. |
| v0.11.0 | Observability | Incorporate more accurate execution and capacity metrics, along with an interface for external querying. |
| v0.12.0 | Performance and robustness | Conduct reproducible benchmarks, load testing, recovery testing and optimization of critical components. |
| v0.13.0 | API stabilization | Define a reusable public API, document contracts and consolidate compatibility and configuration. |
| v1.0.0 | First stable release | Release a documented, tested version with stable contracts for explicitly supported use cases. |

> [!NOTE]
> 📍 Currently in the research and development phase.

## ⚠️ Limitations

- **Finite-batch Runner:** `Runner.Run()` does not admit new operations during a running batch. Continuous admission uses the separate v0.7.0 Coordinator.
- **Cooperative cancellation:** executors must respond to context cancellation. Capacity remains reserved until an active executor actually returns; cancellation cannot forcibly stop a blocked executor.
- **Persistence:** operations and reservations are in memory; process crashes do not provide durable recovery.
- **Initial batch admission:** YAML and batch-mode scenarios are subject to their initial admission constraints.
- **Starvation:** StarvationGuard limits bypasses but does not guarantee a maximum waiting time.
- **Metrics accuracy:** sampled utilization may miss short spikes and does not measure actual CPU or memory consumption.
- **TCP half-close:** a client `CloseWrite()` is treated as disconnection and cancels that connection's operations; clients must keep the write side open until terminal responses arrive.
- **Streaming connection limits:** the continuous server defaults to `-read-timeout=0` to allow long-running requests without read inactivity cancellation. Idle connections can therefore occupy connection slots; use appropriate network controls before exposing the service to untrusted networks.
- **Backpressure:** the streaming server bounds its outgoing response queue; a client that does not read may have its connection closed and its work cancelled.
- **Streaming client:** reads a finite JSONL file, rather than supporting indefinite interactive submission. `-timeout` applies to the whole client exchange.
- **Security and operational maturity:** no built-in durable result recovery or authenticated multi-tenant ownership; load and endurance testing remain future roadmap work.

## 🔒 License

Pondus Gateway is distributed under the GNU General Public License (GPL), in accordance with the terms of the `LICENSE` file.

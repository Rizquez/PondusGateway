# Pondus Gateway - v0.6.0

Pondus Gateway is an experimental engine for the admission, scheduling and concurrent execution of operations.

It allows researchers to study how different scheduling strategies manage workloads with limited capacity, variable costs and multiple clients.

The project is being developed according to three priorities, in this order:

1. **Overload protection:** respecting capacity, concurrency and queue limits.
2. **Resource utilization:** using available capacity efficiently.
3. **Starvation reduction:** preventing pending operations from being indefinitely preempted.

## 🚀 Quick start

### Direct Execution

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

The server applies the configured admission limits and scheduler, and the client receives the results of the submitted workload.

### Makefile

| Command | Description |
| --- | --- |
| `make test` | Runs the tests 20 times, the tests with the race detector 5 times, and `go vet`. |
| `make run-flags` | Direct execution with command-line flags. |
| `make run-basic` | Runs the `clients/basic.yaml` scenario. |
| `make run-multiple` | Runs the `clients/multiple.yaml` scenario. |
| `make run-compare` | Compares all strategies using `clients/multiple.yaml`. |
| `make run-server` | Starts the TCP server on `127.0.0.1:9000`. |
| `make run-client` | Sends `clients/request.json` to the server. |

## ⚙️ Features

MVP v0.5.0 includes:

- **Admission control:** configurable limits for capacity, concurrency and pending operations.
- **Scheduling:** FIFO, Capacity-Fit and Capacity-Fit with starvation protection.
- **Concurrent execution:** Runner with result management, cooperative cancellation and error recovery.
- **Isolation:** exclusive sessions to protect the Controller during batch execution.
- **Multi-client simulation:** operations with different costs, defined using YAML.
- **Observability:** metrics for results, duration and estimated capacity utilization.

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
| v0.7.0 | Concurrent service | Evolve from isolated batch executions to the continuous reception of operations from multiple clients. 📍 |
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

- **Batch execution:** Does not support the continuous addition of operations while a Runner is executing its batch.
- **Cooperative cancellation:** Runners must respect context. A blocked runner may prevent Run() from completing, even after a timeout.
- **Persistence:** Operations and reservations are held in memory, with no transactional recovery in the event of process crashes.
- **Initial admission:** YAML scenarios must be designed to initially admit all operations, without considering future capacity releases.
- **Starvation:** StarvationGuard limits preemptions but does not guarantee a maximum wait time.
- **Metrics accuracy:** Sampling may omit brief spikes and performance comparisons may vary across runs.
- **Interfaces:** The MVP does not expose network services and the YAML configuration only allows selecting the sleep executor.
- **TCP disconnects:** The server interprets the closure of the write channel (`CloseWrite()`) as a cancellation request, just like a full disconnect. The client must keep the connection open until it receives a response. Cancellation is cooperative and resources remain reserved until execution completes.

## 🔒 License

Pondus Gateway is distributed under the GNU General Public License (GPL), in accordance with the terms of the `LICENSE` file.

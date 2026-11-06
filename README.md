# Pondus Gateway - v0.5.0

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
go run ./cmd/pondus -client clients/basic.yaml -compare
```

Runs the same workload using FIFO, Capacity-Fit and Capacity-Fit with starvation protection, using separate controllers.

### Makefile

```bash
make default         # Default simulation
make basic-client    # Basic YAML configuration
make multi-client    # Multi-client simulation
make compare         # Strategy comparison
make unit-tests      # Tests
```

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
| v0.6.0 ** | TCP communication | Implement a TCP server and a test client to send workloads and receive results. |
| v0.7.0 | Concurrent service | Evolve from isolated batch executions to the continuous reception of operations from multiple clients. |
| v0.8.0 | Advanced overload control | Incorporate appropriate limits and admission policies for concurrent clients, saturation and disconnections. |
| v0.9.0 | Cost estimation | Incorporate mechanisms to estimate the cost of operations and use those estimates in admission. |
| v0.10.0 | Advanced scheduling | Evaluate and improve fairness, starvation protection and capacity utilization under heterogeneous loads. |
| v0.11.0 | Observability | Incorporate more accurate execution and capacity metrics, along with an interface for external querying. |
| v0.12.0 | Performance and robustness | Conduct reproducible benchmarks, load testing, recovery testing and optimization of critical components. |
| v0.13.0 | API stabilization | Define a reusable public API, document contracts and consolidate compatibility and configuration. |
| v1.0.0 | First stable release | Release a documented, tested version with stable contracts for explicitly supported use cases. |

> [!NOTE]
> ** Currently in the research and development phase.

## 📂 Project structure

```text
pondus-gateway
├── clients
│   ├── basic.yaml
│   └── multiple.yaml
├── cmd
│   ├── pondus                      # Command-Line interface, configuration and simulations
│   │   ├── compare.go
│   │   ├── config.go
│   │   ├── main.go
│   │   ├── measure.go
│   │   └── pondus_test.go
│   └── .gitkeep
├── internal
│   ├── admission                   # Admission, queue, promotions and exclusive sessions
│   │   ├── admission_test.go
│   │   ├── controller.go
│   │   └── session.go
│   ├── budget                      # Capacity reservations and releases
│   │   ├── capacity_test.go
│   │   └── capacity.go
│   ├── core                        # Operations, states, lifecycles and limits
│   │   ├── core_test.go
│   │   ├── lifecycle.go
│   │   ├── limits.go
│   │   ├── operation.go
│   │   └── state.go
│   ├── execution                   # Concurrent execution, results and recovery
│   │   ├── cpu.go
│   │   ├── execution_test.go
│   │   ├── executor.go
│   │   ├── failing.go
│   │   ├── runner.go
│   │   └── sleep.go
│   ├── observability               # Results summary and metrics calculation
│   │   ├── metrics_test.go
│   │   └── metrics.go
│   └── scheduler                   # Strategies for selecting pending operations
│       ├── capacity_fit.go
│       ├── fifo.go
│       ├── scheduler_test.go
│       └── starvation_guard.go
├── .gitignore
├── go.mod
├── go.sum
├── LICENSE
├── Makefile
└── README.md
```

## ⚠️ Limitations

- **Batch execution:** Does not support the continuous addition of operations while a Runner is executing its batch.
- **Cooperative cancellation:** Runners must respect context. A blocked runner may prevent Run() from completing, even after a timeout.
- **Persistence:** Operations and reservations are held in memory, with no transactional recovery in the event of process crashes.
- **Initial admission:** YAML scenarios must be designed to initially admit all operations, without considering future capacity releases.
- **Starvation:** StarvationGuard limits preemptions but does not guarantee a maximum wait time.
- **Metrics accuracy:** Sampling may omit brief spikes and performance comparisons may vary across runs.
- **Interfaces:** The MVP does not expose network services and the YAML configuration only allows selecting the sleep executor.

## 🔒 License

Pondus Gateway is distributed under the GNU General Public License (GPL), in accordance with the terms of the `LICENSE` file.

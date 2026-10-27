# PondusGateway - v0.5.0

Pondus Gateway is a concurrent workload admission and scheduling engine written in Go.

It explores how different scheduling strategies behave under limited execution capacity, variable operation costs, and multiple clients.

## 📍 Goals

Pondus Gateway focuses on three priorities:

1. Protect the system against overload.
2. Improve utilization of available capacity.
3. Reduce starvation among pending operations.

## 🛠️ Features

- Concurrent operation execution.
- Configurable concurrency and capacity limits.
- Admission control with a bounded queue.
- FIFO and Capacity-Fit scheduling.
- Starvation protection through bounded bypasses.
- Context cancellation and per-operation timeouts.
- Operation lifecycle tracking.
- YAML-based workloads with multiple clients.
- Execution metrics and scheduler comparisons.

## 🚀 Quick start

Run the default simulation:

```bash
make flags
```

Run a YAML configuration:

```bash
make basic-client
```

Run a multi-client workload:

```bash
make multi-client
```

Compare all scheduling strategies:

```bash
make compare
```

Each operation reserves capacity according to its cost.

The configured SleepExecutor simulates execution duration.

> [!NOTE]
> Operation cost does not represent CPU or memory consumption.

### Run tests

```bash
make unit-tests
```

## Scheduling strategies

### FIFO

Selects the oldest pending operation when sufficient capacity is available.

### Capacity-Fit

Selects the first pending operation that fits within the currently available capacity.

### Capacity-Fit with starvation protection

Limits how many operations can bypass the oldest pending operation before requiring it to run.

This may reduce short-term capacity utilization.

## Metrics

The CLI reports:

- Completed, failed, cancelled and rejected operations.
- Total batch duration.
- Maximum observed reserved capacity.
- Estimated average reserved-capacity utilization.

Capacity utilization is sampled periodically. 

> [!IMPORTANT]
> It does not represent actual CPU or memory utilization and short peaks can be missed.

## ⚠️ Limitations

Pondus Gateway is an experimental workload engine.

- Workloads are submitted as finite batches.
- It does not expose an HTTP or gRPC service.
- Cancellation depends on executor cooperation.
- Starvation protection does not guarantee a maximum waiting time.
- Capacity measurements are sampling-based estimates.
- The runner does not provide durable persistence or transactional crash recovery.
- Results depend on workload composition and runtime scheduling.

## 📂 Project structure

```
pondus-gateway
├── cmd
│   ├── pondus
│   │   ├── compare_test.go
│   │   ├── compare.go
│   │   ├── config_test.go
│   │   ├── config.go
│   │   ├── main_tes.go
│   │   ├── main.go
│   │   ├── measure.go
│   │   └── workload_test.go
│   └── .gitkeep
├── configs
│   ├── basic.yaml
│   └── multi-client.yaml
├── internal
│   ├── admission
│   │   ├── controller_comparison_test.go
│   │   ├── controller_stress_test.go
│   │   ├── controller_test.go
│   │   └── controller.go
│   ├── budget
│   │   ├── capacity_edge_test.go
│   │   ├── capacity_test.go
│   │   └── capacity.go
│   ├── core
│   │   ├── lifecycle_test.go
│   │   ├── lifecycle.go
│   │   ├── limits_test.go
│   │   ├── limits.go
│   │   ├── operation_test.go
│   │   ├── operation.go
│   │   ├── state_test.go
│   │   └── state.go
│   ├── execution
│   │   ├── cpu.go
│   │   ├── executor_test.go
│   │   ├── executor.go
│   │   ├── failing.go
│   │   ├── runner_cancellation_test.go
│   │   ├── runner_cleanup_test.go
│   │   ├── runner_exclusivity_test.go
│   │   ├── runner_lifecycle_test.go
│   │   ├── runner_progress_test.go
│   │   ├── runner_test.go
│   │   ├── runner.go
│   │   └── sleep.go
│   ├── observability
│   │   ├── metrics_test.go
│   │   └── metrics.go
│   └── scheduler
│       ├── capacity_fit_test.go
│       ├── capacity_fit.go
│       ├── fifo_test.go
│       ├── fifo.go
│       ├── starvation_guard_test.go
│       └── starvation_guard.go
├── .gitignore
├── go.mod
├── go.sum
├── LICENSE
├── Makefile
└── README.md
```

## 🚧 Roadmap

| Version   | Phase            | Objective                                               |
|-----------|------------------|---------------------------------------------------------|
| v0.1.0    | Fundamentals     | Define and represent operations                         |
| v0.2.0    | Capacity Control | Protect the system from overload                        |
| v0.3.0    | Scheduling       | Decide which operations to execute                      |
| v0.4.0    | Execution        | Execute operations concurrently                         |
| v0.5.0    | MVP              | Simulate workloads, measure results, and use the engine |

> [!IMPORTANT]
> **MVP:** A program written in `Go` capable of receiving requests from multiple clients, monitoring available capacity, scheduling their execution, preventing the system from exceeding configured limits, and displaying measurable results.

### After the MVP

| Version   | Possible developments                      |
|-----------|--------------------------------------------|
| v0.6.0    | TCP server and test client                 |
| v0.7.0    | Dynamic cost estimation                    |
| v0.8.0    | Advanced planning algorithms               |
| v0.9.0    | Observability and external metrics         |
| v1.0.0    | First stable version as a reusable library |

## 🔒 License

This project is licensed under the MIT license, which allows its use, distribution, and modification under the conditions specified in the `LICENSE` file.

## 👨‍💻 Autor

- Pedro Rizquez: pedro.rizquez.94@hotmail.com

# Pondus Gateway - v0.5.0

Pondus Gateway is an experimental engine for the admission, scheduling and concurrent execution of operations, developed in Go.

Its goal is to study the behavior of different scheduling strategies under constraints such as capacity, concurrency, variable costs per operation and multiple clients.

The project allows users to simulate workloads, manage capacity reserves, execute operations concurrently and compare the results obtained using different strategies.

## 📍 Objectives

Pondus Gateway establishes three priorities, in the following order:

- Protection against overload: Prevent accepted operations from exceeding the configured limits for capacity, concurrency and queuing.
- Resource utilization: Select operations that can be executed within the available capacity.
- Starvation reduction: Limit the preemption of pending operations to reduce the risk of indefinite waiting.

These priorities guide the engine's design and its planning strategies.

## 🚧 Roadmap

| Version | Phase | Objective |
|---|---|---|
| v0.1.0 | Fundamentals | Define and represent operations |
| v0.2.0 | Capacity Control | Protect the system from overload |
| v0.3.0 | Scheduling | Decide which operations to execute |
| v0.4.0 | Execution | Execute operations concurrently |
| v0.5.0 ** | MVP | Simulate workloads, measure results and use the engine |

### After the MVP

| Version | Possible developments |
|---|---|
| v0.6.0 | TCP server and test client |
| v0.7.0 | Dynamic cost estimation |
| v0.8.0 | Advanced planning algorithms |
| v0.9.0 | Observability and external metrics |
| v1.0.0 | First stable version as a reusable library |

> [!NOTE]
> ** Phase currently in development

## 🚀 Quick start

The CLI supports three execution modes.

### 1. Direct Mode

This mode allows you to configure the simulation using the following arguments:

| Argument | Description |
| --- | --- |
| -concurrency | Maximum number of concurrent operations |
| -capacity | Maximum available capacity |
| -operations | Number of operations to be generated |
| -duration | Simulated duration of each operation |

Example:

```bash
go run ./cmd/pondus -concurrency=4 -capacity=10 -operations=20 -duration=100ms
```

This mode uses FIFO, generates unit-cost transactions and configures the queue capacity based on the number of requested transactions.

### 2. YAML Mode

Allows you to load a configuration from a file:

```bash
go run ./cmd/pondus -client clients/basic.yaml
```

The configuration defines admission limits, operations or clients, the executor, and the scheduling strategy.

A workload defined using `operations.count` or a list of clients is supported, but not both simultaneously.

Multi-client operations are generated in rounds, following the order of the configured clients.

### 3. Comparison Mode

Runs the same batch of operations using the three available strategies:

```bash
go run ./cmd/pondus -client clients/basic.yaml -compare
```

Each strategy uses a separate Controller and the same admission limits.

The results are presented as execution and capacity utilization metrics.

> [!IMPORTANT]
> The cost of an operation is represented by logical units of reserved capacity; it is not directly equivalent to CPU or RAM usage.

### Makefile

Run the default simulation:

```bash
make default
```

Run a basic client (YAML configuration):

```bash
make basic-client
```

Run a multi-client workload (YAML configuration):

```bash
make multi-client
```

Compare all scheduling strategies:

```bash
make compare
```

Run tests:

```bash
make unit-tests
```

## 📬 Planning Strategies

### FIFO

Selects the oldest pending operation when there is sufficient capacity to execute it.

Follows the first-in, first-out principle and does not bring forward later operations to take advantage of capacity gaps.

### Capacity-Fit

Selects the first pending operation whose cost can be covered by the available capacity.

Allows lower-cost operations to be brought forward when a previous operation cannot yet be executed.

This can improve capacity utilization, although it may also increase the wait time for some operations.

### Capacity-Fit (Starvation Protection)

Combines Capacity-Fit with a protection mechanism based on a maximum number of bypasses (*max_bypasses*).

When a pending operation reaches the set bypass limit, the strategy restricts new promotions that would exceed it.

The protected operation still requires sufficient capacity to be executed.

This mechanism reduces the risk of starvation but does not guarantee a maximum wait time.

## ⚙️ Admission and Execution

The engine uses a Controller to manage:

- The limits on concurrent operations.
- The available capacity and reserves for each active operation.
- The queue of pending operations.
- Admission, rejection, and promotion decisions.

The Runner coordinates concurrent execution through executors that implement the Executor interface.

### Exclusive Sessions

During an execution, the Runner acquires an exclusive session on the Controller.

This prevents other components from directly modifying the operations managed by the Runner while the session remains active.

The session is released once the execution and corresponding cleanup are complete, provided there are no pending operations or reservations.

### Cancellation and Errors

Cancellation is cooperative: executors receive a `context.Context` and are responsible for handling its cancellation signals.

The results distinguish between:

- The decision to accept an operation.
- Its execution status, once it has been accepted.
- The individual error returned by the executor.

Individual failures of an operation do not necessarily imply a general error in the Runner.

Coordination or recovery errors are reported via the error returned by Runner.Run(), which may be accompanied by individual results.

📊 Metrics

The CLI provides the following metrics:

- Total number of operations.
- Completed operations.
- Failed operations.
- Canceled operations.
- Rejected operations.
- Total batch duration.
- Maximum observed reserved capacity.
- Estimated average utilization percentage of reserved capacity.

Utilization is calculated by periodically sampling the Controller’s status.

> [!IMPORTANT]
> Capacity metrics represent logical reserves, not actual CPU or memory usage.
>
> The observed maximum capacity may be lower than the actual maximum reached if there are variations between samples.
>
> Average utilization is a temporal estimate calculated from the collected samples.

## 📂 Project structure

```text
pondus-gateway
├── clients
│   ├── basic.yaml
│   └── multiple.yaml
├── cmd
│   ├── pondus
│   │   ├── compare.go
│   │   ├── config.go
│   │   ├── main.go
│   │   ├── measure.go
│   │   └── pondus_test.go
│   └── .gitkeep
├── internal
│   ├── admission
│   │   ├── admission_test.go
│   │   ├── controller.go
│   │   └── session.go
│   ├── budget
│   │   ├── capacity_test.go
│   │   └── capacity.go
│   ├── core
│   │   ├── core_test.go
│   │   ├── lifecycle.go
│   │   ├── limits.go
│   │   ├── operation.go
│   │   └── state.go
│   ├── execution
│   │   ├── cpu.go
│   │   ├── execution_test.go
│   │   ├── executor.go
│   │   ├── failing.go
│   │   ├── runner.go
│   │   └── sleep.go
│   ├── observability
│   │   ├── metrics_test.go
│   │   └── metrics.go
│   └── scheduler
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

### Package Responsibilities

| Package | Responsibility |
| --- | --- |
| core | Operations, states, lifecycles and limits |
| budget | Capacity reservations and releases |
| scheduler | Strategies for selecting pending operations |
| admission | Admission, queue, promotions and exclusive sessions |
| execution | Concurrent execution, results and recovery |
| observability | Results summary and metrics calculation |
| cmd/pondus | Command-Line interface, configuration and simulations |

## ⚠️ Limitations

### Execution

- **Finite workloads:** The Runner processes batches of operations defined before execution begins. It does not support the continuous addition of new operations during an execution.

- **Cooperative Abortion:** Runners must respect context abortion. A timeout does not necessarily terminate an operation. If a runner remains blocked indefinitely, Runner.Run() may not terminate.

- **In-Memory State:** Operations, reservations, and execution states do not have long-term persistence or transactional recovery in the event of a process crash.

### Admission and scheduling

- **Initial batch validation:** YAML simulations require that all operations be initially admissible, including both active and pending operations. This validation does not account for future capacity releases during execution.

- **No Time Guarantee Against Starvation:** StarvationGuard limits preemptions but does not guarantee a maximum wait time or an execution deadline.

- **Controller Exclusivity:** During batch execution, the Controller is reserved for its Runner. Concurrent external modification of the Controller is not supported as a mechanism for dynamic work scheduling.

### Metrics and comparisons

- **Periodic sampling:** The peak reserved capacity is the observed maximum, not necessarily the actual maximum. Average utilization is an estimate that may omit brief variations between samples.

- **Experimental comparisons:** Observed durations and metrics may vary depending on the batch composition, strategy behavior, and runtime planning. The results of a single comparison do not constitute a statistically reproducible benchmark.

### Interfaces

- **No network service:** The MVP provides a CLI but does not expose an HTTP, gRPC, or TCP server.

- **Limited executor configuration:** The YAML CLI currently supports the `sleep` executor. Other execution implementations are not available as YAML-configurable types.

## 🔒 License

This project is licensed under the GNU GENERAL PUBLIC LICENSE, which allows its use, distribution and modification under the conditions specified in the `LICENSE` file.

# PondusGateway - v0.2.0

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

## 📂 Project structure

```
pondus-gateway
├── internal
│   ├── capacity
│   │   ├── capacity_edge_test.go
│   │   ├── capacity_test.go
│   │   └── capacity.go
│   ├── controller
│   │   ├── controller_stress_test.go
│   │   ├── controller_test.go
│   │   └── controller.go
│   ├── core
│   │   ├── lifecycle_test.go
│   │   ├── lifecycle.go
│   │   ├── operation_test.go
│   │   ├── operation.go
│   │   ├── state_test.go
│   │   └── state.go
│   ├── limits
│   │   ├── limits_test.go
│   │   └── limits.go
│   └── scheduler
│       ├── capacity_fit_test.go
│       ├── capacity_fit.go
│       ├── starvation_guard_test.go
│       ├── starvation_guard.go
│       ├── fifo_test.go
│       └── fifo.go
├── .gitignore
├── go.mod
├── LICENSE
├── Makefile
└── README.md
```

## 🔒 License

This project is licensed under the MIT license, which allows its use, distribution, and modification under the conditions specified in the `LICENSE` file.

## 👨‍💻 Autor

- Pedro Rizquez: pedro.rizquez.94@hotmail.com

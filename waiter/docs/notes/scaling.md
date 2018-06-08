# Waiter's Scaling Logic

* `autoscaler-goroutine` runs in a loop every `timeout-interval-ms` ms.
* `autoscaler-goroutine` determines global services state and triggers scaling of individual services via `scale-apps`.
    * `scale-apps` calls `scale-app` per service with `expired-instances`, `healthy-instances`, `outstanding-requests`, `target-instances`, `instances` (scheduled instances) to retrieve the `scale-amount`.
        * `scale-app` computes `scale-amount`, `scale-to-instances` and `target-instances` as `scaling-state`.
    * `scale-apps` uses `scale-amount` to determine calling `apply-scaling!` with `outstanding-requests`, `task-count` (requested instances), `instances` (scheduled instances), `scale-amount` and `scale-to-instances`.
        * `apply-scaling!` sends scaling request along `executor-multiplexer-chan`.
        * `service-scaling-executor` reads `executor-multiplexer-chan` to trigger scale-up or scale-down calls.
    * `scale-apps` returns `scaling-state` to use as seed data for next iteration of scaling computation for the service.

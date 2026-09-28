# Helius scheduler bindings

This workspace package extends upstream `agave-scheduler-bindings` 5.0.0 with
the dedicated bundle-simulation protocol used by the Helius external scheduler.
All upstream ABI types are re-exported unchanged.

The matching scheduling-utils handshake uses protocol version 6 and adds one
shared MPMC request queue and one shared MPMC response queue for the simulation
worker pool. External clients must use this package and the matching handshake.

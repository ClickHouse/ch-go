// Package chpool is a connection pool for ch.
//
// # Acquire order
//
// By default (AcquireLIFO), Acquire returns the most recently released
// connection. This keeps few connections in use and lets idle ones close.
// Set Options.AcquireStrategy to AcquireFIFO to rotate through all idle
// connections instead. Use FIFO when connections go to different
// replicas behind a load balancer and you want queries spread evenly
// across them. FIFO keeps every connection in use, so the pool does not
// shrink while it is under load, and each Acquire costs a little more.
package chpool

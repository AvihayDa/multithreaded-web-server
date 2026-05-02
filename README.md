# Multithreaded Web Server

A multithreaded web server implemented in C as part of the Operating Systems course at the Technion.

The project extends a basic single-threaded web server into a concurrent server that handles multiple client requests using a fixed-size worker thread pool and synchronized shared data structures.

## Main Features

- Fixed-size worker thread pool
- Producer-consumer request handling
- Bounded FIFO request queue
- Synchronization using pthread mutexes and condition variables
- Writer-priority readers-writers synchronization for the shared request log
- Per-request and per-thread statistics collection
- Support for GET and POST requests

## Build and Run

```
make
./server <port> <threads> <queue_size> <debug_sleep_time>
```

Example:

```
./server 5003 8 16 0
```

## Assignment Context

This project was developed for HW3 in the Operating Systems course at the Technion.

The original assignment description is included in:

```
docs/OS_HW3_2026.pdf
```

## Technologies

- C
- Linux
- POSIX threads (pthreads)
- Mutexes and condition variables

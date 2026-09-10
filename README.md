# Go Engineer Skill Test — Job Processing Service

## Overview

You are given a small Go HTTP service that accepts jobs and processes them asynchronously using a worker pool.

The repository intentionally contains a few implementation and reliability issues. Your task is to identify and fix them while keeping the existing API contract.

**Expected time:** 1–2 hours.

## Goal

Make the service production-ready for the scenarios covered by the existing tests and the requirements below.

### API

#### `POST /jobs`

Creates a job.

Request:

```json
{"payload":"hello"}
```

Requirements:
- `payload` is required and must not be blank.
- Return `201 Created` with the created job as JSON.
- Each job has a unique ID and starts with status `queued`.

#### `GET /jobs/{id}`

Returns a job by ID.

Requirements:
- Return `200 OK` when found.
- Return `404 Not Found` when the ID does not exist.
- A job moves through `queued` → `processing` → `completed`.
- If processing fails, the job must become `failed` and expose an error message.

## Requirements

1. **Concurrency safety**
   - The service must be safe under concurrent HTTP requests and workers.
   - Running `go test -race ./...` must pass.

2. **Worker pool**
   - The configured number of workers should process queued jobs concurrently.
   - Do not create an unbounded goroutine per job.

3. **Backpressure**
   - The queue has a finite capacity.
   - When the queue is full, `POST /jobs` must fail cleanly rather than blocking forever.

4. **Context and shutdown**
   - The server should stop accepting new work during shutdown.
   - Workers must exit cleanly when the service is stopped.
   - Do not leave goroutines running indefinitely.

5. **HTTP behavior**
   - Return appropriate status codes and JSON responses.
   - Do not expose internal implementation details or panic on malformed requests.

6. **Testing**
   - Fix or add tests where appropriate.
   - At minimum, cover the important concurrency and error paths you change.

7. **Code quality**
   - Keep the public API and project structure reasonably close to the provided version.
   - Prefer simple, idiomatic Go over unnecessary abstractions.

## Processing behavior

The supplied processor simulates work. Payloads containing the string `fail` should result in a failed job. Other payloads should complete successfully.

You do **not** need to add authentication, a database, Docker, external services, or any blockchain/Web3 functionality.

## How to run

```bash
go test ./...
go run ./cmd/server
```

The server listens on `:8080` by default.

## Git workflow — required

The repository includes its `.git` directory. Please preserve the existing Git history.

Before starting the task:

```bash
git status
git switch -c candidate/<your-name>
git add .
git commit -m "start test"
```

Then make your changes on `candidate/<your-name>`.

When you finish:

```bash
git add .
git commit -m "done test"
git status
```

Please submit the repository with the `.git` directory included so we can review your implementation and commit history.

## Evaluation

We will primarily evaluate:

- correctness and API behavior
- concurrency/race safety
- graceful shutdown and resource management
- quality of tests
- idiomatic Go and maintainability
- ability to reason about failure cases
- Git workflow and clarity of commits

Please do not spend time on cosmetic changes. Focus on correctness, reliability, and clear engineering decisions.

## Implementation notes

- Queue admission and queue closure use the same mutex, preventing a send from racing with shutdown.
- Jobs are stored before enqueueing so workers cannot observe a missing record; rejected jobs are removed before `Create` returns.
- The service owns the cancellation context passed to processors.
- `Stop` rejects new work, cancels processing, closes the finite queue, and waits for every worker to exit.
- Interrupted jobs become `failed` and retain an error message.
- `POST /jobs` accepts one bounded JSON request value and maps service failures to stable HTTP responses.

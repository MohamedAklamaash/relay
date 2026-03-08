# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/) and the project adheres to
semantic versioning.

## [Unreleased]

### Added
- Redis-backed client, worker server, and cron scheduler
- At-least-once delivery with visibility-timeout crash recovery
- Immediate, delayed, and recurring cron jobs
- Automatic retries with exponential backoff and dead-letter archive
- Task deduplication via `Unique` and explicit `TaskID`
- Weighted priority queues with pause/resume
- Per-task timeouts, deadlines, and remote cancellation
- Prometheus metrics
- `relay` CLI for queue inspection and management

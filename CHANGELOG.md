# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]
### Changed
- `ApiClientBase` internals replaced with the shared
  `agent_utilities.http.BaseApiClient` fleet base (httpx): same public
  surface (`request()` shapes, `last_etag`, constructor), now with
  rate-limit capture, bounded 429 backoff, and log redaction for free.
- Dropped the direct `requests` dependency (transport is now httpx via
  `agent-utilities`).
- Bumped `agent-utilities` pin to `>=0.47.2` — **requires unreleased
  agent-utilities (`agent_utilities.http` ships in the next release) — do
  not push until that release is on PyPI**; until then, run tests with the
  dev tree on `PYTHONPATH` and expect `uv lock` against the public index to
  fail to resolve.

## [0.1.0] - 2026-06-01
### Added
- Initial project structure and standardization
- Standardized README, AGENTS, and `.env.example`
- Copied testing and script boilerplate from gitlab-api

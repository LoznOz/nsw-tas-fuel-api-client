# FuelCheck operation-local candidate — 15 October 2026

Repository: LoznOz fork (origin verified as https://github.com/LoznOz/nsw-tas-fuel-api-client.git).

Branch: `codex/fuelcheck-operation-local`.

Worktree: `E:/HA Dev Work/Codex/FPA/fuelcheck-operation-local`.

Verified parent and unchanged HEAD: `b96c588bcc0d0fa179de6ceac513cac10d82cfe9`.

## Exact changed files

- `nsw_tas_fuel/client.py`

- `nsw_tas_fuel/diagnostics.py` (new)

- `tests/test_operation_accounting.py` (new)

- `OPERATION_LOCAL_REVIEW.md` (this report)

This RFC 850 correction modifies only diagnostics.py, test_operation_accounting.py and this report. client.py remains byte-for-byte unchanged. All four candidate files were already staged at entry to this tranche; those staged contents are preserved, with the three corrections left unstaged. No staging, commit, push, PR or merge was performed in this tranche. No provider requests, live HA installation, cache policy, prices/new behavior, DTO structures, canonical models or frozen FPA contracts changed. DTO-01 is excluded.

## Semantics and consumption

The existing guarded method boundary now owns one operation for the outermost decorated call on a given client/task. Nested calls share it through a ContextVar. A child task inheriting a context creates its own state, as does another client; no client-wide subtraction is used. Intended concurrency is tasks on one event loop, matching aiohttp; cross-thread sharing is unsupported.

An immutable OperationEvidence carries a generated identifier, local method name, outcome, one logical acquisition, independent OAuth/data/retried-data counts, total=OAuth+data, shared OAuth reuse count, last observed status, normalized rate-limit guidance and an ordered tuple of HTTPAttemptEvidence. Each attempt records kind, retry flag and observed status (unknown if no response was obtained). Retries are a subset of data attempts. Counts advance immediately at session.get/session.request handoff, before response entry, so transport exceptions and cancellation retain the attempt. These are session-level API handoffs, not guaranteed physical network transactions. aiohttp redirects or internal connection retries are not separately instrumented, and a handoff may fail before any network transmission. Counts do not prove provider receipt, billable quota use or successful response parsing.

Shared OAuth attribution: the lock holder that actually dispatches owns that one attempt; successful waiting operations record shared_oauth_reuses and zero OAuth attempts. Summed operation totals match client totals over completed operations from the same observation window. A waiting operation cancelled before dispatch has zero attempts. If the refresh fails or its owner is cancelled, a subsequent waiter may dispatch its own refresh; that is a new actual attempt, not duplicated attribution. No detached refresh task or new authentication retry policy is introduced. A late 401 only invalidates the same token used for that request, preserving a replacement already obtained by another operation.

Use the optional keyword-only `operation_observer=records.append` constructor argument to collect completion records for successes and failures, including cancellations. Existing public method signatures and result values remain intact. The callback is synchronous and should be fast; ordinary callback exceptions and synchronous callback-raised asyncio.CancelledError are isolated and logged with a local constant. Genuine cancellation from acquisition awaits propagates unchanged. ContextVar ownership is restored before observer publication and in a finally block even if evidence finalization fails. No unbounded record history is kept by the client. Escaping errors/cancellations also receive `.operation_evidence`; failure evidence does not require an observer. An enclosing asyncio.timeout converts cancellation into TimeoutError outside the client, so collect deadline evidence through the observer (outcome=cancelled).

Unexpected parsing/DTO exceptions now become NSWFuelApiClientError with a local safe message and detached cause/context. Existing typed client failures retain their classes/messages. This is the one error-interface migration: callers catching raw DTO ValueError/KeyError should catch NSWFuelApiClientError instead. No DTO preservation or deserialization change was made.

HTTP 429 produces NSWFuelApiClientRateLimitError, a subclass of the existing base error, on both OAuth and data responses before attempting to read the body, including unreadable bodies. Retry-After supports nonnegative ASCII integer seconds and the three HTTP-date wire forms: IMF-fixdate, obsolete RFC 850 and asctime. The complete syntax is validated before date parsing; trailing garbage and numeric timezone substitutions are rejected. Asctime implies UTC. Guidance is normalized once per response against UTC with past dates clamped to zero; the identical value is reused by the operation record and typed error. Missing, malformed, oversized, non-finite or excessively large guidance is unknown. No raw header is retained; no 429 retry was added. Existing one-retry 401/408 policy and wait remain intact. The new error can be imported from nsw_tas_fuel.client; existing package exports are unchanged.

Diagnostic evidence excludes credentials, headers, URLs, bodies, station identifiers and exception text. Existing P0 log/error redaction, cancellation propagation and error-chain detachment pass. Raw traceback-local inspection and arbitrary external logging are outside this guarantee.

## Validation

Existing Python 3.14.8 / aiohttp 3.13.5 environment; pytest 9.1.1 and Ruff 0.16.9. No dependencies installed or bounds changed.

- Client regressions: 17 passed.

- Existing request-accounting tests: 3 passed.

- Operation-local tests: 122 passed (52 previously reviewed cases plus 70 RFC 850 correction regressions).

- Existing full synthetic diagnostic/P0 suite, importing this candidate: 86 passed.

- Ruff check entire candidate: passed.

- Ruff format entire candidate --check --no-cache: passed (16 files); report strict UTF-8 decoding passed separately.

- git diff --check: passed; explicit UTF-8/trailing-whitespace checks also cover all four candidate files, including untracked files.

Candidate test command (existing interpreter):

    python -B -m pytest -o addopts= -p no:cacheprovider --allow-hosts=127.0.0.1,::1 tests/test_client.py tests/test_request_accounting.py tests/test_operation_accounting.py

The diagnostic suite was invoked via pytest.main with this worktree first in sys.path, tools/fuelcheck_diagnostics_v01 second, its pytest.ini and offline_guard plugin, and its tests directory. The printed client module path verified candidate selection. That suite includes read-only extracted integration method probes, not native HA execution.

Coverage includes cold/warm calls, concurrent shared sessions, successful simultaneous refresh, overlapping stale 401s, 401 and 408 retries, parsing/DTO failure after 200, transport timeout, OAuth authentication/malformed response failure, cancellation before handoff/inside transport/retry wait, external deadline, inherited task context, 429 valid/missing/malformed guidance, HTTP date normalization, observer failure and synthetic canary redaction.

### Independently reproduced findings and corrections

1. Ambient exception attribution: removed sys.exc_info entirely. Every invocation tracks its own escaping exception and terminal outcome. Regression cases verify both successful and failed acquisitions inside an unrelated caller handler, untouched caller exception/cause, correct returned values or error evidence, and restored ContextVar state.
2. Observer cancellation: synchronous observer-raised CancelledError is isolated separately from genuine acquisition cancellation. Six parameterized cases cover ordinary and cancellation observer failures during successful, failed and cancelled acquisitions. They verify returned DTO values, safe original errors/cancellation messages, immutable records, outcomes, counts and context restoration.
3. Typed 429: status/header classification precedes body reading on both OAuth and data paths. Two unreadable-body cases assert no body read, a typed rate-limit error, exact evidence and one attempt with no retry.
4. Retry-After: full-match HTTP-date validation rejects trailing garbage and malformed dates; eight date cases cover malformed syntax/calendar/time, past-date zero and the obsolete wire forms. Two advancing-clock cases (OAuth/data) demonstrate one clock sample and identical error/record guidance.
5. Report encoding: the resumed report decoded successfully as strict UTF-8. It was explicitly rewritten as UTF-8 and verified again, alongside full-tree Ruff formatting.

Two additional shared-refresh cases cover failed and cancelled refresh owners. The waiting operation obtains a fresh token and succeeds; each actual refresh handoff is owned once, with aggregate OAuth=2, data=1, retries=0, total=3. Lock release, cancellation preservation, observed statuses and separate operation IDs are asserted. No failed/cancelled refresh is mislabelled as successful shared reuse.

All 142 candidate tests and 86 existing diagnostic/P0 tests passed after the RFC 850 correction: 228 total, zero failures/skips. The 70 new cases also passed as a focused run (52 unrelated operation cases deselected). No provider traffic was used. Real-network integration tests were not collected. Candidate tests restricted connections to loopback for Windows asyncio's internal socket pair; the external diagnostic suite used its offline_guard plugin, with the candidate client module path asserted.

Exact final static checks from the candidate root, using the existing Ruff executable:

    ruff check . --no-cache
    ruff format . --check --no-cache
    git diff --check

The intermediate format check detected mixed line endings introduced by the added tests. Ruff normalized that file; the final full-tree format check passed. The report records the final candidate results without claiming GitHub CI or installed-package validation.

## Remaining work and smallest follow-on

No cache or HA integration work is included. HA still needs an explicitly approved adapter mapping operation records to common FPA evidence and typed 429 categories, plus native HA compatibility validation. Retention/export belongs to that adapter; do not infer operation counts from client lifetime deltas. Any provider-wide throttling, Retry-After scheduling or cache policy change requires a separate policy decision.

Smallest next step: reconcile the outdated index by staging the independently verified latest working-tree versions of the four approved files in a separately authorised commit step. After approval, a separate bounded adapter tranche can consume these records without changing provider cache or DTO policy. CI configuration is preserved; GitHub CI was not run because push/PR actions are prohibited. Local equivalents of its client-test, lint and formatting checks passed; no clean install/build or native HA validation is claimed.

## Resumed-session verification - 15 October 2026

At session entry the five corrections and their 22 regression cases were already present in the unstaged candidate. Their behavior was inspected and independently rerun locally; no new production-code change was needed. This session adds three tests for restoration of another client's caller ContextVar on success/failure and restoration even when evidence finalization raises. Internal evidence-finalization failures still propagate and prevent record publication; the latter test verifies unconditional context cleanup, not suppression of internal defects.

The report was decoded strictly as UTF-8, rewritten explicitly as UTF-8 with normalized paragraph spacing, and included in final formatting verification. Final commands use --no-cache to avoid generated Ruff state.

## Final candidate state

Branch and baseline HEAD remain unchanged. Only the four candidate files listed above differ from HEAD. At entry to the RFC 850 tranche, all four were already staged; the earlier empty-index statement no longer described the actual worktree. This tranche preserves that staged snapshot and leaves corrections to three files unstaged (AM for the report, diagnostics and operation tests; M for the already-staged client). Nothing was staged, committed, pushed, merged or deployed by this tranche. No adapter work began. Public acquisition signatures and successful DTO values remain unchanged; the earlier documented raw DTO exception migration is retained.

## RFC 850 century correction - 15 October 2026

The final independent review reproduced a P2 error: email.utils.parsedate_to_datetime uses a fixed two-digit-year cutoff. Under a 2026 UTC clock, RFC 850 years 69, 70 and 75 were decoded into the 1900s and incorrectly clamped to zero, losing valid future retry guidance.

The correction follows [RFC 9110 section 5.6.7](https://www.rfc-editor.org/rfc/rfc9110.html#section-5.6.7). The strict existing full-match grammar now captures the RFC 850 year. After parsing the calendar/time fields, one UTC sample resolves the latest matching year no more than fifty calendar years ahead and calculates the delay. Component comparison handles the exact time boundary and leap-day clocks without constructing an invalid anniversary datetime. The resolved date is then calendar-validated; invalid resolved leap days remain unknown. IMF-fixdate/asctime years stay explicit, numeric seconds do not sample the clock, and past dates remain zero.

Explicit full-year test expectations were selected from the RFC rule rather than from parser output or the implementation's century arithmetic. Examples:

| Clock (UTC) | RFC 850 date/time | Independently expected full date (UTC) |
|---|---|---|
| 2026-10-14 00:00:00 | 14-Oct-69 00:01:00 | 2069-10-14 00:01:00 |
| 2026-10-14 00:00:00 | 14-Oct-70 00:01:00 | 2070-10-14 00:01:00 |
| 2026-10-14 00:00:00 | 14-Oct-75 00:01:00 | 2075-10-14 00:01:00 |
| 2026-10-14 00:00:00 | 13-Oct-76 23:59:59 | 2076-10-13 23:59:59 (inside boundary) |
| 2026-10-14 00:00:00 | 14-Oct-76 00:00:00 | 2076-10-14 00:00:00 (exact boundary) |
| 2026-10-14 00:00:00 | 14-Oct-76 00:00:01 | 1976-10-14 00:00:01 (outside boundary; delay zero) |
| 1999-12-31 23:59:30 | 01-Jan-00 00:00:00 | 2000-01-01 00:00:00 (delay 30 seconds) |
| 2099-12-31 23:59:30 | 01-Jan-00 00:00:00 | 2100-01-01 00:00:00 (delay 30 seconds) |
| 2099-12-31 23:59:30 | 01-Jan-50 00:00:00 | 2050-01-01 00:00:00 (outside boundary; delay zero) |

The new coverage contains 20 direct normalization cases, the same 20 cases through both OAuth and data 429 paths (40), six malformed/invalid resolved-date cases and four other-format preservation cases. Additional clocks cover 1899, 2024 leap day, 2069, 2100 and 2399. Resolved leap days in 1900/2100 are rejected; 2400-02-29 remains valid. An in-memory comparison against the entry staged parser reproduced 13 pre-fix mismatches in the 20 valid cases; all corrected outputs and all literal weekdays were cross-checked against the explicit expected full dates.

The 40 end-to-end cases use an advancing clock and unreadable bodies, asserting exactly one clock sample, no body read, no retry, one owned attempt and identical guidance in the typed rate-limit error and immutable operation record. The original 2026 year-69 reproducer now returns 1,356,998,460 seconds rather than zero. Existing cancellation, observer, ContextVar, shared-refresh, DTO migration and P0 tests all pass unchanged.

Finalization behavior is intentionally unchanged: an internal freeze defect propagates, restores ContextVar state and loses publication; it can replace an existing failure or cancellation. No ordinary transport-triggered finalization defect was reproduced in the independent review. Native HA and clean-install/build validation remain outstanding, and session handoff counts retain the measurement limits described above. The latest working-tree candidate has now passed the final independent verification recorded below; staging and commit remain separately authorised actions.


## Final RFC 850 independent verification - 15 October 2026

**ACCEPT for staging:** the latest working-tree implementation and tests were inspected against baseline HEAD and RFC 9110 section 5.6.7. The staged snapshot was treated as outdated. No implementation or test files changed during this review. This section and the next-step wording are documentation-only updates, followed by final full-tree formatting, strict UTF-8, whitespace and state consistency checks.

An independent probe constructed its own oracle by enumerating full-year candidates and selecting the latest eligible calendar timestamp, rather than using the implementation's century arithmetic or email parser output. All 6,400 comparisons passed: 100 two-digit years, eight UTC clocks and eight date/time positions. Clocks include 1899/1999/2099/2399 century transitions, October 2026, a microsecond boundary, February 2024 leap day and 2069. The probe checked exactly one UTC sample per date. Separate leap-century probes confirmed invalid resolved dates in 1900/2100 remain unknown while 2400-02-29 is valid.

Independent literal probes under 2026-10-14 00:00:00 UTC produced:

- 14-Oct-69 00:01:00 -> 2069, 1,356,998,460 seconds.
- 14-Oct-70 00:01:00 -> 2070, 1,388,534,460 seconds.
- 14-Oct-75 00:01:00 -> 2075, 1,546,300,860 seconds.

A separately constructed fake transport also verified original caller-handler contamination stays fixed; callback-generated cancellation cannot replace success, failure or genuine acquisition cancellation; preceding ContextVar state is restored; and both OAuth/data RFC 850 429 paths preserve typed errors, identical normalized guidance, a single clock sample and no body read/retry. Failed and cancelled refresh owners each recovered two concurrent waiters with OAuth=2, data=2, retries=0, total=4. Per-operation OAuth ownership was checked against dispatching task names, not inferred from shared-counter subtraction. Evidence remains frozen dataclasses containing immutable primitive fields and an immutable tuple of frozen attempt records.

Exact current repository results:

| Suite | Passed | Failed | Errors | Skipped |
|---|---:|---:|---:|---:|
| Existing client regressions | 17 | 0 | 0 | 0 |
| Existing request accounting | 3 | 0 | 0 | 0 |
| Operation-local accounting, including 70 RFC cases | 122 | 0 | 0 | 0 |
| Complete existing synthetic diagnostics/P0 | 86 | 0 | 0 | 0 |
| Total | 228 | 0 | 0 | 0 |

The candidate command and external offline_guard invocation described above were rerun; candidate module selection was asserted. Ruff lint, full-tree format check (16 files), strict UTF-8/trailing-whitespace checks on all four candidate files, git diff --check and git diff --cached --check passed. No live provider/geocoder requests or native HA execution occurred. The independent probes are additional in-memory checks, not included in the 228 repository-test count.

No substantive defect or unexpected implementation scope expansion was found. Native acquisition signatures/results, the documented safe DTO exception migration, 401/408 retry policy, no-retry 429 policy, cache behavior and HA/Core independence remain intact. Session-level handoff accounting limitations, callback performance expectations, internal-finalization failure behavior, runtime-owned durability and installed-package/native HA gates remain as documented. Guidance outside supported calendar/datetime representation remains unknown; this review does not certify physical-network transaction accounting or crash-proof persistence.

HEAD remains b96c588bcc0d0fa179de6ceac513cac10d82cfe9 on codex/fuelcheck-operation-local. The existing staged blobs were preserved exactly. Latest working-tree client/diagnostics/test bytes were preserved exactly; only this report changed. Expected status remains M for the staged-only client and AM for diagnostics, tests and report. Do not commit the old index snapshot as the verified final candidate. No Git identity was configured and nothing was staged, committed, pushed, merged or deployed during this verification.

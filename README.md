# durable-agent

A durable execution engine for AI agent workflows, built on [`agentq`](../agentq)
(a generic workflow/step/event engine, sibling crate in this workspace).
Demonstrated here via a support-ticket resolution workflow: classify an
incoming ticket with an LLM, draft a reply, park for human approval, send the
reply, and mark the ticket resolved — surviving worker crashes at any point
along the way.

## Start it

```
cargo run
```

No external credentials are required — with no API keys set, the app uses a
mock LLM classifier, a mock AgentMail sender, and a no-op trace sink, so the
whole demo works end to end out of the box.

To also enable the crash-demo endpoint:

```
cargo run --features dev-tools
```

This alone does not enable killing the worker — see `DURABLE_AGENT_ALLOW_DEV_KILL`
below.

## UI

Open **http://127.0.0.1:8080/** once the server is running.

## Environment variables

All are optional. Unset, the app falls back to mocks/no-ops so it still runs.

| Variable | Purpose | If unset |
|---|---|---|
| `OPENAI_API_KEY` | Real `gpt-4o-mini` ticket classification via `async-openai` | Falls back to `MockClassifier` (canned classification/draft reply) |
| `AGENTMAIL_API_KEY` | Real AgentMail sends for approval requests and customer replies | Falls back to `MockApprovalSender`/`MockCustomerMailer` (logged, not sent) |
| `AGENTMAIL_FROM_ADDRESS` | AgentMail inbox address to send from | Same fallback as above — all three of `AGENTMAIL_API_KEY`, `AGENTMAIL_FROM_ADDRESS`, `APPROVER_EMAIL` must be set together or the mocks are used |
| `APPROVER_EMAIL` | Address the approval request is sent to | See above |
| `AGENTMAIL_WEBHOOK_SECRET` | Svix signing secret (`whsec_...`) to verify inbound `/webhooks/agentmail` replies | Webhook signature verification is skipped (only relevant if you're wiring a real AgentMail inbound webhook) |
| `RESPAN_API_KEY` | Forwards workflow execution spans to Respan for observability | Falls back to a no-op trace sink (tracing calls are skipped entirely, never block the workflow) |
| `DURABLE_AGENT_ALLOW_DEV_KILL` | Must be exactly `1` to allow `POST /dev/kill` to actually SIGKILL the running process | Endpoint returns 403 (or doesn't exist at all without the `dev-tools` build feature) |
| `CLASSIFY_TIMEOUT_SECS` | Per-step wall-clock timeout for `ClassifyTicket` (seconds) | Defaults to `30` |
| `SEND_REPLY_TIMEOUT_SECS` | Per-step wall-clock timeout for `SendReply` (seconds) | Defaults to `15` |
| `DEMO_CLASSIFIER_MODE` | Demo-only mock classifier behavior: `fail_once`, `fail_permanent`, or `slow` (see `scripts/demo.sh`) | Normal mock classification |
| `DURABLE_AGENT_API_KEY` | Bearer token required on workflow management routes (`/workflows*`, `/metrics`, etc.) | Management routes are unauthenticated |

## 10-minute walkthrough

1. `cargo run`, then open `http://127.0.0.1:8080/`.
2. Fill in the create-workflow form (customer email / subject / body have
   working placeholder defaults — edit or leave as-is) and click **Create &
   run**. The new workflow appears in the workflow list on the left with a
   status pill, and is auto-selected.
3. Watch the timeline: `IngestTicket` and `ClassifyTicket` complete quickly
   (mocked LLM), then the workflow reaches `RequestApproval` and its status
   pill turns to `waiting`. The event log shows `ClassifyTicket`'s output —
   the mock category/urgency/draft-reply JSON.
4. Click **Approve reply** on this first workflow. Watch it run `SendReply` →
   `UpdateTicketSystem` → `Complete` and its pill turn to `completed`.
5. Create a second workflow the same way. Once it's `waiting`, type a reason
   in the reject field and click **Reject reply**. Watch its pill turn to
   `failed` — the step icon for `RequestApproval` shows a distinct
   permanent-failure mark (not the same icon used for a recoverable crash).
6. Create a third workflow, and while it's still `running` or `waiting`,
   click **Cancel workflow**. Its pill turns to `cancelled` and the event log shows
   `WorkflowCancelled`; the timeline stops advancing.
7. Check the metrics strip at the top — `workflows_created`, `workflow_runs`,
   `approvals`, `rejections`, `cancellations` should reflect the actions
   above, and `workflows_waiting` reflects any workflow currently parked at
   the approval step.
8. **Crash/recovery (optional, needs `--features dev-tools`)**: stop the
   server, restart it with `cargo run --features dev-tools`, set
   `DURABLE_AGENT_ALLOW_DEV_KILL=1` in its environment, create a fourth
   workflow and run it. While it's mid-`ClassifyTicket` (fast with the mock
   classifier — you may need to watch closely or retry once), hit
   `POST /dev/kill` (e.g. `curl -X POST http://127.0.0.1:8080/dev/kill`).
   The process exits immediately. Restart the same command — startup
   recovery logs `re-admitted interrupted step(s)` and the workflow resumes
   from where it was, without re-running `IngestTicket`.

## What's mocked vs. real

| Piece | Mock (default) | Real (needs credentials) |
|---|---|---|
| Classification | `MockClassifier` — canned category/urgency/draft | `OpenAiClassifier` — real `gpt-4o-mini` call, needs `OPENAI_API_KEY` |
| Approval request + customer reply | `MockApprovalSender`/`MockCustomerMailer` — logged only | `AgentMailSender` — real AgentMail API calls, needs `AGENTMAIL_API_KEY`/`AGENTMAIL_FROM_ADDRESS`/`APPROVER_EMAIL` |
| Tracing | `NoopSink` — tracing calls skipped | `RespanSink` — spans forwarded to Respan, needs `RESPAN_API_KEY` |
| Ticket system | `SqliteTicketSystem` — `resolved_tickets` table in `durable-agent.db` (durable, local; not an external CRM) | Same SQLite-backed store (no separate external ticket product) |

The entire walkthrough above works with zero credentials set.

## Architecture and deployment

See [`WORKFLOW.md`](WORKFLOW.md) for the workflow's step-by-step design and
[`DEPLOYMENT.md`](DEPLOYMENT.md) for the intended production deployment
shape.

## Reliability semantics

This engine gives **at-least-once execution, not exactly-once**. Recovery
after a crash may re-execute a step that was interrupted mid-flight; any step
with an external side effect (sending an email, updating a ticket system)
must be idempotent on its own terms for the workflow to be effectively-once
end to end.

While a step is running, the worker **renews its lease** on that step so a
second worker cannot claim the same in-flight work just because wall-clock
time passed — this mitigates duplicate execution across a live worker vs. a
recovering one during long steps. After a process death, recovery still waits
for the lease to expire before re-admitting the step (see `scripts/demo.sh
crash`).

`SendReply` has no dedup key of its own — it relies on the engine skipping
steps already recorded as `Completed`, so a crash strictly *before*
completion is recorded can, in principle, cause a duplicate send on retry.
`UpdateTicketSystem` is idempotent (keyed on `ticket_id` in SQLite).

This is a **single-node** engine — no distributed coordination, no leader
election, no multi-worker scheduling. There is one specific, still-open
concurrency limitation from an early correctness pass, kept as a documented
decision rather than fixed: two call chains that both read/act on the same
`worker_id` are not mutually exclusive at the storage layer once their retry
attempt numbers diverge — V1's job-key dedup masks this for the common case,
but it is not a general guarantee. Nothing in the current roadmap requires
fixing this; it is deliberately deferred to a future reliability phase.

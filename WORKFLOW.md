# durable-agent demo workflow — support ticket resolution

The showcase workflow for durable-agent, built on agentq v2. Steps below map
directly to `StepDef`s registered on one `Workflow`.

## Steps

1. **IngestTicket** — parse the incoming ticket (customer id, subject, body)
   from the API payload. Idempotency key: `ticket_id`. Pure, no external call.
2. **ClassifyTicket** — call an LLM to produce category, urgency, and a
   drafted reply. The long-running, retryable step (LLM latency/timeouts,
   429s). This is the step we SIGKILL the worker during for the demo.
3. **RequestApproval** — persist `StepStatus::Waiting`, send the draft to a
   human via the AgentMail adapter. Workflow sits idle, no thread parked.
4. **SendReply** *(resumes here after approval)* — email the approved (or
   human-edited) reply to the customer. External side effect with real
   duplicate-send risk — there is no dedup key of its own; safety relies on
   the engine skipping a step already recorded as `Completed`, so a crash
   strictly before that record lands can still cause a duplicate send on
   retry.
5. **UpdateTicketSystem** — mark the ticket resolved in the ticketing system
   (mocked CRM/Zendesk-shaped API). Genuinely idempotent, keyed on
   `ticket_id` (an in-memory `HashSet`, unlike step 4).
6. **Complete** — terminal, no-op step that just closes the workflow.

## Beyond the happy path

- **Rejection**: `POST /workflows/:id/reject` at the `RequestApproval` step
  fails the workflow (non-retryable) instead of resuming to `SendReply`.
- **Cancellation**: `POST /workflows/:id/cancel` works from any
  `pending`/`running`/`waiting` state and blocks any approve/reject that
  arrives after it.
- **Retry classification**: transient failures (network errors, 429s, 5xx)
  retry with backoff per `RetryPolicy`; non-retryable failures (4xx other
  than 429, an explicit rejection) fail the workflow immediately instead of
  retrying.
- **Timeout**: a step exceeding its configured timeout is treated as a
  failure and routed through the same retry/non-retryable classification as
  any other failure.

## External integrations (mocked behind adapters, real interface)

- LLM call for step 2 — a trait `Classifier`, one real implementation
  (any HTTP LLM API) and one local mock that returns canned classifications
  for the demo, so it runs with no API key.
- AgentMail for step 3's approval request and step 4's reply — same
  adapter-with-local-fallback shape as the original plan.
- Ticketing system for step 5 — an in-memory mock CRM (a `HashMap` behind
  a small HTTP server), swappable for a real one later.

## The demo sequence (maps to the artifact's lifecycle timeline)

```
POST /workflows             -> ticket-4821 workflow created
POST /workflows/:id/run     -> IngestTicket completes, ClassifyTicket starts
kill -9 <worker pid>        -> mid ClassifyTicket, lease left dangling
(restart worker)            -> recovery finds expired lease, re-admits
                                ClassifyTicket as a new attempt
                             -> IngestTicket is NOT re-run (already Completed)
ClassifyTicket completes    -> RequestApproval sends via AgentMail, workflow
                                becomes Waiting
POST /workflows/:id/approve -> resume(workflow_id, step=3, input=approved)
                             -> SendReply -> UpdateTicketSystem -> Complete
```

## Why this scenario over the original order/quote example

Same durability story (idempotent side effects, crash recovery, human
approval), but the AI step (classify + draft) is genuinely agentic rather
than data plumbing, which is closer to what the target audience (AI agent
infra companies) is actually building.

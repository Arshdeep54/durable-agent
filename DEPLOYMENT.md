# durable-agent — architecture & deployment

## LLM

`gpt-4o-mini` via [`async-openai`](https://github.com/64bit/async-openai), used
only by the `Classifier` trait's OpenAI implementation (`ClassifyTicket`
step). Cheap and fast enough for per-ticket classification + a short drafted
reply; no reason to reach for a bigger model here. The trait has a local mock
implementation too, so the demo and tests never need a live API key.

```rust
trait Classifier {
    async fn classify(&self, ticket: &Ticket) -> Result<Classification, ClassifyError>;
}
struct OpenAiClassifier { client: async_openai::Client<...> }
struct MockClassifier; // canned responses, used in tests and API-key-less demo runs
```

## Process architecture

```
EC2 instance (single small box, e.g. t4g.micro)
├── durable-agent binary        — engine + workflow defs + axum API, one process
├── SQLite file (agentq_store.db) on the instance's EBS volume
│     → durable state; survives reboot, crash, SIGKILL
├── systemd unit: durable-agent.service, Restart=on-failure
│     → the "Kill Worker" dev control just SIGKILLs this process.
│       systemd restarts it; recovery.rs finds the expired lease on
│       boot and re-admits the interrupted step. This is the real
│       production crash path, not a simulated one.
└── cloudflared (Cloudflare Tunnel) → api.<domain>
      → no inbound port open except SSH; TLS handled by Cloudflare

Cloudflare Pages
└── static frontend (the console UI) → app.<domain>
      Git-connected repo: push to main, Pages builds and deploys itself.
      Calls api.<domain> over fetch; CORS allowlists app.<domain>.
```

### Why this shape

- **No Docker/Kubernetes.** One Rust binary, one target triple, systemd is a
  sufficient process supervisor. Add a container only if a second service
  shows up that actually needs one.
- **No separate CI build farm.** GitHub Actions SSHs into the EC2 box,
  `cargo build --release`, restarts the systemd unit — that's the whole
  backend deploy pipeline. Cloudflare Pages needs no pipeline at all; its
  Git integration builds and deploys on push.
- **Cloudflare Tunnel, not an open port.** Skips cert management and keeps
  the EC2 security group closed except for SSH.
- **Secrets via `systemd EnvironmentFile=`**, not a dotenv crate — the OS
  already solves "load key=value pairs into a process's environment
  before it starts," so nothing extra is pulled into `Cargo.toml` for it.
  `/etc/durable-agent/.env` (root-owned, `600`) holds `OPENAI_API_KEY` and
  `AGENTMAIL_API_KEY`.

## Deploy flow

```
# backend (GitHub Actions, on push to main)
ssh ec2 'cd durable-agent && git pull && cargo build --release'
ssh ec2 'sudo systemctl restart durable-agent'

# frontend
# nothing to script — Cloudflare Pages watches the repo directly
```

## Open items (need a decision before this is buildable)

- Domain name to point Cloudflare at.
- AgentMail: confirmed to wire in from the start (not deferred behind a
  mock) — need the AgentMail API key and account set up before `RequestApproval`
  can be implemented for real. The adapter still sits behind the same
  generic trait as any other integration, so a mock impl stays available
  for tests even though it isn't the demo's primary path.

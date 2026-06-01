<!-- markdownlint-disable -->

# ACARS Router — Senior Engineer Audit

**Reviewed:** workspace at `acars_router/` (3 crates, ~3,700 LOC of Rust)
**Toolchain:** edition 2021, MSRV 1.94, resolver 2, no `[workspace.lints]`
**Author of this report:** internal review
**Verdict:** Functional, but structurally weak. Multiple latent bugs, a handful of real performance issues (notably blocking DNS inside the async hot path and lock contention in the fan-out), pervasive copy-paste duplication, and a great deal of pre-2018 / "I just learned Rust" idioms. None of it is unfixable; most of it is mechanical.

---

## Remediation status

| PR                                                                                            | Audit refs                                                                   | Status   | Commit        |
| --------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------- | -------- | ------------- |
| PR1: edition 2024 + workspace lints + clippy clean                                            | §2.1, §2.2, §2.3, §2.4, §2.6 (`SocketListenerServer`), §5 (mechanical sweep) | **Done** | `56d1d7b`     |
| PR2: residual cleanups (commented git deps, reconnect-strategy nit)                           | §2.5 partial, §8 nit                                                         | **Done** | `39753f8`     |
| PR3: sanity checker fixes + HFDL/IMSL/IRDM coverage + `print_values` collapse                 | §3.1, §3.2, §3.13 (`print_values`)                                           | **Done** | `f5e6666`     |
| PR4: unified async DNS resolver (hickory)                                                     | §3.3, §3.4                                                                   | **Done** | `30a174e`     |
| PR5a: `Protocol` enum + `ProtocolIo` view + `Input::*_configured` collapse                    | §4.1 (partial), §4.3, listen-zmq bug                                         | **Done** | `f8b5c83`     |
| PR5b: collapse `start_processes` onto `ProtocolIo`; delete `ServerType` (unify on `Protocol`) | §4.2                                                                         | **Done** | `d3b3c38`     |
| PR5b2: collapse `sanity_checker` onto `ProtocolIo` iteration                                  | §3.1 follow-up, §4.4                                                         | **Done** | (this commit) |
| PR5c: SIO 0.7.1 migration + `CachedDnsTcp` `UnderlyingIo` impl                                | Appendix A.1.2                                                               | Done     | `bd3c9b6`     |
| PR6a: `broadcast` fan-out (deletes `Mutex<Vec<Sender>>` + per-peer mpsc)                      | §3.5, §3.6                                                                   | Done     | `c56fc47`     |
| PR6b: `JoinSet` + `CancellationToken` shutdown                                                | §3.12, §4.9                                                                  | Done     | `630590e`     |
| PR7: `packet_handler` rewrite                                                                 | §3.7, §4.8                                                                   | Done     | `735d4c0`     |
| PR8: dedupe / counters / freq table polish                                                    | §3.8, §3.10, §3.11                                                           | Done     | `fa048bd`     |
| PR9: tests + rustdoc cleanup                                                                  | §6                                                                           | **Done** | `7f5284e`     |
| Version bump 1.3.1 → 0.2.0 (semver reset post-audit)                                          | —                                                                            | **Done** | `7e6d9e2`     |
| PR10: UDP DNS cache — 30-min default TTL + failure-driven invalidation                        | §3.4 follow-up                                                               | **Done** | `c9d496a`     |
| PR11: `MessageHandlerConfig` Option/sub-struct refactor + workspace allow removal             | §3.13 (`struct_excessive_bools`)                                             | **Done** | `8e594e0`     |

**Open strategic question:** `log` vs `tracing` (audit §4.6). `tokio` already has the `tracing` feature enabled but the codebase uses `log` via `sdre-rust-logging` (which is `env_logger`-based). Switching is a workspace-wide change touching every log macro and would drop `sdre-rust-logging`. Decision deferred pending owner input.

**Remaining open items:**

- `log` vs `tracing` decision (§4.6).
- ~~Workspace-level `clippy::struct_excessive_bools = "allow"` was added to keep `Input` and `MessageHandlerConfig` compiling. Remove the workspace allow and restructure those two configs (group related bools into sub-structs or enums) once a refactor lands.~~ Fixed in PR11: `MessageHandlerConfig` reshaped into `Option<DedupeOpts>` / `Option<String>` station override / `StatsOpts` sub-struct (`add_proxy_id` + `stats.verbose` are the only remaining bools, both load-bearing). `Input` carries a site-local `#[allow(clippy::struct_excessive_bools)]` with a justifying comment: every bool is a documented `--flag` + `AR_*` env var; substructs would just push the lint into the substruct. Workspace allow removed.
- `udp_services.rs` retains the "if resolution fails, retry on every send" behaviour. Audit §3.4 flagged this as a pathology; owner decision (PR10) is to **keep** it: when DNS is broken, the user wants the router to retry hard so it recovers immediately on DNS restoration, not back off.

**Bugs found mid-stream (not addressed in their discovery PR):**

- ~~`Input::{acars,vdlm,hfdl,imsl,irdm}_configured` (`acars_config/src/lib.rs`) do **not** consult `listen_zmq_*` when deciding whether a protocol is configured.~~ Fixed in PR5a — replaced with `ProtocolIo::is_configured()` which covers all nine endpoint fields including `listen_zmq`.

---

## 1. Executive summary (TL;DR)

| Severity           | Count   | Examples                                                                                                                                                                                                                                                                                                        |
| ------------------ | ------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Bugs / correctness | 7       | DNS resolved with blocking `std::net` call in async hot path; sanity checker silently skips HFDL/IMSL/IRDM; TCP receiver rejects all hostnames; IPv6 not parseable by validator; `Shared::broadcast` never reaps dead peers; reassembly mutex re-locked 4× per message; unbounded per-peer channel = DoS vector |
| Architecture       | major   | 5× copy-paste of every protocol path (`start_processes`, `Input`, `sanity_checker`, message handler frequency-table); 50+ flat `Option<Vec<…>>` fields on `Input`                                                                                                                                               |
| Performance        | several | `Arc<Mutex<i32>>` counters instead of atomics; `Arc<Mutex<Vec<Sender>>>` fan-out held across `await`; `VecDeque` linear scan for dedupe; `serde_json` round-trip per hash; freq table is `Vec` + linear scan                                                                                                    |
| Idiom / style      | many    | `#[macro_use] extern crate log;` and `extern crate foo;` everywhere; `pub extern crate tokio as tokio`; `match v.contains(&false) { true => …, false => … }`; `format!("{}{}", "{", x)`; pre-2021 lifetime closures; `Vec<bool>` "result accumulator"                                                           |
| Build / process    | a few   | No `[workspace.lints]`; no `cargo deny` / `cargo audit`; CI doesn't enforce `-D warnings`; pinned-to-the-patch dep versions; dead code marked `#[allow(dead_code)]` instead of deleted                                                                                                                          |

The high-level picture matches your own assessment in point (d): the structure is bad. Almost everything that hurts (boilerplate, missed validation paths, scattered DNS handling, fan-out lock contention) is downstream of the same root cause: **the program models 5 protocols as 5 hard-coded copies of the same pipeline instead of one pipeline parameterised by a `Protocol` value.**

---

## 2. Toolchain / workspace hygiene

> **Status:** §2.1, §2.2, §2.3, §2.4 fully resolved in PR1 (`56d1d7b`). §2.5 partially resolved (commented git deps deleted in PR2); patch-pinning intentionally retained. §2.6 resolved (`SocketListenerServer` deleted in PR1; `SenderServer<T>` field removal deferred to PR5 alongside the `ProtocolIo` refactor).

### 2.1 Edition & MSRV (your point b)

- `edition = "2021"`, `rust-version = "1.94"`. Edition **2024** has been stable since 1.85 (Feb 2025). Bump to `edition = "2024"`, MSRV to something modern (1.85+). Most of the code already compiles cleanly under 2024; the only things that need touching are some `dyn Trait` warnings and the `extern crate` carcasses (see §2.3).
- `rust-version = "1.94"` is unusually high for a "released" tool. Consider committing to ~stable‑3 unless you actually use 1.94 features. None are used here.
- `Dockerfile` uses Rust 1.95. Fine, but pin via toolchain file (`rust-toolchain.toml`) so all surfaces agree.

### 2.2 Lints (your point a)

- **No `[workspace.lints]` table anywhere.** No crate has `[lints]` either; the only enforcement is whatever CI's `cargo clippy` defaults give you. Add a canonical workspace-level table such as:

```toml
[workspace.lints.rust]
unsafe_code        = "forbid"
missing_docs       = "warn"
rust_2018_idioms   = { level = "warn", priority = -1 }
unreachable_pub    = "warn"

[workspace.lints.clippy]
pedantic           = { level = "warn", priority = -1 }
nursery            = { level = "warn", priority = -1 }
cargo              = { level = "warn", priority = -1 }
# explicit opt-outs that are noisy in this codebase
module_name_repetitions = "allow"
missing_errors_doc      = "allow"
```

…and in each member crate:

```toml
[lints]
workspace = true
```

That single change will surface dozens of the style issues in §5 automatically (`needless_clone`, `large_stack_arrays`, `match_bool`, `redundant_closure`, `await_holding_lock`, `or_fun_call`, etc.).

- CI runs `cargo clippy` but doesn't `-D warnings`. Lint enforcement is therefore advisory. Once `[workspace.lints]` exists, switch CI to `cargo clippy --all-targets --all-features -- -D warnings`.
- No `cargo deny`, no `cargo audit`, no `cargo-machete` (the latter would have caught all the dead `extern crate`s). Add at least `cargo deny check` for licenses/advisories.

### 2.3 Pre-2018 idioms

In every crate:

```rust
#[macro_use]
extern crate log;
extern crate acars_config;
extern crate acars_connection_manager;
extern crate sdre_rust_logging;
extern crate serde;
extern crate serde_json;
```

(`rust/bin/acars_router/src/main.rs:8`, also `lib.rs` of both libraries.) None of these `extern crate` lines have been necessary since edition 2018. `#[macro_use] extern crate log;` should be replaced with `use log::{debug, error, info, trace, warn};` per module, **or** the project should commit to `tracing` (you already pull `tokio` features = `["full","tracing"]` — pick one). Then delete every `extern crate` in the tree.

### 2.4 Re-exports

`rust/libraries/acars_config/src/lib.rs:1` does `pub extern crate clap as clap;` and `rust/libraries/acars_connection_manager/src/lib.rs:9` does `pub extern crate tokio as tokio;`. Use the modern form:

```rust
pub use clap;
pub use tokio;
```

Better yet: don't re-export them at all. `main.rs:16` does `use acars_config::clap::Parser;` instead of importing `clap` directly in its own `Cargo.toml`. That coupling makes both crates harder to upgrade independently.

### 2.5 Dependency pinning

Workspace deps are pinned to the patch (`tokio = "1.52.2"`, `serde = "1.0.228"`, `log = "0.4.29"`, etc.). For libraries this prevents downstream consumers from picking compatible patch upgrades; for a workspace this is mostly cosmetic but does generate noisy renovate PRs. Prefer `"1"` / `"1.0"` for stable, well-versioned crates, and let `Cargo.lock` do the pinning.

### 2.6 Dead code retained

- `SocketListenerServer` (`service_init.rs:687-934`, 248 lines, `#[allow(dead_code)]`) is an entire unfinished "unified listener" rewrite with comment "All this below is brand new, not wired in." Delete it (git remembers) or finish it on a branch; don't ship it.
- `SenderServer<T>::host`, `proto_name`: marked `#[allow(dead_code)]` (`lib.rs:39`).
- `Vec<bool>` validity dance (`sanity_checker.rs:248-265`) only ever returns "did anything fail" — see §3.1.

---

## 3. Correctness bugs (in rough order of severity)

### 3.1 Sanity checker silently skips HFDL/IMSL/IRDM

> **Status:** resolved in PR3. All four checker functions now enumerate every protocol × every category (including `listen_zmq_*`, which was missing across the board). Cross-protocol port-collision tests added.

`rust/libraries/acars_config/src/sanity_checker.rs:49-196`

`check_port_validity`, `check_host_validity`, `check_no_duplicate_ports`, and `check_no_duplicate_hosts` only list ACARS and VDLM2 sources. HFDL, IMSL, and IRDM (added later) are **not validated**, **not deduplicated against each other**, and **not deduplicated against ACARS/VDLM2**. A user can:

- specify `--listen-tcp-hfdl 5550 --listen-tcp-acars 5550` and it will start, then one of the binds fails at runtime
- specify a malformed `--send-tcp-hfdl host` and it won't be caught until message-send time

This is the single most user-visible bug in the audit. The fix is to refactor `Input` so the checker iterates over `Protocol::iter()` (§4).

### 3.2 DNS validator rejects IPv6 _and_ doesn't actually validate hosts

> **Status:** structural validation resolved in PR3 — `check_host_is_valid` now tries `SocketAddr::from_str` first (handles IPv4 + bracketed IPv6), falls back to `rsplit_once(':')` for hostname:port, validates port as non-zero `u16`, and rejects empty / whitespace / `/` / unbracketed-colon hosts. Bracketed contents are re-validated via `IpAddr::from_str`. **Active DNS resolution at sanity-check time still not done** — deferred to PR4 alongside the unified resolver.

`sanity_checker.rs:294-364`:

1. Branches on `socket.chars().any(|c| c.is_alphabetic())` — any IPv6 address with hex letters (`fe80::1`, `[::1]:8080`) goes down the wrong branch.
2. In that branch, `socket.split(':').collect::<Vec<_>>()` and `match len { 2 => …, _ => error("more than one colon") }` — guarantees rejection of every legal IPv6 literal.
3. Even in the "numeric" branch, `SocketAddr::from_str("[::1]:8080")` works, but only if it gets there — and it doesn't because step (1) routes hex-letter addresses elsewhere.

Plus: it never resolves the hostname (even a sanity-check resolution would help), so `--receive-tcp-acars not.a.host:8080` passes the checker.

### 3.3 TCP receiver hard-fails on every hostname

> **Status:** resolved in PR4. `TCPReceiverServer::run` now resolves `host:port` through the shared `hickory_resolver::TokioResolver` before handing the `SocketAddr` to `StubbornTcpStream::connect_with_options`.

`rust/libraries/acars_connection_manager/src/tcp_services.rs:177`:

```rust
let addr = match self.host.parse::<SocketAddr>() {
    Ok(addr) => addr,
    Err(e) => {
        error!(... "Error parsing host: {}" ...);
        return Ok(());
    }
};
```

`SocketAddr::from_str` does **not** resolve hostnames. So `--receive-tcp-acars host.example.com:8080` silently exits the task with a logged error, never connects, and the user never finds out why no data is flowing. Compare with the UDP sender, which goes through `to_socket_addrs()` (also wrong, see §3.4) at least _some_ of the time. Fix: `tokio::net::lookup_host(&self.host).await?.next()`, or better, a real resolver (§3.5).

### 3.4 Blocking DNS on the tokio runtime (your point c)

> **Status:** resolved in PR4. `UDPSenderServer::send_bytes` now calls the async `dns::resolve_host_port` instead of the blocking `std::net::ToSocketAddrs::to_socket_addrs()`. The existing per-target TTL cache (`last_success` / `dns_cache_duration`) is preserved and runs in front of hickory's own internal cache. PR10 additionally bumped the default TTL from 15s → 1800s (30 min) and added failure-driven invalidation: any `sendto()` error clears that destination's cached `SocketAddr` so the next message re-resolves regardless of TTL. The "retry resolution on every send while DNS is broken" pathology noted below is **deliberately retained** per owner direction — when DNS is down the user wants recovery the moment it comes back, not a back-off.

`rust/libraries/acars_connection_manager/src/udp_services.rs:204` calls `ra.addr.to_socket_addrs()` — that's `std::net::ToSocketAddrs::to_socket_addrs`, which is **blocking** and goes through the system resolver. This is called from `send_bytes`, which is an `async fn` executed on the tokio worker pool. Every cache miss therefore blocks a runtime worker for however long getaddrinfo takes (can be many seconds under failure).

Even when the cache is warm, this is the only place in the whole program that does any kind of cache. `tcp_services` and `service_init` both hand a hostname string to `StubbornTcpStream::connect_with_options(host.to_string(), …)` (`service_init.rs:662`); `stubborn-io` will resolve fresh on every reconnect attempt with no caching of its own.

**Recommendation:** consolidate ALL outbound DNS in one place. Use `hickory-resolver` with its built-in cache (replaces `trust-dns-resolver`), expose it as an `Arc<TokioResolver>`, and have every TCP/UDP sender path go through it. Configure the cache TTL from `--udp-dns-cache-seconds` (rename to `--dns-cache-seconds`). Side benefit: kills the bespoke `ResolvedAddr` struct and fixes the "if first resolution fails, retry on every single packet" pathology at `udp_services.rs:199-233` (`if ra.resopt.is_none() || elapsed > ttl` re-resolves forever on failure with no backoff).

Side note: `udp_services.rs:207` filters out non-IPv4 addresses (`if res.is_ipv4()`). That decision should be configurable; some users _do_ deploy v6-only.

### 3.5 The fan-out lock is held across awaits

`rust/libraries/acars_connection_manager/src/service_init.rs:646-657`:

```rust
async fn monitor(self, mut rx_processed: Receiver<AcarsVdlm2Message>, name: &str) {
    while let Some(message) = rx_processed.recv().await {
        for sender_server in self.lock().await.iter() {
            match sender_server.send(message.clone()).await { … }
        }
    }
}
```

The `Mutex<Vec<Sender<…>>>` is locked **for the entire duration of sending to every downstream**. A single slow consumer pauses _all_ output (and prevents new senders from being added). Also: `message.clone()` once per consumer is a deep clone of an `AcarsVdlm2Message`.

Same issue in `tcp_services.rs:413-425` (`handle_message`): `state.lock().await.broadcast(&message).await` locks `Shared` for the whole peer iteration, where `broadcast` does `let _ = peer.1.send(message.into())` per peer — and silently ignores `SendError` (so dead peers are never reaped → memory leak; the `peers` HashMap only shrinks when the peer task itself notices disconnect).

**Recommendation:** replace the entire `Arc<Mutex<Vec<Sender>>>` + `Arc<Mutex<Shared>>` fan-out with `tokio::sync::broadcast` (clones the message ref into a ring buffer; slow consumers `Lagged` instead of stalling fast ones). For per-peer queues, prefer a bounded `mpsc` and a janitor task that prunes when `try_send` returns `Full`/`Closed`.

### 3.6 Unbounded per-peer channel

`tcp_services.rs:364`: `let (tx, rx) = mpsc::unbounded_channel();` for each `Peer`. A slow / hung TCP client → memory grows without bound while the producer keeps pushing. With `broadcast` (above) this evaporates.

### 3.7 Reassembly locks the mutex 4× per message

`rust/libraries/acars_connection_manager/src/packet_handler.rs:80-159`:

```rust
self.clean_queue().await;                                    // lock 1 (also itself locks)
if let Ok(msg) = new_message_string.decode_message() {
    self.queue.lock().await.remove(&peer);                   // lock 2
    return Some(msg);
}
...
if self.queue.lock().await.contains_key(&peer) {             // lock 3
    let (time, message_to_test) = self.queue.lock().await    // lock 4 (.unwrap()!)
        .get(&peer).unwrap().clone();
    ...
}
match output_message {
    Some(_) => { self.queue.lock().await.remove(&peer); }    // lock 5
    None => { self.queue.lock().await.insert(...); }         // lock 6
}
```

The author's own comments (`// FIXME: Ideally this entire function should not lock the mutex all the time`) acknowledge the smell. The `.unwrap()` between `contains_key` and `get` is a TOCTOU panic waiting to happen if two messages from the same peer race. Replace with a single `let mut q = self.queue.lock().await;` at function top and an `Entry` API.

Also: `PacketHandler` is one per listener, but the underlying receive loops are single-tasked, so the mutex is effectively pointless contention — a `RefCell` (or just `&mut self`) would do, if the struct were redesigned.

### 3.8 Dedupe queue is O(N) per message

`rust/libraries/acars_connection_manager/src/message_handler.rs:296-321`:

```rust
for (time, hashed_value_saved) in dedupe_queue_loop.lock().await.iter() {
    ...
}
```

Linear scan of the entire window for every message. For a 2-second window at high message rates this is small; for any larger window it's quadratic in the window size. Use `HashSet<u64>` keyed by hash with parallel `VecDeque<(u64,u64)>` for expiry, or a proper `lru` / `moka` cache with TTL.

The whole `clean_up_dedupe_queue` background task can also disappear if entries are expired lazily on insert.

### 3.9 Hashing path serialises JSON to a `String`

`message_handler.rs:505-528`: `message.to_string()` (i.e. `serde_json::to_string`) just to feed bytes into `DefaultHasher`. Use `serde_json::to_writer(&mut HashWriter(&mut hasher), &message)` (or hash a stable canonical form). Currently every non-duplicate allocates a fresh `String` of the entire message just to discard it.

Also: `DefaultHasher` is `SipHash-1-3`, but using `std::collections::hash_map::DefaultHasher` directly is explicitly discouraged. Use `ahash`/`xxhash` for non-cryptographic dedupe, or `siphasher` directly with a fixed seed if you care about cross-run stability.

### 3.10 Frequency table is `Vec<FrequencyCount>` with linear search ×5

`message_handler.rs:156-263`: five near-identical 16-line blocks, each doing:

```rust
let mut found = false;
for freq in all_frequencies_logged.lock().await.iter_mut() {
    if freq.freq == frequency { freq.count += 1; found = true; break; }
}
if !found { all_frequencies_logged.lock().await.push(FrequencyCount { ... }); }
```

Two lock acquisitions per message (one held across the search, then dropped, then a new one for the push). Use `HashMap<String, u64>` (or `DashMap` to remove the mutex). Better: extract a helper `record_frequency(&self, freq: &str)`.

### 3.11 Counters use `Arc<Mutex<i32>>`

`message_handler.rs:74-75, 87-89, 132-133, 427-430`. `*counter.lock().await += 1` per message. Use `Arc<AtomicU64>` with `Relaxed`. Removes 2 locks per message plus the print-stats path.

### 3.12 `main` polls every 100ms forever

```rust
// main.rs / service_init.rs:303-307
loop { sleep(Duration::from_millis(100)).await; }
```

This is the program's only "stay alive" mechanism. There is **no shutdown signal handling**, **no `JoinSet`** tracking the spawned tasks, and **no way for a child task's panic / fatal error to terminate the process** — it just gets logged and the runtime keeps idling. On `SIGTERM` the process is killed mid-flight, peers see RSTs, dedupe state is lost.

Replace with:

- A `tokio_util::sync::CancellationToken` passed to each spawned task.
- `tokio::signal::ctrl_c()` / `SIGTERM` handler triggers cancel.
- A `tokio::task::JoinSet` so the main task `join_next().await`s and exits cleanly when all children settle, or escalates on first error.

### 3.13 Other smaller issues

- `service_init.rs:594` (`Shared::new` for every TCP serve port) creates **a separate `Shared` state per `serve_tcp_*` port**. So a TCP client connected to `:15550` for ACARS never sees a peer connected to `:15551` for ACARS — fine, that's intentional. But a single connected client on `:15550` who subscribes early gets the message a second time after another peer joins? Actually no, the design's OK; mentioning because the wiring is subtle and undocumented.
- `service_init.rs:707-711` (`SocketListenerServer::run`): parses `"0.0.0.0"` via `IpAddr::from_str` and propagates the error — but `"0.0.0.0"` is a constant. Use `Ipv4Addr::UNSPECIFIED`. This is dead code so it doesn't matter, but it's emblematic.
- `tcp_services.rs:209-212`: `TcpKeepalive::new().with_time(5s).with_interval(5s)` — no `with_retries(...)`. The keepalive defaults differ across OS. Set them explicitly.
- `tcp_services.rs:217`: `LinesCodec::new()` (no max-length) on the _receiver_ side; `tcp_services.rs:104` does set `new_with_max_length(8000)` on the _listener_ side. Inconsistent. A malicious upstream feeding a never-newline can OOM the receiver.
- `udp_services.rs:61, 812`: buffer fixed at 5000 bytes — but `--max-udp-packet-size` defaults to 60000 for _sending_. Receive buffer is implicitly smaller than sender's expected MTU.
- `udp_services.rs:241-285`: sender sleeps 100ms between MTU-sized chunks. That's not backpressure; it's a fixed cap on output rate per destination. If a message is split into N fragments you'll spend `(N-1)*100ms` regardless of network. Use socket backpressure (`send_to` returns when kernel buffer empties).
- `message_handler.rs:147-150`: `match SystemTime::now().duration_since(UNIX_EPOCH) { Ok(n) => n.as_secs_f64(), Err(_) => f64::default() }` — silently treats a clock skew error as time = 0.0, which then causes every message to look ancient and get rejected by the skew check. Just `.expect("system clock before unix epoch")` or propagate.
- `message_handler.rs:485-488`: same pattern, returns 0 on error → `current_time - timestamp` underflows on `u64`. Panic in debug, wraparound in release.
- `Input::print_values` (`acars_config/src/lib.rs:274-337`): 64 hand-maintained `debug!` lines. `#[derive(Debug)]` already exists on `Input`; just `debug!("Config: {self:#?}")`. The hand-maintained list is also already drifting from the field list (no `AR_DISABLE_*` for some, etc.). **Status: resolved in PR3** — collapsed to `debug!("Configuration: {self:#?}");`.

---

## 4. Architectural recommendations (your point d)

The structure is the way it is because the codebase models the _physical layout of the CLI flags_ rather than the _domain_. The domain has 5 (or N) protocols, each of which has the same set of inputs and outputs.

### 4.1 Introduce a `Protocol` enum and a `ProtocolConfig` struct

```rust
#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash, clap::ValueEnum, strum::EnumIter, strum::Display)]
pub enum Protocol { Acars, Vdlm2, Hfdl, Imsl, Irdm }

#[derive(Clone, Debug, Default)]
pub struct ProtocolIo {
    pub listen_udp: Vec<u16>,
    pub listen_tcp: Vec<u16>,
    pub listen_zmq: Vec<u16>,
    pub receive_tcp: Vec<String>,
    pub receive_zmq: Vec<String>,
    pub send_udp:    Vec<String>,
    pub send_tcp:    Vec<String>,
    pub serve_tcp:   Vec<u16>,
    pub serve_zmq:   Vec<u16>,
    pub disabled:    bool,
}

pub struct Config {
    pub global: GlobalConfig,
    pub protocols: EnumMap<Protocol, ProtocolIo>,
}
```

(`enum_map`/`strum` for the iterations.) **Every** site in §3.1, §4.2-4.5, and most of `message_handler.rs::watch_message_queue` then collapses to a single `for proto in Protocol::iter()` loop.

For the CLI, you have two reasonable options:

- Keep the 50 individual `--listen-udp-acars` flags and just have a private `From<Input> for Config` that fans them into the enum map. Existing users / env vars stay backward compatible.
- Take a single `--config <file>` (TOML/YAML/JSON) and deprecate the matrix of flags. Long-term win, short-term migration cost.

### 4.2 Collapse `start_processes`

`service_init.rs:45-308` is one function, **263 lines**, made of **5 near-identical 50-line blocks**. After §4.1 it is roughly:

```rust
pub async fn start_processes(cfg: Config, cancel: CancellationToken) -> Result<()> {
    let mut tasks = JoinSet::new();
    for (proto, io) in cfg.protocols.iter().filter(|(_, io)| io.is_configured()) {
        let (tx_in,  rx_in)  = mpsc::channel(BUFFER);
        let (tx_out, rx_out) = mpsc::channel(BUFFER);
        spawn_listeners(&mut tasks, proto, io.clone(), tx_in, cancel.clone());
        spawn_handler  (&mut tasks, proto, cfg.global.clone(), rx_in, tx_out, cancel.clone());
        spawn_senders  (&mut tasks, proto, io.clone(), rx_out, cancel.clone());
    }
    drain(tasks, cancel).await
}
```

### 4.3 Collapse `Input::*_configured`

Replace the 5 nearly-identical `fn acars_configured / vdlm_configured / hfdl_configured / imsl_configured / irdm_configured` methods with one method on `ProtocolIo`:

```rust
impl ProtocolIo {
    pub fn is_configured(&self) -> bool {
        !self.disabled && (
            !self.listen_udp.is_empty() || !self.listen_tcp.is_empty() ||
            !self.listen_zmq.is_empty() || !self.receive_tcp.is_empty() ||
            !self.receive_zmq.is_empty() || !self.send_udp.is_empty() ||
            !self.send_tcp.is_empty()    || !self.serve_tcp.is_empty() ||
            !self.serve_zmq.is_empty()
        )
    }
}
```

### 4.4 Collapse the sanity checker

After §4.1, all of §3.1 / §3.2 turn into:

```rust
pub fn validate(&self) -> Result<(), Vec<ValidationError>> {
    let mut errs = vec![];
    let mut bound_udp = HashSet::new();
    let mut bound_tcp_zmq = HashSet::new();
    for (proto, io) in &self.protocols {
        check_ports(proto, "listen_udp", &io.listen_udp, &mut bound_udp,    &mut errs);
        check_ports(proto, "listen_tcp", &io.listen_tcp, &mut bound_tcp_zmq,&mut errs);
        ...
    }
    if errs.is_empty() { Ok(()) } else { Err(errs) }
}
```

…and you can return _which_ errors failed (currently the user just gets `"Config option sanity check failed, with 3 total errors"`). Also kill the entire `Vec<bool>` / `validate_results` apparatus.

### 4.5 Collapse the frequency-recording switch in `message_handler.rs`

`AcarsVdlm2Message` should expose a `fn frequency(&self) -> Option<Cow<'_, str>>`; if not patchable upstream, define an extension trait. Then the five copy-pasted blocks (§3.10) become:

```rust
if let Some(freq) = message.frequency() {
    record_frequency(&all_frequencies_logged, &freq).await;
}
```

### 4.6 Use `tracing` not `log`

You already enable `tokio` tracing. Adopt `tracing` + `tracing-subscriber`, attach a `Span` per `(proto, transport, peer)`, drop the `[ACARS UDP LISTENER 0.0.0.0:5550]` string interpolation that's manually re-built dozens of times.

### 4.7 Unified DNS

One `DnsResolver` (hickory) wrapped in `Arc`, passed into every sender (TCP, UDP, sender that reconnects via stubborn-io). One TTL knob (`--dns-cache-ttl`). One metrics counter for cache hits/misses. See §3.4.

### 4.8 Unified message-splitting

The "split by `\n`, then by `}{`, then re-bracket first/middle/last" logic appears in **four places**: `tcp_services.rs:104-150`, `tcp_services.rs:220-283` (with a "FIXME: This feels very non-rust idiomatic" comment of its own), `udp_services.rs:93-139`, `service_init.rs:751-806` _and_ `service_init.rs:869-922` (the latter two are inside the dead `SocketListenerServer`). They are not identical — drift has already happened. Extract `fn split_concatenated_json(buf: &str) -> impl Iterator<Item = String>`.

### 4.9 Bounded backpressure end-to-end

Replace every `mpsc::channel(32)` magic number with a single `BUFFER` constant (or a config knob), and replace every `mpsc::unbounded_channel` with bounded. Wire `tokio::sync::broadcast` for fan-out (§3.5). This is the single highest-leverage performance and correctness change.

---

## 5. Style nits worth a sweep

Most of these come for free with `clippy::pedantic` + `nursery`.

- `match self.contains(&false) { true => false, false => true }` (`sanity_checker.rs:257-263`) → `!self.contains(&false)`.
- `match value.eq(&0)` and `length.eq(&1)` everywhere (`sanity_checker.rs:280`, `tcp_services.rs:114`, `udp_services.rs:99`, etc.) → `== 0` / `== 1`. `eq` is here exclusively to satisfy a long-fixed clippy lint.
- `format!("{}{}", "}", x)` (`udp_services.rs:116, 123, 130`; `tcp_services.rs:243, 250, 257`) → `format!("}}{x}")` / `format!("{{{x}}}")`. The current form was a workaround for old Rust not allowing `{{` inside `format!`, which has not been true for many years.
- `Vec<bool>` accumulator (`sanity_checker.rs:28-46`, `136-163`, `167-196`, `74-77`, `100`) — better expressed as `errs: Vec<ValidationError>` and `if errs.is_empty()`.
- Manual `let mut found = false; for x in … { if … { found = true; break; } }` — `if vec.iter().any(|x| …)`.
- `let config: Input = self.clone();` (`sanity_checker.rs:83`) followed by `if let Some(config_hosts) = config.receive_tcp_acars { … }` — deep-cloning the whole `Input` to consume its fields. Use `&self`/`.as_ref()`.
- `pub fn check_host(self, …)` (`sanity_checker.rs:238`) — consumes `self` for a read-only validation.
- `let new_channel: Sender<String> = channel.clone();` — explicit types are noise; `let new_channel = channel.clone();`. There are dozens of these per file.
- `let proto_name: String = format!(...)` — same.
- `Box<dyn Error>` returns everywhere. Define `thiserror::Error` types (e.g. `RouterError`, `ListenerError`); use `anyhow::Result` only at the `main` boundary.
- `process::exit(1)` from `main` — return `Err(...)` instead so destructors run.
- `extern crate sdre_rust_logging;` is unused beyond `use sdre_rust_logging::SetupLogging;`. Delete.
- `let _ = peer.1.send(...)` (`tcp_services.rs:349`) — explicit "I'm ignoring this error" merits a comment, or better: handle it.
- `format!("0.0.0.0:{}", port)` everywhere — use `SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), port)`. Also: zero IPv6 support anywhere (`::` bind would let `acars_router` serve both stacks on dual-stack OS).
- `let socket = UdpSocket::bind(addr).await; match socket { Err(e) => …, Ok(socket) => { … all the work … } }` (`udp_services.rs:64-144`, `service_init.rs:540-630`) — invert with `?` and let the `match` go.
- `args.udp_dns_cache_seconds: f64` — `f64`-seconds for what should be `Duration` (or at worst u64 ms). Same for `args.reassembly_window`, `args.skew_window`, `args.dedupe_window` — all of them should arrive as `humantime::Duration` (clap supports this).
- `stats_every: u64` is "minutes" and immediately gets multiplied by 60 (`message_handler.rs:80`) — `Duration` would document the unit.
- `MessageHandlerConfig::new` (`message_handler.rs:38-65`) writes the entire struct twice, once for "with station name", once "without". Use `Default::default()` and one conditional assignment.
- `let queue_type_stats: String = self.queue_type.clone(); let queue_type_dedupe: String = self.queue_type.clone();` — three string clones for one logical name. Use `Arc<str>`.
- `BufReader::new(stream)` then `Framed::new(reader, LinesCodec::new())` (`tcp_services.rs:216-217`) — `Framed` already buffers. The `BufReader` is redundant.
- `ZMQReceiverServer` does `composed_message.iter().map(...).collect::<Vec<_>>().join(" ")` and then `strip_suffix("\r\n").or_else(strip_suffix('\n'))` — both `join` and `strip_suffix` allocate. Trim once at the end.
- `Cargo.toml` has commented-out `acars_vdlm2_parser` git branches — delete from main.

---

## 6. Tests & docs

- 4 unit tests total across the workspace (3 in `sanity_checker.rs`, 1 in `message_handler.rs`). Of those, none cover the actual routing pipeline. The `tcp_services` / `udp_services` / `zmq_services` / `packet_handler` / `service_init` modules have **zero** test coverage. With `Protocol` introduced (§4.1) you could property-test the splitter (§4.8) and the validator (§4.4) easily.
- ~~No integration tests despite `test_data/` directory and `source-stubs/` (which appear to be Python harnesses). Wire them into `cargo test` (with `#[ignore]` for the slow/network ones) so they don't bit-rot.~~ PR9: deleted both directories entirely (Python/shell scaffolding bit-rotted and was never invoked by CI). The four JSON corpora moved to `rust/libraries/acars_connection_manager/tests/fixtures/*.jsonl` and now drive Rust-native corpus tests for `packet_handler` and `message_handler`.
- Public items (`TCPReceiverServer`, `TCPServeServer`, `ZMQReceiverServer`, `ZMQListenerServer`, `print_stats`, `print_formatted_stats`, etc.) have no rustdoc, or have rustdoc that documents the _type name verbatim_ ("TCP Receiver server. This is used to ..." copy-paste between struct and impl). With `missing_docs = "warn"` enabled (§2.2) this becomes a forcing function.

---

## 7. Suggested order of work

1. **Lints + edition bump.** Add `[workspace.lints]`, `[lints] workspace = true`, edition 2024, modern MSRV, `-D warnings` in CI. Fix what falls out (mostly mechanical, see §5). Result: ~150 LOC removed, no behaviour change.
2. **Kill `extern crate` and `#[macro_use]`.** Choose `log` _or_ `tracing`. Delete dead `SocketListenerServer`. Delete commented git deps. Result: ~300 LOC removed.
3. **Sanity checker bug fixes & coverage of HFDL/IMSL/IRDM.** §3.1, §3.2. Smallest user-visible bug fix that doesn't require restructuring.
4. **Unified DNS resolver.** §3.4, §3.3. Single hickory resolver; thread through all senders. Resolves the "we hit DNS a lot" concern definitively.
5. **`Protocol` enum / `ProtocolIo` struct refactor.** §4.1-4.5. Removes the bulk of the boilerplate; pre-condition for the next two items.
6. **`broadcast` + `JoinSet` + `CancellationToken`.** §3.5, §3.6, §3.12. Real backpressure + real shutdown.
7. **`packet_handler` rewrite.** §3.7, §4.8. Single splitter, single lock.
8. **Dedupe / counters / freq table on atomics / hashmap.** §3.8, §3.10, §3.11. Performance polish.
9. **Tests + docs.** §6.

Items 1-4 are mostly mechanical and unlock cleaner reviews of 5-8. Items 5-7 are the structural change you asked for.

---

## 8. Things that are good (credit where due)

- The CLI is comprehensive and documented inline; user-facing UX is consistent.
- Workspace layout (one bin, two libs) is clean even if the libs themselves need internal restructuring.
- TCP keepalive _is_ set on outbound receiver sockets (`tcp_services.rs:206-212`) — many people forget this entirely. Just needs to be more configurable.
- `stubborn-io` for auto-reconnect is the right call; you didn't roll your own reconnect-with-backoff (sort of — the `get_our_standard_reconnect_strategy` with 14 entries of `Duration::from_secs(5)` is clearly hand-written and should be `(0..14).map(|_| Duration::from_secs(5))`, but the idea is correct).
- Dockerfile uses `--mount`-friendly layout and a multi-stage build; image is small.
- The pre-commit / nix flake setup means the lint changes in step 1 will actually be enforced locally.

---

_End of report._

---

# Appendix A — `sdre-stubborn-io` review (as it affects acars_router)

Read of `~/GitHub/sdre-stubborn-io` at the version vendored by acars_router (workspace dep `sdre-stubborn-io = "0.6.19"`; local checkout is `0.6.14` but the relevant surface is unchanged). Only items that block / would improve acars_router are flagged here; a fuller audit of the crate on its own merits is out of scope for this report.

## A.1 The DNS root cause lives here

`src/tokio/tcp.rs:7-14`:

```rust
impl<A> UnderlyingIo<A> for TcpStream
where A: ToSocketAddrs + Sync + Send + Clone + Unpin + 'static,
{
    fn establish(addr: A) -> Pin<Box<dyn Future<Output = io::Result<Self>> + Send>> {
        Box::pin(TcpStream::connect(addr))
    }
}
```

`establish` receives the ctor arg, calls `tokio::net::TcpStream::connect(addr)`, which calls `getaddrinfo` (via `tokio::net::lookup_host`). When acars*router passes a `String` like `"feed.example.com:5550"`, **every reconnect attempt does a fresh DNS lookup**. With acars_router's reconnect strategy of 14× 5 seconds, then up to 60s, against a transiently-failing destination, this hits DNS \_constantly*. There is no caching layer anywhere in stubborn-io.

Two things follow.

### A.1.1 What stubborn-io could do for everyone

Add a _context_ to `establish` so resolvers / configs / metrics can be threaded in:

```rust
pub trait UnderlyingIo<C>: Sized + Unpin
where C: Clone + Send + Unpin
{
    type Context: Clone + Send + Sync = ();   // assoc-type default

    fn establish(
        ctor_arg: C,
        ctx: Self::Context,
    ) -> Pin<Box<dyn Future<Output = io::Result<Self>> + Send>>;
    ...
}
```

Then acars_router (or any user) can stash an `Arc<DnsResolver>` (e.g. `hickory_resolver::TokioResolver`) in `Context`, and a custom `UnderlyingIo<String>` impl resolves via cache before connect. Backwards-compatible default = `()`.

This is the single most impactful structural change.

### A.1.2 What acars_router can do today, without changes to stubborn-io

The trait is already user-extensible. acars_router should define its own underlying type and stop using `StubbornTcpStream<String>`:

```rust
struct CachedDnsTcp { stream: TcpStream }
struct ConnectTarget { host: Arc<str>, resolver: Arc<TokioResolver> }
impl Clone for ConnectTarget { ... }

impl UnderlyingIo<ConnectTarget> for CachedDnsTcp {
    fn establish(t: ConnectTarget) -> Pin<Box<dyn Future<Output = io::Result<Self>> + Send>> {
        Box::pin(async move {
            let addr = t.resolver.lookup_socket(&t.host).await?;
            let stream = TcpStream::connect(addr).await?;
            Ok(CachedDnsTcp { stream })
        })
    }
}
type StubbornCachedTcp = StubbornIo<CachedDnsTcp, ConnectTarget>;
```

That's it. No upstream PR required; the `String`-based `StubbornTcpStream` alias just shouldn't be used by acars_router going forward.

## A.2 Missing connect timeout

`tcp.rs:12`: `TcpStream::connect(addr)` has no timeout. On a SYN-blackhole, the OS default (~75 s on Linux, longer on others) governs. acars_router has no other guard. Two reconnects in a row to a dead peer can stall a producer for minutes.

Add `ReconnectOptions::with_connect_timeout(Duration)` and wrap `T::establish(ctor_arg).await` in `tokio::time::timeout(...)` at both call sites (`io.rs:167, 212, 296`). Trivial change; large operational win.

## A.3 Disconnect-detection allow-list is wrong for TCP

`io.rs:26-42`:

```rust
matches!(err.kind(),
    NotFound | PermissionDenied | ConnectionRefused | ConnectionReset
    | ConnectionAborted | NotConnected | AddrInUse | AddrNotAvailable
    | BrokenPipe | AlreadyExists)
```

- **Missing**: `TimedOut`, `HostUnreachable`, `NetworkUnreachable`, `NetworkDown`, `Interrupted` (debatable). A flapping route therefore does _not_ trigger reconnect for acars_router → stream looks dead, no recovery.
- **Spurious for TCP**: `NotFound`, `AlreadyExists`, `PermissionDenied` — these are file/POSIX-style errors that don't occur on a `TcpStream`. Looks like leftover File-impl precedent. Harmless but confusing.

The whole policy should be virtual on a per-`UnderlyingIo` basis (it already is, technically — `is_disconnect_error` is overridable), but the _default for TcpStream_ should be its own list and not the trait default. Override in `impl UnderlyingIo<A> for TcpStream`.

## A.4 Callback API is too weak for cache eviction / metrics

`config.rs:20-26`:

```rust
pub on_connect_callback:     Box<dyn Fn() + Send + Sync>,
pub on_disconnect_callback:  Box<dyn Fn() + Send + Sync>,
pub on_connect_fail_callback:Box<dyn Fn() + Send + Sync>,
```

- `Fn()` — no parameters, no return. Cannot tell which target disconnected (only one connection name string fixed at construction). With acars_router running dozens of these, metrics like "reconnect rate per destination" require shared state and a closure per call site to disambiguate.
- `Fn` not `FnMut` — forces interior mutability for any state update.
- No `on_reconnect_attempt(attempt_num)` hook — that's the natural hook for "invalidate DNS cache entry before retrying" if the integration goes the upstream-fix route (A.1.1).

Suggested replacement:

```rust
#[non_exhaustive]
pub enum ReconnectEvent<'a> {
    Connected, Disconnected, ConnectFailed { err: &'a io::Error, attempt: usize },
    ReconnectScheduled { attempt: usize, delay: Duration },
}
pub on_event: Arc<dyn Fn(ReconnectEvent<'_>) + Send + Sync>,
```

Single field, typed events, `Arc` for cheap clone, future-proof via `#[non_exhaustive]`.

## A.5 `block_on_write_failures = false` silently drops data

`io.rs:412-419` and `io.rs:498-512` (vectored): when a write detects a disconnect and the option is false, the impl returns `Poll::Ready(Ok(buf.len()))` — i.e. lies to the caller about how many bytes were written. Comment says "the write is skipped. No error is returned to the caller." That's a footgun. acars_router uses this default (the option is never set), so every `tcp_services.rs:301` `socket.write_all(&encoded_message).await` can silently drop full ACARS messages with only an `error!` log.

Recommendations, in order of preference:

1. Flip the default to `true`. The current default is the dangerous one.
2. At minimum, emit an `error!` _plus_ a hookable event (A.4) so acars_router can increment a "dropped" counter.
3. Document this loudly in the rustdoc on `with_block_on_write_failures` (currently the rustdoc on the _field_ is one line and the _builder_ is zero).

For acars_router specifically: until this is fixed, set `.with_block_on_write_failures(true)` in `acars_connection_manager::reconnect_options`.

## A.6 Smaller cleanups (won't move the needle for acars_router but they're free wins)

- `io.rs:76`: `reconnect_attempt: Box::pin(async { unreachable!("Not going to happen") })` — placeholder future stored unconditionally. Use `Option<Pin<Box<...>>>` and unwrap at the use sites; the `unreachable!()` is a structural smell.
- `io.rs:132-144`: `FormatName` trait implemented only for `String`, used only by one private method. Just inline it as `fn formatted_name(&self) -> String`. Also: it allocates `"StubbornIo({}): "` on **every log line**. Cache the formatted name as `Arc<str>` in `ReconnectOptions` at construction and clone-by-ref.
- `io.rs:269-271`: two clones of `connection_name` (`String`) per `on_disconnect`. With `Arc<str>` (above) becomes free.
- `config.rs:39-49`: `#[allow(clippy::new_without_default)]` + `pub fn new()` — just `derive Default` after the callbacks become `Arc<dyn Fn(...)>` (or keep `Box` but implement `Default` manually).
- `config.rs:43`: default of `exit_if_first_connect_fails = true` is hostile for the dominant "retry forever" case. acars_router overrides this on **every** call site (`acars_connection_manager/src/lib.rs:104`). Flip the default.
- `io.rs:177`: `error!` on initial connect failure when `exit_if_first_connect_fails = false` — that's the _expected_ path. Use `warn!` (or `info!`).
- `lib.rs:9`: rustdoc claims MSRV 1.39; `tokio = "1.48"` requires much newer. Update doc; add `rust-version = "..."` to `Cargo.toml`. (Same `[workspace.lints]` recommendation as acars_router — there's none here either.)
- No `cargo deny` / `cargo audit`. `rand` 0.9 was just adopted (PR appears in `strategies.rs` via `from_os_rng()` / `random::<f64>()`), keep that consistent.
- Public API surface: there's no way to query "am I currently `FailedAndExhausted`?" from outside. `is_terminated() -> bool` would let acars_router know to give up and spawn a fresh connection task rather than holding a dead handle.

## A.7 Net effect on the acars_router remediation plan

In §3.4 / §4.7 of the main report I recommended introducing a single `hickory-resolver`-based DNS cache shared by all senders. With the existing stubborn-io API that means doing (A.1.2) — a custom `UnderlyingIo` impl in acars_router itself. That's ~30 lines of code and zero upstream churn; it should be the chosen path.

The upstream stubborn-io changes worth doing (in priority order):

1. **A.2** connect timeout — operational must-have for everyone, not just acars_router.
2. **A.5** flip `block_on_write_failures` default to `true` — silent data loss is unacceptable as a default.
3. **A.3** broaden TCP disconnect-error allow-list.
4. **A.1.1** context-injection for `establish` — cleanest long-term answer to DNS caching for _any_ user, not just acars_router rolling its own.
5. **A.4** typed event callback.

If you only do #1 and #2, acars_router gets meaningful resilience improvements with the smallest blast radius. The DNS work is best done inside acars_router via (A.1.2).

_End of appendix._

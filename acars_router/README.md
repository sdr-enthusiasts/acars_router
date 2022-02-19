# ACARS Router

## Runtime Configuration

`acars_router` can be configured via command line arguments or environment variables. Command line arguments take preference over environment variables.

When using environment variables use `;` to separate entries, for example: `AR_SEND_UDP_ACARS="1.2.3.4:5550;5.6.7.8:5550"`

### Input

#### ACARS

| Argument | Environment Variable | Description | Default |
| -------- | -------------------- | ----------- | --------|
| `--listen-udp-acars` | `AR_LISTEN_UDP_ACARS` | UDP port to listen for ACARS messages. Can be specified multiple times to listen on multiple ports. | |
| `--listen-tcp-acars` | `AR_LISTEN_TCP_ACARS` | TCP port to listen for ACARS messages. Can be specified multiple times to listen on multiple ports. | |
| `--receive-tcp-acars` | `AR_RECV_TCP_ACARS` | Connect to "host:port" (over TCP) and receive ACARS messages. Can be specified multiple times to receive from multiple sources. | |

#### VDLM2

| Argument | Environment Variable | Description | Default |
| -------- | -------------------- | ----------- | --------|
| `--listen-udp-vdlm2` | `AR_LISTEN_UDP_VDLM2` | UDP port to listen for VDLM2 messages. Can be specified multiple times to listen on multiple ports. | |
| `--listen-tcp-vdlm2` | `AR_LISTEN_TCP_VDLM2` | TCP port to listen for VDLM2 messages. Can be specified multiple times to listen on multiple ports. | |
| `--listen-udp-vdlm2` | `AR_LISTEN_UDP_VDLM2` | Connect to "host:port" (over TCP) and receive VDLM2 messages. Can be specified multiple times to receive from multiple sources. | |

### Output

#### ACARS

| Argument | Environment Variable | Description | Default |
| -------- | -------------------- | ----------- | --------|
| `--send-udp-acars` | `AR_SEND_UDP_ACARS` | Send ACARS messages via UDP datagram to `host:port`. Can be specified multiple times to send to multiple clients. | |
| `--send-tcp-acars` | `AR_SEND_TCP_ACARS` | Send ACARS messages via TCP to `host:port`. Can be specified multiple times to send to multiple clients. | |
| `--serve-tcp-acars` | `AR_SERVE_TCP_ACARS` | Serve ACARS messages on TCP `port`. Can be specified multiple times to serve on multiple ports. | |

#### VDLM2

| Argument | Environment Variable | Description | Default |
| -------- | -------------------- | ----------- | --------|
| `--send-udp-vdlm2` | `AR_SEND_UDP_VDLM2` | Send VDLM2 messages via UDP datagram to `host:port`. Can be specified multiple times to send to multiple clients. | |
| `--send-tcp-vdlm2` | `AR_SEND_TCP_VDLM2` | Send VDLM2 messages via TCP to `host:port`. Can be specified multiple times to send to multiple clients. | |
| `--serve-tcp-vdlm2` | `AR_SERVE_TCP_VDLM2` | Serve VDLM2 messages on TCP `port`. Can be specified multiple times to serve on multiple ports. | |

### Logging

| Argument | Environment Variable | Description | Default |
| -------- | -------------------- | ----------- | --------|
| `--stats-every` | `AR_STATS_EVERY` | Print statistics every `N` minutes | `5` |
| `-v` `--verbose` | `AR_VERBOSITY` | Increase log verbosity. `-v`/`AR_VERBOSITY=1` = Debug. `-vv`/`AR_VERBOSITY=2` = Trace (raw packets printed) | `0` (info) |

### Advanced

| Argument | Environment Variable | Description | Default |
| -------- | -------------------- | ----------- | --------|
| `--skew-window` | `AR_SKEW_WINDOW` | Reject messages with a timestamp greater than +/- this many seconds. | 1 |
| `--threads-json-deserialiser` | `AR_THREADS_JSON_DESERIALISER` | Number of threads for JSON deserialisers (per message protocol) | Number CPU cores |
| `--threads-hasher` | `AR_THREADS_HASHER` | Number of threads for message hashers (per message protocol) | Number CPU cores |
| `--threads-output-queue-populator` | `AR_OUTPUT_QUEUE_POPULATOR` | Number of threads for output queue populators (per message protocol) | Number CPU cores |

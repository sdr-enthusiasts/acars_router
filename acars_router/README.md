# ACARS Router

## Runtime Configuration

`acars_router` can be configured via command line arguments or environment variables. Command line arguments take preference over environment variables.

When using environment variables use `;` to separate entries, for example: `AR_SEND_UDP_ACARS="1.2.3.4:5550;5.6.7.8:5550"`

| Argument | Environment Variable | Description | Default |
| -------- | -------------------- | ----------- | --------|
| `-lau` <br /> `--listen-udp-acars` | `AR_LISTEN_UDP_ACARS` | UDP port to listen for ACARS messages. <br /> Can be specified multiple times to listen on multiple ports. | `5550` |
| `-lvu` <br /> `--listen-udp-vdlm2` | `AR_LISTEN_UDP_VDLM2` | UDP port to listen for ACARS messages. <br /> Can be specified multiple times to listen on multiple ports. | `5555` |
| `-sua` <br /> `--send-udp-acars` | `AR_SEND_UDP_ACARS` | Client to send ACARS messages in `host:port` format. <br /> Can be specified multiple times to send to multiple clients. | |
| `-suv` <br /> `--send-udp-vdlm2` | `AR_SEND_UDP_VDLM2` | Client to send ACARS messages in `host:port` format. <br /> Can be specified multiple times to send to multiple clients. | |

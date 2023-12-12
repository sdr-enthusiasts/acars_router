# sdr-enthusiasts/acars_router

`acars_router` receives, validates, deduplicates, modifies and routes ACARS and VDLM2 JSON messages.

## Example Feeder `docker-compose.yml`

```yaml
version: "3.8"

volumes:
  - acarshub_run

services:
    acarshub:
    image: ghcr.io/sdr-enthusiasts/docker-acarshub:latest
    tty: true
    container_name: acarshub
    restart: always
    ports:
      - 8080:80
    environment:
      - ENABLE_VDLM=EXTERNAL
      - ENABLE_ACARS=EXTERNAL
      - FEED=true
      - IATA_OVERRIDE=UP|UPS|United Parcel Service;GS|FTH|Mountain Aviation (Foothills);GS|EJA|ExecJet
      - ENABLE_ADSB=true
      - ADSB_LAT=${FEEDER_LAT}
      - ADSB_LON=${FEEDER_LON}
      - ADSB_URL=${ACARS_TAR_HOST}
    volumes:
      - "acars_run:/run/acars"
    tmpfs:
      - /database:exec,size=64M
      - /run:exec,size=64M
      - /var/log

  acars_router:
    image: ghcr.io/sdr-enthusiasts/acars_router:latest
    tty: true
    container_name: acars_router
    restart: always
    environment:
      - TZ=${FEEDER_TZ}
      - AR_ENABLE_DEDUPE=true
      - AR_SEND_UDP_ACARS=acarshub:5550;feed.airframes.io:5550
      - AR_SEND_UDP_VDLM2=acarshub:5555
      - AR_SEND_TCP_VDLM2=feed.airframes.io:5553
      - AR_SEND_TCP_HFDL=feed.airframes.io:5556
      - AR_RECV_ZMQ_VDLM2=dumpvdl2:45555
    tmpfs:
      - /run:exec,size=64M
      - /var/log
```

Change the `GAIN`, `FREQUENCIES`, `SERIAL`, `TZ`, `ADSB_LAT`, `ADSB_LON` and `AR_OVERRIDE_STATION_NAME` to suit your environment.

With the above deployment:

- JSON messages generated by `acarsdec` will be sent to `acars_router` via UDP.
- `acars_router` will connect to `dumpvdl2` via TCP/ZMQ and receive JSON messages.
- `acars_router` will set your station ID on all messages.
- `acars_router` will then output to `acarshub`.

## Ports

All ports are configurable. By default, the following ports will be used:

| Port    | Protocol | Description                                                                      |
| ------- | -------- | -------------------------------------------------------------------------------- |
| `5550`  | `UDP`    | ACARS injest. Clients will send acars data to this port via UDP.                 |
| `5550`  | `TCP`    | ACARS injest. Clients will send acars data to this port via TCP.                 |
| `5555`  | `UDP`    | VDLM2 injest. Clients will send VDLM2 data to this port via UDP.                 |
| `5555`  | `TCP`    | VDLM2 injest. Clients will send VDLM2 data to this port via TCP.                 |
| `5556`  | `UDP`    | HFDL injest. Clients will send HFDL data to this port via UDP.                   |
| `5556`  | `TCP`    | HFDL injest. Clients will send HFDL data to this port via TCP.                   |
| `15550` | `TCP`    | ACARS server. Can be used for other clients to connect and get messages          |
| `15555` | `TCP`    | VDLM2 server. Can be used for other clients to connect and get messages          |
| `15556` | `TCP`    | HFDL server. Can be used for other clients to connect and get messages           |
| `35550` | `ZMQ`    | ACARS injest. Clients will connect to this port and send acars messages over ZMQ |
| `35555` | `ZMQ`    | VDLM2 injest. Clients will connect to this port and send VDLM2 messages over ZMQ |
| `35556` | `ZMQ`    | HFDL injest. Clients will connect to this port and send HFDL messages over ZMQ   |
| `45550` | `ZMQ`    | ACARS server. Can be used for other ZMQ clients to connect and get messages      |
| `45555` | `ZMQ`    | VDLM2 server. Can be used for other ZMQ clients to connect and get messages      |
| `45556` | `ZMQ`    | HFDL server. Can be used for other ZMQ clients to connect and get messages       |

If you want any port(s) to be exposed outside of the docker network, please be sure to append them to the ports section of the `docker-compose.yml` file.

## Environment Variables

All env variables with `SEND` or `RECV` in them can have multiple destinations. Each destination should be separated by a `;`. For example, `SEND_UDP_ACARS=acarshub:5550;acarshub2:5550` will send UDP ACARS messages to both `acarshub` and `acarshub2`.

The nomenclature for the environment variables is as follows:

### Nomenclature used in variable naming

#### Input/Inbound data

- Receiver: ACARS router will connect out to a remote host and receive data from it. (TCP/ZMQ)
- Listener: ACARS router will listen on a port for incoming data (UDP) or incoming connection based on socket type (TCP/ZMQ)

#### Output/Outbound data

- Sender: ACARS router will connect out to a remote host and send data to it. (TCP/ZMQ)
- Server: ACARS router will send data to a remote host (UDP) or listen for incoming connection (TCP/ZMQ) and send data to it.

### Outbound data

| Env Variable       | Command Line Switch | Default | Description                              |
| ------------------ | ------------------- | ------- | ---------------------------------------- |
| AR_SEND_UDP_ACARS  | --send-udp-acars    | `unset` | UDP host:port to send ACARS messages to  |
| AR_SEND_UDP_VDLM2  | --send-udp-vdlm2    | `unset` | UDP host:port to send VDLM2 messages to  |
| AR_SEND_UDP_HFDL   | --send-udp-hfdl     | `unset` | UDP host:port to send HFDL messages to   |
| AR_SEND_TCP_ACARS  | --send-tcp-acars    | `unset` | TCP host:port to send ACARS messages to  |
| AR_SEND_TCP_VDLM2  | --send-tcp-vdlm2    | `unset` | TCP host:port to send VDLM2 messages to  |
| AR_SEND_TCP_HFDL   | --send-tcp-hfdl     | `unset` | TCP host:port to send HFDL messages to   |
| AR_SERVE_TCP_ACARS | --serve-tcp-acars   | `5550`  | TCP host:port to serve ACARS messages to |
| AR_SERVE_TCP_VDLM2 | --serve-tcp-vdlm2   | `5555`  | TCP host:port to serve VDLM2 messages to |
| AR_SERVE_TCP_HFDL  | --serve-tcp-hfdl    | `5556`  | TCP host:port to serve HFDL messages to  |
| AR_SERVE_ZMQ_ACARS | --serve-zmq-acars   | `45550` | ZMQ host:port to serve ACARS messages to |
| AR_SERVE_ZMQ_VDLM2 | --serve-zmq-vdlm2   | `45555` | ZMQ host:port to serve VDLM2 messages to |
| AR_SERVE_ZMQ_HFDL  | --serve-zmq-hfdl    | `45556` | ZMQ host:port to serve HFDL messages to  |

### Inbound data

| Env Variable        | Command Line Switch | Default | Description                                  |
| ------------------- | ------------------- | ------- | -------------------------------------------- |
| AR_RECV_ZMQ_ACARS   | --recv-zmq-acars    | `unset` | ZMQ host:port to receive ACARS messages from |
| AR_RECV_ZMQ_VDLM2   | --recv-zmq-vdlm2    | `unset` | ZMQ host:port to receive VDLM2 messages from |
| AR_RECV_ZMQ_HFDL    | --recv-zmq-hfdl     | `unset` | ZMQ host:port to receive HFDL messages from  |
| AR_RECV_TCP_ACARS   | --recv-tcp-acars    | `unset` | TCP host:port to receive ACARS messages from |
| AR_RECV_TCP_VDLM2   | --recv-tcp-vdlm2    | `unset` | TCP host:port to receive VDLM2 messages from |
| AR_RECV_TCP_HFDL    | --recv-tcp-hfdl     | `unset` | TCP host:port to receive HFDL messages from  |
| AR_LISTEN_TCP_ACARS | --listen-tcp-acars  | `5550`  | TCP port to listen for ACARS messages from   |
| AR_LISTEN_TCP_VDLM2 | --listen-tcp-vdlm2  | `5555`  | TCP port to listen for VDLM2 messages from   |
| AR_LISTEN_TCP_HFDL  | --listen-tcp-hfdl   | `5556`  | TCP port to listen for HFDL messages from    |
| AR_LISTEN_UDP_ACARS | --listen-udp-acars  | `5550`  | UDP port to listen for ACARS messages from   |
| AR_LISTEN_UDP_VDLM2 | --listen-udp-vdlm2  | `5555`  | UDP port to listen for VDLM2 messages from   |
| AR_LISTEN_UDP_HFDL  | --listen-udp-hfdl   | `5556`  | UDP port to listen for HFDL messages from    |
| AR_LISTEN_ZMQ_ACARS | --listen-zmq-acars  | `35550` | ZMQ port to listen for ACARS messages from   |
| AR_LISTEN_ZMQ_VDLM2 | --listen-zmq-vdlm2  | `35555` | ZMQ port to listen for VDLM2 messages from   |
| AR_LISTEN_ZMQ_HFDL  | --listen-zmq-hfdl   | `35556` | ZMQ port to listen for HFDL messages from    |

### General Options

| Env Variable             | Command Line Switch     | Default | Description                                                                                                                                                                                                     |
| ------------------------ | ----------------------- | ------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| AR_VERBOSITY             | --verbose               | `info`  | Verbosity level. Valid values are `debug`, `info`, `warning`, `error`                                                                                                                                           |
| AR_ENABLE_DEDUPE         | --enable-dedupe         | `false` | Enable message deduplication. Valid values are `true` or `false`                                                                                                                                                |
| AR_DEDUPE_WINDOW         | --dedupe-window         | `2`     | Window for how long a message will be considered a duplicate if the same message is received again.                                                                                                             |
| AR_SKEW_WINDOW           | --skew-window           | `5`     | If a message is older then the skew window it will be automatically rejected, in seconds. If you are receiving only ACARS/VDLM2 1 or 2 seconds is a good value. With HFDL you will need to increase the window. |
| AR_MAX_UDP_PACKET_SIZE   | --max-udp-packet-size   | `60000` | Maximum UDP packet size. Messages greater then this will be send in multiple parts                                                                                                                              |
| AR_ADD_PROXY_ID          | --add-proxy-id          | `true`  | Append to the message a header indicating that acars router processed the message                                                                                                                               |
| AR_OVERRIDE_STATION_NAME | --override-station-name | `unset` | Change the station id field (identifying your station name for unstream clients) is set to this instead of what was in the message originally                                                                   |
| AR_STATS_EVERY           | --stats-every           | `5`     | How often to print stats to the log in minutes                                                                                                                                                                  |
| AR_STATS_VERBOSE         | --stats-verbose         | `false` | Print verbose stats to the log                                                                                                                                                                                  |
| AR_REASSEMBLY_WINDOW     | --reassemble-window     | `1.0`   | If a message comes in, but part of the message is missing, this value will be used to keep the partial message fragment around while attempting to wait for the second (or subsequent) part(s)                  |

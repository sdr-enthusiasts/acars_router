# sdr-enthusiasts/acars_router

* Merge & dedupe ACARS/VDLM2 messages
* Send ACARS/VDLM2 messages to multiple servers

## The plan

* Receive ACARS & VDLM2 data from acarsdec & vdlm2dec
* If dedupe enabled:
  * Have a buffer where messages stay for X seconds
  * When a message is received, buffer is scanned, duplicate messages are discarded
  * After X seconds, message is popped from the buffer and sent to clients
* Else:
  * Send to clients

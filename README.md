# acarsmerge

Merge & dedupe ACARS data

## The plan

- Receive ACARS & VDLM2 data from acarsdec & vdlm2dec
- Have a buffer where messages stay for X seconds
- When a message is received, buffer is scanned, duplicate messages are discarded
- After X seconds, message is popped from the buffer and sent out

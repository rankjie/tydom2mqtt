# 3.6.0-reconnect.1

- Retry connection failures during connection setup and message reception.
- Continue polling after a temporary connection failure.
- Exit with a failure status if a critical background task stops unexpectedly.
- Bound the HTTP authentication handshake with the existing reply timeout.

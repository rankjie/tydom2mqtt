## Developer guide

### Get the dependencies & the dev dependencies
```bash
cd app && pip install -r requirements.txt -r requirements.dev.txt
```

### Format & lint the code
This project uses [Ruff](https://docs.astral.sh/ruff/) (enforced by CI).
```bash
cd app && ruff format .      # apply formatting
cd app && ruff check .       # lint
```

### Run the tests
```bash
cd app && pytest
```

### Connection recovery
The Tydom listener retries transport failures during connection, setup, and
message reception after five seconds. A failed refresh is retried on the next
polling cycle. Unexpected listener or polling failures stop the process with a
nonzero exit status so the Home Assistant watchdog can restart it. Device
commands are not replayed automatically after a disconnect.

### Build the Docker image
```bash
docker build -t tydom2mqtt .
```

### Run the Docker image
```bash
docker run -it --rm -e TYDOM_MAC="001A25123456" -e TYDOM_PASSWORD="secret" tydom2mqtt
```

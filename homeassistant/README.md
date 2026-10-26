# tydom2mqtt

Home Assistant app for the reconnect fixes in this fork. Images are published
from version tags using the repository's Docker release workflow.

Add this repository in the Home Assistant app store:

```
https://github.com/rankjie/tydom2mqtt#fix/tydom-reconnect
```

The app uses the same configuration fields and MQTT discovery identifiers as
upstream. When switching repositories, stop the previous app before starting
this one so only one bridge controls the Tydom connection. Keep the previous
app installed until the new connection and device updates have been verified.

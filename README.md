## Morphes Uplink

This is the official Morpheus plugin for interacting with a uplink/parent Morpheus server.  Currently this plugin supports:

* DNS
  * DNS Zones
  * Consumed DNS Records
  * Creation of DNS Records
* IPAM
  * IP Pools
  * Consumed IP Records
  * Creation / Reservation of IPv4 and IPv6

### Configuring

Once the plugin is loaded in the environment (`Administration -> Integrations -> Plugins` ). Morpheus Uplink becomes available under `Infrastructure -> Network -> Services`.

When adding the integration simply enter the URL of the parent Morpheus API Endpoint (no path is needed just the root url) and the credentials with sufficient enough privileges to talk to the API.

### Building

This is a Morpheus plugin that leverages the `morpheus-plugin-core` which can be referenced by visiting [https://developer.morpheusdata.com](https://developer.morpheusdata.com). It is a groovy plugin designed to be uploaded into a Morpheus environment. To build this product from scratch simply run the shadowJar gradle task on java 11:

```bash
./gradlew shadowJar
```

A jar will be produced in the `build/lib` folder that can be uploaded into a Morpheus environment.
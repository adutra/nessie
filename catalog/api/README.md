# Nessie Catalog API and transports

## Transport protocol

The transport protocol is optimized for remote calls.

### Known objects omission / client side caching

Clients, including the Java API implemented in `nessie-catalog-api-base`, support client side caching of immutable
objects. Immutable objects are pieces of a table or view snapshot, including individual schema definitions, partitioning
definitions, sort definitions, data access manifests and more.

Clients inform the server via a bloom-filter based mechanism which objects they already know about. The server can then
omit objects that have an ID contained in the bloom-filter from the response. Since a bloom-filter is a probabilistic
data structure (i.e. it can return false-positives), it may happen that a client needs an additional round-trip to
fetch the objects that are required but have been omitted. A similar situation may happen when the client side cache
evicts objects while a request is being executed.

## Java API

The user facing Nessie Catalog API is accessible via the [NessieCatalogAPI class], which provides an instance
of the [NessieCatalog class].

### Transports (Java)

The Java API supports both REST and gRPC transport protocols, REST is used by default.

Transports are abstracted from user facing API and use an API that is optimized for remote access.

### Configuration (Java)

Configuration of the Nessie Catalog API and the transport (REST or gRPC) being used, uses [SmallRye Config] based
on [MicroProfile Config].

The configuration is read from:

* `~/.env`
* process environment
* Java system properties
* classpath resources named `META-INF/nessie-catalog.properties`
* `~/config/nessie-catalog.propertes`
* `~/config/nessie-catalog.yaml`
* `./nessie-catalog.propertes`
* `./nessie-catalog.yaml`
* discovered config sources

Discovery of SmallRye Config validators, converters and interceptors is enabled.

Support for [secret keys] is enabled (encrypted configuration values).


[SmallRye Config]: https://smallrye.io/smallrye-config/

[MicroProfile Config]: https://github.com/eclipse/microprofile-config/

[secret keys]: https://smallrye.io/smallrye-config/Main/config/secret-keys/

[NessieCatalogAPI class]: ./api/src/main/java/org/projectnessie/catalog/api/base/NessieCatalogAPI.java

[NessieCatalog class]: ../schema/model/src/main/java/org/projectnessie/catalog/NessieCatalog.java

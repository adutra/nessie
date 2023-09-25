# Nessie Catalog - Schema

**TODO rename this module.** 

## Table formats

* Iceberg
* Delta

## Nessie Core and Catalog APIs

All necessary functionality is provided by the Nessie catalog API. Users do not need to use the Nessie core API.
The Nessie catalog API however uses types from the Nessie core API (from the `org.projectnessie.nessie:nessie-model`
artifact).

## Nomenclature

Terms like "catalog", "schema", "table", "column" are used across Iceberg, Delta and other APIs. To avoid confusion
and the need to use fully qualified Java type names, many Java types in this module start with `Nessie`. For example:
the type `NessieSchema` does not conflict with Iceberg's `Schema` type.

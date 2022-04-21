# Milestone 1
## TPCC client (Size: S)
    - runs with libpq
    - maybe try with OG's jdbc?
    - run the test on single node to figure out the entire key space and generate the SKV partition range splits
        for 100 warehouse run, start k2 instance with all machine mem and dump the keyspace with Inspect

## OG (chogori-opengauss)
### build logic (Size: S)
    - k2build (Centos)
    - CI
    - Integration test example. Consider copying all chogori-sql integ tests
### PGGate(chogori-sql) reuse/copy (Size: M)
    - copy existing code with deps and build with OG
    - does not include integration with OG
    - does not include migration of Seastar/std::future based client to the new HTTP client

### Table Access API (Size: M)
    - extend the table AM api
    - FDW may not be enough for bootstrap cases
    - partial updates, deletes, conditional writes
### FDW (Size: M)
    - use for scan
### Index Access API (Size: M)
    - reading through secondary index
    - index scan
    - no projections for now
### Catalog API (Size: M)
    - versioning for multiple instances
    - caching and invalidation based on versioning
    - scan API
    - SKV integration for schema CRUD
### Bootstrap (Size: M)
    - single instance bootstrap
    - some notion of bootstrap-complete for other OG instances

### AUX services (disable for M1) (Size: S)
    - WAL
    - scheduler
    - monitor
    - xlog
    - some sort of leader election to help designate the process which will run these

### Instrumentation (Size: M)
    - prometheus-based
    - (chogori-prometheus-cpp)
    - instrument HTTP client
    - hookup OG logs to Loki
    - some metrics in pggate?

## SKV 
### HTTP Client for OG integration (Size: M)
    - standalone client (no-deps other stl/posix)
    - probably just a simple blocking API
    - allows us to talk to the HTTP proxy on machine
    - Schema, SKVRecord, other DTO data types ideally shared with platform
    - e.g. SKVRecord read(SKVRecord key);

### HTTP Proxy (chogori-platform) (Size: M)
    - dynamic create collection
    - dynamic create schema
    - scan
    - partial update
    - delete
    - conditional writes

# Miletone wrapup (bugbash: october)
## Running tests
## fixing bugs
## etc

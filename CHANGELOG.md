# Changes for Datasplash

## [0.7.20] - 2024-03-16
### Changed
* Bump beam to 2.54.0
* Bump charred to 1.034
* Bump math.combinatronics
* Bump clojure
* Downgrade slf4j to 1.7.x as v2 is not compatible with beam > 2.53.0
* Replace :dev profile logback dep with slj4j-simple

## [0.7.19] - 2023-12-15
- Bump beam to 2.52.0
- Add taoensso/encore required dependency for nippy.

## [v0.7.18] - 2023-09-29
- Bump beam to 2.50.0
- Bump charred to 1.033
- Bump superstring to 3.2.0

## [v0.7.17] - 2023-04-07
- add ds/distinct-by, ds/keep
- normalize BQ schema
- support for maxLength, precision, scale fields in BQ tables
- new PAssert testing utilities
- clean up aot files to reduce size
- BQ specs for schema-validation
- new exported clj-kondo config
- bump charred to 1.028, beam sdk to 2.46.0

## [v0.7.16] - 2023-01-27
- Bump charred to 1.019, beam to 2.44.0

## [v0.7.15] - 2022-12-15
- Bump charred to 1.018

## [v0.7.14] - 2022-12-02
- adapt charred options to avoid throwing on empty lines
- Bump charred to 1.016, beam to 2.43.0

## [v0.7.13] - 2022-10-26
- Add BQ raw read `:query-location` option.
- Add support for Pcoll set operations.
- Bump beam to 2.42.0
- Bump charred to 1.0.14

## [v0.7.12] - 2022-09-16
- handle collection for generating pipeline args
- Bump beam to 2.41.0
- Bump charred to 1.0.12

## [v0.7.11] - 2022-08-23
- Add BQ clustering & optimized-write options in write-bq-table.
- Bump beam to 2.40.0.
- Bump charred to 1.0.11.
- Bump nippy to 3.2.0.

## [v0.7.10] - 2022-06-24
- Bump nippy to fix serializing of boolean values in map
  collections.
- Bump Beam to 2.39.0.

## [v0.7.9] - 2022-05-24
- Bump charred to fix more white-space issues. Enable options to match
  previous output.
- Add write-json-file-test.

## [v0.7.8] - 2022-05-16

- More complete tests, again.
- Add ability to specify a teardown function for a `DoFn`
- Invoke `start-bundle`, `finish-bundle` and `teardown-fn` when specified
- Reuse no-op function in `dofn`
- switch from cheshire to com.cnuernber/charred.

## [v0.7.7] - 2022-05-13

- Enforce target jdk as 1.8
- Bump clojure to 1.11.1
- more complete tests

## [v0.7.6] - 2022-04-23

- don't require bq, pubsub, datastore in api.clj - Issue #94
- Bump beam to 2.38.0

## [v0.7.5] - 2022-04-09

- replace cheshire usage with jsonista by @domparry
- Bump beam to 2.37.0; bump other deps

## [v0.7.4] - 2021-12-15

- Datastore fix by @domparry for breaking change introduced by nippy
  bump in 0.7.3
- Bump beam dep (2.34.0)
- Bump tools.logging (1.2.2)
- Clean up

## [v0.7.3] - 2021-08-14

- Resolve various java compiler warnings
- Bump dependencies including beam (2.31.0)
- Bump nippy to 3.1.1, adding alter-var-root of
  `nippy/*thaw-serializable-allowlist*` to allow serializable
  org.apache.beam.sdk.values.KV
- Fix probems with Id keys in Google Datastore storage

## [v0.7.2] - 2021-01-07

- fix locking problem with requires in safe-exec => Issue #97
- Update Apache Beam to 2.26.0

## [v0.7.1] - 2020-10-06

- Update Apache Beam to 2.24.0

## [v0.7.0] - 2020-09-07

- Add Kafka IO support
- Add delete-datastore-raw to datastore features
- add :schema-update-options for BQ write
- Update Apache Beam to 2.22.0
- Performance improvements
- Fixed require bug in `partition-by`

## [v0.6.6] - 2019-09-11
### Fixes

- Fix `cogroup-by` for numbers of pcoll > 10 => Issue #83
- Fix `:many-files` option in `read-text-file` => Issue #75

## [v0.6.5] - 2019-08-23

- Add FileIO Read to read a PCollection of files
- Add ElasticSearchIO
- Upgrade to Apache Beam 2.15.0
- Upgrade dependencies

## [v0.6.4] - 2019-01-16

- Upgrade to Apache Beam 2.9.0
- Upgrade dependencies

## [v0.6.3] - 2018-11-19

- Add json-schema in bq write
- Combine-fn accepts now map as arguments
- Remove deprecated write-by

## [v0.6.2] - 2018-11-15

- Upgrade to Apache Beam 2.8.0
- Support dynamic file writes
- Support `:deflate` and `:zip` compressions
- `bq/->time-partitioning` now accepts `:field` and `:require-partition-filter`
- Improve docs

## [v0.6.1] - 2018-03-20
### Changes

- Upgrade to Dataflow 2.3.0
- Support time-partitioned Big Query tables (thanks to @neuromantik33)

### Fixes

- Fix broken input from BigQuery
- Fix missing safe-exec from filename policy
- Fix broken serializing after group-by in some cases
- Fix rare (hopefully) bug introduced by being lazy. with the vals after a group-by. Now we are greedy.

## [v0.6.0] - 2018-02-13
### Changes

- Compatibility with Dataflow 2.2.0
- Compatibility with Clojure 1.9 and specs (some code had to be ported to Java shims because `proxy` is not `Serializable` anymore in Clojure 1.9)

### Fixes

- Removed superfluous logging, `safe-exec` call and type hints

## [v0.5.3] - 2017-12-18
### Fixes

- Fix options on PipelineWithOptions to have it work again on Dataflow (oops, sorry!)

## [v0.5.2] - 2017-12-14
### Changes

- Fixed `defoptions` again and made it useful for interop with Beam
- Added `:checkpoint` option, will be made more useful later
- Switched to new PubSub API
- Add support for custom filename-policy

### Fixes

- Fixed a possible bug causing StackOverflows on `combine`

## [v0.5.1] - 2017-10-05
### Changes

- Added `wait-pipeline-result`

### Fixes

- Fix a bug with incorrect call to `seq` in combine
- Add a missing safe-exec in `write-bq-table-raw`

## [v0.5.0] - 2017-09-18
### Changes

- Update to v2.x Dataflow version (Apache Beam)
- Add new streaming features (custom timestamp in PubSub #30 (thanks to @RoIT), BigQuery streaming writes)
- Add compressed TextIO output #31 (thanks to @RoIT)
- Add FileNamePolicy compatibility for TextIO

## [v0.4.1] - 2017-03-09
### Changes

- Handle recursion in RECORD schema generation for Big Query #9 (thanks to @torbjornvatn)
- Add windows and trigger support #13 (thanks to @RoIT)
- Bump Dataflow SDK to 1.9.0
- Add support for standard SQL syntax for Big Query #11 (thanks to @torbjornvatn)
- Read data from pubsub topics #10 (thanks to @torbjornvatn)

## [v0.4.0] - 2016-11-12
### Breaking Changes

- Update **Datastore** API to v1, add wrappers for creating *Entities* from and converting to Clojure maps. Some breakage in the *datasplash.datastore* namespace ensues.

### Changes

- Add pubsub namespace #6 (Thanks to @MartinSahlen)
- Put slf4j deps in :dev profiles #8 (Thanks to @torbjornvatn)

[Unreleased]: https://https://github.com/ngrunwald/datasplash/0.7.20...devel
[0.7.20]: https://github.com/ngrunwald/datasplash/-/compare/v0.7.19...0.7.20
[0.7.19]: https://github.com/ngrunwald/datasplash/-/compare/v0.7.18...0.7.19
[v0.7.18]: https://github.com/ngrunwald/datasplash/-/compare/v0.7.17...v0.7.18
[v0.7.17]: https://github.com/ngrunwald/datasplash/-/compare/v0.7.16...v0.7.17
[v0.7.16]: https://github.com/ngrunwald/datasplash/-/compare/v0.7.15...v0.7.16
[v0.7.15]: https://github.com/ngrunwald/datasplash/-/compare/v0.7.14...v0.7.15
[v0.7.14]: https://github.com/ngrunwald/datasplash/-/compare/v0.7.13...v0.7.14
[v0.7.13]: https://github.com/ngrunwald/datasplash/-/compare/v0.7.12...v0.7.13
[v0.7.12]: https://github.com/ngrunwald/datasplash/-/compare/v0.7.11...v0.7.12
[v0.7.11]: https://github.com/ngrunwald/datasplash/-/compare/v0.7.10...v0.7.11
[v0.7.10]: https://github.com/ngrunwald/datasplash/-/compare/v0.7.9...v0.7.10
[v0.7.9]: https://github.com/ngrunwald/datasplash/-/compare/v0.7.8...v0.7.9
[v0.7.8]: https://github.com/ngrunwald/datasplash/-/compare/v0.7.7...v0.7.8
[v0.7.7]: https://github.com/ngrunwald/datasplash/-/compare/v0.7.6...v0.7.7
[v0.7.6]: https://github.com/ngrunwald/datasplash/-/compare/v0.7.6...v0.7.7
[v0.7.5]: https://github.com/ngrunwald/datasplash/-/compare/v0.7.5...v0.7.6
[v0.7.4]: https://github.com/ngrunwald/datasplash/-/compare/v0.7.4...v0.7.5
[v0.7.3]: https://github.com/ngrunwald/datasplash/-/compare/v0.7.3...v0.7.4
[v0.7.2]: https://github.com/ngrunwald/datasplash/-/compare/v0.7.2...v0.7.3
[v0.7.1]: https://github.com/ngrunwald/datasplash/-/compare/v0.7.1...v0.7.2
[v0.7.0]: https://github.com/ngrunwald/datasplash/-/compare/v0.7.0...v0.7.1
[v0.6.6]: https://github.com/ngrunwald/datasplash/-/compare/v0.6.6...v0.7.0
[v0.6.5]: https://github.com/ngrunwald/datasplash/-/compare/v0.6.5...v0.6.6
[v0.6.4]: https://github.com/ngrunwald/datasplash/-/compare/v0.6.4...v0.6.5
[v0.6.3]: https://github.com/ngrunwald/datasplash/-/compare/v0.6.3...v0.6.4
[v0.6.2]: https://github.com/ngrunwald/datasplash/-/compare/v0.6.2...v0.6.3
[v0.6.1]: https://github.com/ngrunwald/datasplash/-/compare/v0.6.1...v0.6.2
[v0.6.0]: https://github.com/ngrunwald/datasplash/-/compare/v0.6.0...v0.6.1
[v0.5.3]: https://github.com/ngrunwald/datasplash/-/compare/v0.5.3...v0.6.0
[v0.5.2]: https://github.com/ngrunwald/datasplash/-/compare/v0.5.2...v0.5.3
[v0.5.1]: https://github.com/ngrunwald/datasplash/-/compare/v0.5.1...v0.5.2
[v0.4.0]: https://github.com/ngrunwald/datasplash/-/compare/v0.4.0...v0.4.1
[v0.3.1]: https://github.com/ngrunwald/datasplash/-/compare/v0.3.1...v0.4.0

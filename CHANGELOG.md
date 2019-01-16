# Changes for Datasplash

## v0.6.4 (2019-01-16)

**[compare](https://github.com/ngrunwald/datasplash/compare/v0.6.3...v0.6.4)**

- Upgrade to Apache Beam 2.9.0
- Upgrade dependencies

## v0.6.3 (2018-11-19)

**[compare](https://github.com/ngrunwald/datasplash/compare/v0.6.2...v0.6.3)**

- Add json-schema in bq write
- Combine-fn accepts now map as arguments
- Remove deprecated write-by


## v0.6.2 (2018-11-15)

**[compare](https://github.com/ngrunwald/datasplash/compare/v0.6.1...v0.6.2)**

- Upgrade to Apache Beam 2.8.0
- Support dynamic file writes
- Support `:deflate` and `:zip` compressions
- `bq/->time-partitioning` now accepts `:field` and `:require-partition-filter`
- Improve docs

## v0.6.1 (2018-03-20)

**[compare](https://github.com/ngrunwald/datasplash/compare/v0.6.0...v0.6.1)**

### Changes

- Upgrade to Dataflow 2.3.0
- Support time-partitioned Big Query tables (thanks to @neuromantik33)

### Fixes

- Fix broken input from BigQuery
- Fix missing safe-exec from filename policy
- Fix broken serializing after group-by in some cases
- Fix rare (hopefully) bug introduced by being lazy. with the vals after a group-by. Now we are greedy.

## v0.6.0 (2018-02-13)

**[compare](https://github.com/ngrunwald/datasplash/compare/v0.5.3...v0.6.0)**

### Changes

- Compatibility with Dataflow 2.2.0
- Compatibility with Clojure 1.9 and specs (some code had to be ported to Java shims because `proxy` is not `Serializable` anymore in Clojure 1.9)

### Fixes

- Removed superfluous logging, `safe-exec` call and type hints

## v0.5.3 (2017-12-18)

**[compare](https://github.com/ngrunwald/datasplash/compare/v0.5.2...v0.5.3)**

### Fixes

- Fix options on PipelineWithOptions to have it work again on Dataflow (oops, sorry!)

## v0.5.2 (2017-12-14)

**[compare](https://github.com/ngrunwald/datasplash/compare/v0.5.1...v0.5.2)**

### Changes

- Fixed `defoptions` again and made it useful for interop with Beam
- Added `:checkpoint` option, will be made more useful later
- Switched to new PubSub API
- Add support for custom filename-policy

### Fixes

- Fixed a possible bug causing StackOverflows on `combine`

## v0.5.1 (2017-10-05)

### Changes

- Added `wait-pipeline-result`

### Fixes

- Fix a bug with incorrect call to `seq` in combine
- Add a missing safe-exec in `write-bq-table-raw`

## v0.5.0 (2017-09-18)

### Changes

- Update to v2.x Dataflow version (Apache Beam)
- Add new streaming features (custom timestamp in PubSub #30 (thanks to @RoIT), BigQuery streaming writes)
- Add compressed TextIO output #31 (thanks to @RoIT)
- Add FileNamePolicy compatibility for TextIO

## v0.4.1 (2017-03-09)

**[compare](https://github.com/ngrunwald/datasplash/compare/v0.4.0...v0.4.1)**

### Changes

- Handle recursion in RECORD schema generation for Big Query #9 (thanks to @torbjornvatn)
- Add windows and trigger support #13 (thanks to @RoIT)
- Bump Dataflow SDK to 1.9.0
- Add support for standard SQL syntax for Big Query #11 (thanks to @torbjornvatn)
- Read data from pubsub topics #10 (thanks to @torbjornvatn)

## v0.4.0 (2016-11-12)

**[compare](https://github.com/ngrunwald/datasplash/compare/v0.3.1...v0.4.0)**

### Breaking Changes

- Update **Datastore** API to v1, add wrappers for creating *Entities* from and converting to Clojure maps. Some breakage in the *datasplash.datastore* namespace ensues.

### Changes

- Add pubsub namespace #6 (Thanks to @MartinSahlen)
- Put slf4j deps in :dev profiles #8 (Thanks to @torbjornvatn)

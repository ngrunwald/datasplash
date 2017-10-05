# Changes for Datasplash

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

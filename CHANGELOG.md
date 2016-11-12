#Changes for Datasplash

## v0.4.0 (2016-11-12)

**[compare](https://github.com/ngrunwald/datasplash/compare/v0.3.1...v0.4.0)**

### Breaking Changes

- Update **Datastore** API to v1, add wrappers for creating *Entities* from and converting to Clojure maps. Some breakage in the *datasplash.datastore* namespace ensues.

### Changes

- Add pubsub namespace #6 (Thanks to @MartinSahlen)
- Put slf4j deps in :dev profiles #8 (Thanks to @torbjornvatn)

# Datasplash

Clojure API for a more dynamic [Google Cloud Dataflow][gcloud] and (not really
battle tested) any other [Apache Beam][beam] backend.

[gcloud]: https://cloud.google.com/dataflow/
[beam]: https://beam.apache.org/

## Usage

For now, you can consult the [API docs](https://oscaro.github.io/datasplash/).

You can also see ports of the official Dataflow examples in the
`datasplash.examples` namespace.

Here is the classic word count:

```clojure
(ns datasplash.examples
  (:require [clojure.string :as str]
            [datasplash.api :as ds])
  (:gen-class))

(defn tokenize
  [l]
  (remove empty? (.split (str/trim l) "[^a-zA-Z']+")))

(ds/defoptions WordCountOptions
  {:input {:type String
           :default "gs://dataflow-samples/shakespeare/kinglear.txt"
           :description "Path of the file to read from"}
   :output {:type String
            :default "kinglear-freqs.txt"
            :description "Path of the file to write to"}
   :numShards {:type Long
               :description "Number of output shards (0 if the system should choose automatically)"
               :default 0}})

(defn -main
  [& str-args]
  (let [p (ds/make-pipeline WordCountOptions str-args)
        {:keys [input output numShards]} (ds/get-pipeline-options p)]
    (->> p
         (ds/read-text-file input {:name "King-Lear"})
         (ds/mapcat tokenize {:name :tokenize})
         (ds/frequencies)
         (ds/map (fn [[k v]] (format "%s: %d" k v)) {:name :format-count})
         (ds/write-text-file output {:num-shards numShards}))))
```
Run it from the repl with:
```clojure
(in-ns 'datasplash.examples)
(compile 'datasplash.examples)
(-main "--input=in.txt" "--output=out.txt")
```
Note that you will need to run `(compile 'datasplash.examples)` every time you
make a change.

Run it locally with:

```bash
lein run -- --input=in.txt --output=out.txt
```

Run in on Google Cloud (if you have done a `gcloud init` on this machine):

```bash
lein run -- --input=gs://dataflow-samples/shakespeare/kinglear.txt --output=gs://my-project-tmp/results.txt  --runner=BlockingDataflowPipelineRunner --project=my-project --stagingLocation=gs://my-project-staging
```

To get help on the available options (directly from Beam):
```bash
lein run -- --help
```

And help on specific tasks:
```bash
lein run -- --help=WordCountOptions
```

## Caveats

- Due to the way the code is loaded when running in distributed mode, you may
  get some exceptions about unbound vars, especially when using instances with
  a high number of cpu. They will not however cause the job to fail and are of
  no consequences. They are caused by the need to prep the Clojure runtime when
  loading the class files in remote instances and some tricky business with
  locks and `require`.
- If you have to write your own low-level `ParDo` objects (you shouldn't), wrap
  all your code in the `safe-exec` macro to avoid issues with unbound vars. Any
  good idea about finding a better way to do this would be greatly appreciated!
- If some of the `UserCodeException` as seen in the cloud UI are mangled and
  missing the relevant part of the Clojure source code, this is due to a bug
  with the way the sdk mangles stacktraces in Clojure. In this case look for
  _ClojureRuntimeException_ in the logs to find the original unaltered
  stacktrace.
- Beware of using Clojure 1.9: `proxy` results are not `Serializable` anymore,
  so you cannot use anywhere in your pipeline Clojure code that uses proxy. Use
  Java shim for these objects instead.
- If you see something like `java.lang.ClassNotFoundException: Options` you
  probably forgot to compile your namespace.
- Whenever you need to check some spec in user code, you will have to first require
  those specs because they may not be loaded in your Clojure runtime. But don't
  use `(require)` because it's not thread safe. See [[this issue]](https://clojure.atlassian.net/browse/CLJ-1876)
  for a workaround.

## License

Copyright © 2015-2018 Oscaro.com

Distributed under the Eclipse Public License either version 1.0 or (at your
option) any later version.

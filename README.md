# Datasplash

[![Clojars Project](https://img.shields.io/clojars/v/datasplash.svg)](https://clojars.org/datasplash)

[![cljdoc badge](https://cljdoc.org/badge/datasplash/datasplash)](https://cljdoc.org/d/datasplash/datasplash/CURRENT)


Clojure API for a more dynamic [Google Cloud Dataflow][gcloud] and (not really
battle tested) any other [Apache Beam][beam] backend.

[gcloud]: https://cloud.google.com/dataflow/
[beam]: https://beam.apache.org/

## Usage

[API docs](https://cljdoc.org/d/datasplash/datasplash/CURRENT/api/datasplash)

You can also see ports of the official Dataflow examples in the
`datasplash.examples` namespace.

Here is the classic word count example.

:information_source: You will need to run `(compile 'datasplash.examples)`
every time you make a change.

```clojure
(ns datasplash.examples
  (:require [clojure.string :as str]
            [datasplash.api :as ds]
            [datasplash.options :refer [defoptions]])
  (:gen-class))

(defn tokenize
  [^String l]
  (remove empty? (.split (str/trim l) "[^a-zA-Z']+")))

(defn count-words
  [p]
  (ds/->> :count-words p
          (ds/mapcat tokenize {:name :tokenize})
          (ds/frequencies)))

(defn format-count
  [[k v]]
  (format "%s: %d" k v))

(defoptions WordCountOptions
  {:input {:default "gs://dataflow-samples/shakespeare/kinglear.txt"
           :type String}
   :output {:default "kinglear-freqs.txt" :type String}
   :numShards {:default 0 :type Long}})

(defn -main
  [& str-args]
  (let [p (ds/make-pipeline WordCountOptions str-args)
        {:keys [input output numShards]} (ds/get-pipeline-options p)]
    (->> p
         (ds/read-text-file input {:name "King-Lear"})
         (count-words)
         (ds/map format-count {:name :format-count})
         (ds/write-text-file output {:num-shards numShards})
         (ds/run-pipeline))))
```

### Run it from the repl

Locally on your machine using a DirectRunner:

```clojure
(in-ns 'datasplash.examples)
(clojure.core/compile 'datasplash.examples)
(-main "--input=sometext.txt" "--output=out-freq.txt" "--numShards=1")
```

Remotely on Google Cloud using a DataflowRunner:

You should have properly configured your Google Cloud account and Dataflow
access from your machine.

```clojure
(in-ns 'datasplash.examples)
(clojure.core/compile 'datasplash.examples)
(-main "--project=my-project"
       "--runner=DataflowRunner"
       "--gcpTempLocation=gs://bucket/tmp"
       "--input=gs://apache-beam-samples/shakespeare/kinglear.txt"
       "--output=gs://bucket/outputs/kinglear-freq.txt"
       "--numShards=1")
```

### Run it as a standalone program

Datasplash needs to be AOT compiled, so you should prepare an uberjar and
run from your main entry like so:

```bash
java -jar my-dataflow-job-uber.jar [beam-args]
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
  use `(require)` because it's not thread safe.
  See [[this issue]](https://clojure.atlassian.net/browse/CLJ-1876) for a workaround.
- If you see a `java.io.IOException: No such file or directory` when invoking
  `compile`, make sure there is a directory in your project root that matches
  the value of `*compile-path*` (default `classes`).


## License

Copyright © 2015-2024 Oscaro.com

Distributed under the Eclipse Public License either version 1.0 or (at your
option) any later version.

# Datasplash

Clojure API for a more dynamic [Google Cloud Dataflow](https://cloud.google.com/dataflow/).

## Usage

For now, you can consult the [API docs](http://theblankscreen.net/datasplash/).

You can also see ports of the official Dataflow examples in the `datasplash.examples` namespace.

Here is the classic word count:

```clojure
(ns datasplash.examples
  (:require [clojure.string :as str]
            [datasplash.api :as ds])
  (:gen-class))

(defn tokenize
  [l]
  (remove empty? (.split (str/trim l) "[^a-zA-Z']+")))


(defn -main
  [input output & str-args]
  (let [p (ds/make-pipeline  str-args)]
    (->> p
         (ds/read-text-file input {:name "King-Lear"})
         (ds/mapcat tokenize {:name :tokenize})
         (ds/frequencies)
         (ds/map (fn [[k v]] (format "%s: %d" k v)) {:name :format-count})
         (ds/write-text-file output))))
```

Run it locally with:

```bash
lein run in.txt out.txt
```

Run in on Google Cloud (if you have done a `gcloud init` on this machine):

```bash
lein run gs://dataflow-samples/shakespeare/kinglear.txt gs://my-project-tmp/results.txt  --runner=BlockingDataflowRunner --project=my-project --stagingLocation=gs://my-project-staging
```



## Caveats

  - Due to the way the code is loaded when running in distributed mode, you may get some exceptions about unbound vars, especially when using instances with a high number of cpu. They will not however cause the job to fail and are of no consequences. They are caused by the need to prep the Clojure runtime when loading the class files in remote instances and some tricky business with locks and `require`.
  - If you have to write your own low-level `ParDo` objects (you shouldn't), wrap all your code in the `safe-exec` macro to avoid issues with unbound vars. Any good idea about finding a better way to do this would be greatly appreciated!
  - If some of the `UserCodeException` as seen in the cloud UI are mangled and missing the relevant part of the Clojure source code, this is due to a bug with the way the sdk mangles stacktraces in Clojure. In this case look for _ClojureRuntimeException_ in the logs to find the original unaltered stacktrace.

## License

Copyright © 2015, 2016, 2017 Oscaro.com

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.

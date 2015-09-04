(defproject datasplash "0.2.0-SNAPSHOT"
  :description "Clojure API for a more dynamic Google Dataflow"
  :url "https://github.com/ngrunwald/datasplash"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.7.0"]
                 [com.google.cloud.dataflow/google-cloud-dataflow-java-sdk-all "1.0.0"]
                 [com.taoensso/nippy "2.9.0"]
                 [org.clojure/math.combinatorics "0.1.1"]
                 [clj-stacktrace "0.2.8"]
                 [cheshire "5.5.0"]
                 [org.clojure/tools.logging "0.3.1"]
                 [clj-wallhack "1.0.1"]]
  :profiles {:dev {:dependencies [[org.hamcrest/hamcrest-all "1.3"]
                                  [junit/junit "4.12"]
                                  [me.raynes/fs "1.4.6"]]
                   :aot [datasplash.api-test datasplash.examples]
                   :codox {:exclude [datasplash.core datasplash.examples]}
                   :plugins [[codox "0.8.12"]]}
             :uberjar {:aot :all}}
  :main datasplash.examples)

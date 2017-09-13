(defproject datasplash "0.4.2-SNAPSHOT"
  :description "Clojure API for a more dynamic Google Cloud Dataflow"
  :url "https://github.com/ngrunwald/datasplash"
  :license {:name "Eclipse Public License"
            :url  "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[cheshire "5.8.0"]
                 [clj-stacktrace "0.2.8"]
                 [com.google.cloud.dataflow/google-cloud-dataflow-java-sdk-all "1.9.1"]
                 [com.taoensso/nippy "2.13.0"]
                 [org.clojure/clojure "1.8.0"]
                 [org.clojure/math.combinatorics "0.1.4"]
                 [org.clojure/tools.logging "0.3.1"]
                 [clj-time "0.13.0"]
                 [superstring "2.1.0"]]
  :profiles {:dev {:dependencies [[junit/junit "4.12"]
                                  [me.raynes/fs "1.4.6"]
                                  [org.hamcrest/hamcrest-all "1.3"]
                                  [org.slf4j/slf4j-api "1.7.25"]
                                  [org.slf4j/slf4j-jdk14 "1.7.25"]]
                   :aot [clojure.tools.logging.impl datasplash.api-test datasplash.examples clj-time.core]
                   :codox {:namespaces [datasplash.api datasplash.bq datasplash.datastore datasplash.pubsub]
                           :source-uri "https://github.com/ngrunwald/datasplash/blob/v0.4.1/{filepath}#L{line}"
                           :metadata {:doc/format :markdown}}
                   :plugins [[lein-codox "0.9.1"]]}
             :uberjar {:aot :all}}
  :main datasplash.examples)

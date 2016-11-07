(defproject datasplash "0.3.1-SNAPSHOT"
  :description "Clojure API for a more dynamic Google Cloud Dataflow"
  :url "https://github.com/ngrunwald/datasplash"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[cheshire "5.6.3"]
                 [clj-stacktrace "0.2.8"]
                 [com.google.cloud.dataflow/google-cloud-dataflow-java-sdk-all "1.8.0"]
                 [com.taoensso/nippy "2.12.2"]
                 [org.clojure/clojure "1.8.0"]
                 [org.clojure/math.combinatorics "0.1.3"]
                 [org.clojure/tools.logging "0.3.1"]
                 [clj-time "0.12.0"]
                 [superstring "2.1.0"]
                 ;; exclude these if you want something else
                 [org.slf4j/slf4j-api "1.7.21"]
                 [org.slf4j/slf4j-jdk14 "1.7.21"]
                 ]
  :profiles {:dev {:dependencies [[junit/junit "4.12"]
                                  [me.raynes/fs "1.4.6"]
                                  [org.hamcrest/hamcrest-all "1.3"]]
                   :aot [datasplash.api-test datasplash.examples clj-time.core]
                   :codox {:namespaces [datasplash.api datasplash.bq datasplash.datastore]
                           :source-uri "https://github.com/ngrunwald/datasplash/blob/master/{filepath}#L{line}"
                           :metadata {:doc/format :markdown}}
                   :plugins [[lein-codox "0.9.1"]]}
             :uberjar {:aot :all}}
  :main datasplash.examples)

(defproject datasplash "0.2.0-SNAPSHOT"
  :description "Clojure API for a more dynamic Google Dataflow"
  :url "https://github.com/ngrunwald/datasplash"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[cheshire "5.5.0"]
                 [clj-stacktrace "0.2.8"]
                 [com.google.cloud.dataflow/google-cloud-dataflow-java-sdk-all "1.2.1"]
                 [com.taoensso/nippy "2.10.0"]
                 [org.clojure/clojure "1.7.0"]
                 [org.clojure/math.combinatorics "0.1.1"]
                 [org.clojure/tools.logging "0.3.1"]
                 [superstring "2.1.0"]]
  :profiles {:dev {:dependencies [[junit/junit "4.12"]
                                  [me.raynes/fs "1.4.6"]
                                  [org.hamcrest/hamcrest-all "1.3"]]
                   :aot [datasplash.api-test datasplash.examples]
                   :codox {:namespaces [datasplash.api datasplash.bq]
                           :source-uri "https://github.com/ngrunwald/datasplash/blob/master/{filepath}#L{line}"}
                   :plugins [[lein-codox "0.9.0"]]}
             :uberjar {:aot :all}}
  :main datasplash.examples)

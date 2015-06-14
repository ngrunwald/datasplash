(defproject datasplash "0.1.0-SNAPSHOT"
  :description "Clojure API for a more dynamic Google Dataflow"
  :url "https://github.com/ngrunwald/datasplash"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [com.google.cloud.dataflow/google-cloud-dataflow-java-sdk-all "0.4.150602" ]
                 [com.taoensso/nippy "2.9.0"]
                 [org.clojure/math.combinatorics "0.1.1"]
                 [clj-stacktrace "0.2.8"]
                 [cheshire "5.5.0"]]
  :profiles {:dev {:dependencies [[org.hamcrest/hamcrest-all "1.3"]
                                  [junit/junit "4.12"]
                                  [me.raynes/fs "1.4.6"]]
                   :aot [datasplash.api-test]
                   :codox {:exclude [datasplash.core]}
                   :plugins [[codox "0.8.12"]]}})

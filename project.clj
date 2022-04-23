(defproject datasplash "0.7.6-SNAPSHOT"
  :description "Clojure API for a more dynamic Google Cloud Dataflow and (hopefully) Apache BEAM"
  :url "https://github.com/ngrunwald/datasplash"
  :license {:name "Eclipse Public License"
            :url  "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.10.3"]
                 [org.clojure/math.combinatorics "0.1.6"]
                 [org.clojure/tools.logging "1.2.4"]

                 [cheshire "5.10.1"]
                 [clj-stacktrace "0.2.8"]
                 [clj-time "0.15.2"]
                 [com.taoensso/nippy "3.1.1"]
                 [org.apache.beam/beam-sdks-java-core "2.37.0"]
                 [org.apache.beam/beam-sdks-java-io-elasticsearch "2.37.0"]
                 [org.apache.beam/beam-sdks-java-io-kafka "2.37.0"]
                 [org.apache.beam/beam-runners-direct-java "2.37.0"]
                 [org.apache.beam/beam-runners-google-cloud-dataflow-java "2.37.0"]
                 [org.apache.beam/beam-runners-core-java "2.37.0"]
                 [org.apache.kafka/kafka-clients "3.1.0"]
                 [superstring "3.1.0"]]
  :source-paths ["src/clj"]
  :pedantic? false
  :java-source-paths ["src/java"]
  :javac-options ["-Xlint:unchecked"]
  :deploy-repositories {"releases" {:url "https://repo.clojars.org"}}
  :profiles {:dev {:dependencies [[junit/junit "4.13.2"]
                                  [me.raynes/fs "1.4.6"]
                                  [org.hamcrest/hamcrest-all "1.3"]
                                  [ch.qos.logback/logback-core "1.2.11"]]
                   :aot  [clojure.tools.logging.impl datasplash.api-test datasplash.examples clj-time.core datasplash.core clojure.tools.reader.reader-types]}
             :uberjar {:aot :all}}
  :main datasplash.examples)

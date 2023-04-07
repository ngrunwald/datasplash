(defproject datasplash "0.7.18-SNAPSHOT"
  :description "Clojure API for a more dynamic Google Cloud Dataflow and (hopefully) Apache BEAM"
  :url "https://github.com/ngrunwald/datasplash"
  :license {:name "Eclipse Public License"
            :url  "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.11.1"]
                 [org.clojure/math.combinatorics "0.2.0"]
                 [org.clojure/tools.logging "1.2.4"]

                 [com.cnuernber/charred "1.028"]
                 [clj-stacktrace "0.2.8"]
                 [clj-time "0.15.2"]
                 [com.taoensso/nippy "3.2.0"]
                 [org.apache.beam/beam-sdks-java-core "2.46.0"]
                 [org.apache.beam/beam-sdks-java-io-elasticsearch "2.46.0"]
                 [org.apache.beam/beam-sdks-java-io-kafka "2.46.0"]
                 [org.apache.beam/beam-runners-direct-java "2.46.0"]
                 [org.apache.beam/beam-runners-google-cloud-dataflow-java "2.46.0"]
                 [org.apache.beam/beam-runners-core-java "2.46.0"]
                 [org.apache.kafka/kafka-clients "3.4.0"]
                 [superstring "3.1.1"]]
  :pedantic? false
  :source-paths ["src/clj"]
  :java-source-paths ["src/java"]
  :javac-options ["-target" "1.8" "-source" "1.8" "-Xlint:unchecked"]
  :deploy-repositories {"releases" {:url "https://repo.clojars.org"}}
  :profiles {:dev {:dependencies [[ch.qos.logback/logback-core "1.3.4" :upgrade false]
                                  [com.oscaro/tools-io "0.3.30"]]
                   :source-paths ["test"]
                   :resource-paths ["test/resources"]
                   :aot [clj-time.core
                         clojure.tools.logging.impl
                         clojure.tools.reader.reader-types
                         datasplash.api-test
                         datasplash.core
                         datasplash.examples
                         datasplash.testing.assert-test]}})

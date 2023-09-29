(defproject datasplash "0.7.19-SNAPSHOT"
  :description "Clojure API for a more dynamic Google Cloud Dataflow and (hopefully) Apache BEAM"
  :url "https://github.com/ngrunwald/datasplash"
  :license {:name "Eclipse Public License"
            :url  "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.11.1"]
                 [org.clojure/math.combinatorics "0.2.0"]
                 [org.clojure/tools.logging "1.2.4"]

                 [com.cnuernber/charred "1.033"]
                 [clj-stacktrace "0.2.8"]
                 [clj-time "0.15.2"]
                 [com.taoensso/nippy "3.2.0"]
                 [org.apache.beam/beam-sdks-java-core "2.50.0"]
                 [org.apache.beam/beam-sdks-java-io-elasticsearch "2.50.0"]
                 [org.apache.beam/beam-sdks-java-io-kafka "2.50.0"]
                 [org.apache.beam/beam-runners-direct-java "2.50.0"]
                 [org.apache.beam/beam-runners-google-cloud-dataflow-java "2.50.0"]
                 [org.apache.beam/beam-runners-core-java "2.50.0"]
                 [org.apache.kafka/kafka-clients "3.5.1"]
                 [superstring "3.2.0"]

                 [org.slf4j/slf4j-api "2.0.9"]]
  :pedantic? false
  :source-paths ["src/clj"]
  :java-source-paths ["src/java"]
  :javac-options ["-target" "1.8" "-source" "1.8" "-Xlint:unchecked"]
  :deploy-repositories {"releases" {:url "https://repo.clojars.org"}}
  :profiles {:dev {:dependencies [[ch.qos.logback/logback-core "1.3.4" :upgrade false]
                                  [com.oscaro/tools-io "0.3.32"]]
                   :source-paths ["test"]
                   :resource-paths ["test/resources"]
                   :aot [clj-time.core
                         clojure.tools.logging.impl
                         clojure.tools.reader.reader-types
                         datasplash.api-test
                         datasplash.core
                         datasplash.examples
                         datasplash.testing.assert-test]}})

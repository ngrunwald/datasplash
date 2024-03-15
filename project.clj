(defproject datasplash "0.7.20"
  :description "Clojure API for a more dynamic Google Cloud Dataflow and (hopefully) Apache BEAM"
  :url "https://github.com/ngrunwald/datasplash"
  :license {:name "Eclipse Public License"
            :url  "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.11.2"]
                 [org.clojure/math.combinatorics "0.3.0"]
                 [org.clojure/tools.logging "1.3.0"]

                 [com.cnuernber/charred "1.034"]
                 [clj-stacktrace "0.2.8"]
                 [clj-time "0.15.2"]

                 [com.taoensso/nippy "3.3.0"]

                 [org.apache.beam/beam-sdks-java-core "2.54.0"]
                 [org.apache.beam/beam-sdks-java-io-elasticsearch "2.54.0"]
                 [org.apache.beam/beam-sdks-java-io-kafka "2.54.0"]
                 [org.apache.beam/beam-runners-direct-java "2.54.0"]
                 [org.apache.beam/beam-runners-google-cloud-dataflow-java "2.54.0"]
                 [org.apache.beam/beam-runners-core-java "2.54.0"]
                 [org.apache.kafka/kafka-clients "3.7.0"]
                 [superstring "3.2.0"]

                 ;; as of 2.53.0, beam is no longer compatible with slf4j-2. See:
                 ;; https://cwiki.apache.org/confluence/display/BEAM/Java+Tips
                 [org.slf4j/slf4j-api "1.7.36"]]
  :pedantic? false
  :source-paths ["src/clj"]
  :java-source-paths ["src/java"]
  :javac-options ["-target" "1.8" "-source" "1.8" "-Xlint:unchecked"]
  :deploy-repositories {"releases" {:url "https://repo.clojars.org"}}
  :profiles {:dev {:dependencies [[org.slf4j/slf4j-simple "1.7.36"]
                                  [com.oscaro/tools-io "0.3.37"]]
                   :source-paths ["test"]
                   :resource-paths ["test/resources"]
                   :aot [clj-time.core
                         clojure.tools.logging.impl
                         clojure.tools.reader.reader-types
                         datasplash.api-test
                         datasplash.core
                         datasplash.examples
                         datasplash.testing.assert-test]}})

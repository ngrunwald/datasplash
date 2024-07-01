(defproject datasplash "0.7.23-SNAPSHOT"
  :description "Clojure API for a more dynamic Google Cloud Dataflow and (hopefully) Apache BEAM"
  :url "https://github.com/ngrunwald/datasplash"
  :license {:name "Eclipse Public License"
            :url  "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.11.3"]
                 [org.clojure/math.combinatorics "0.3.0"]
                 [org.clojure/tools.logging "1.3.0"]

                 [com.cnuernber/charred "1.034"]
                 [clj-stacktrace "0.2.8"]
                 [clj-time "0.15.2"]

                 [com.taoensso/nippy "3.4.2"]

                 [org.apache.beam/beam-sdks-java-core "2.57.0"]
                 [org.apache.beam/beam-sdks-java-io-elasticsearch "2.57.0"]
                 [org.apache.beam/beam-sdks-java-io-kafka "2.57.0"]
                 [org.apache.beam/beam-runners-direct-java "2.57.0"]
                 [org.apache.beam/beam-runners-google-cloud-dataflow-java "2.57.0"]
                 [org.apache.beam/beam-runners-core-java "2.57.0"]
                 [org.apache.kafka/kafka-clients "3.7.0"]
                 [superstring "3.2.0"]

                 ;; required as of beam 2.55.0
                 [junit/junit "4.13.2"]

                 ;; as of 2.53.0, beam is no longer compatible with slf4j-2. See:
                 ;; https://cwiki.apache.org/confluence/display/BEAM/Java+Tips
                 [org.slf4j/slf4j-api "1.7.36"]]
  :pedantic? false
  :target-path "target/%s/"
  :source-paths ["src/clj"]
  :java-source-paths ["src/java"]
  :javac-options ["-target" "1.8" "-source" "1.8" "-Xlint:unchecked"]
  :deploy-repositories {"releases" {:url "https://repo.clojars.org"}}
  :profiles {:dev {:dependencies
                   [[com.oscaro/tools-io "0.3.38"]
                    ;; include compression libs for tests
                    ;;  zstd
                    [com.github.luben/zstd-jni "1.5.6-3"]
                    ;;  lzo & lzop
                    [io.airlift/aircompressor "0.27"]
                    [com.facebook.presto.hadoop/hadoop-apache2 "3.2.0-1"]
                    ;; compatible log implementation for local runs
                    [org.slf4j/slf4j-simple "1.7.36"]]}
             :test {:source-paths ["test"]
                    :aot :all}})

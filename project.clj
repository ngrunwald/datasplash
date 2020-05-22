(defproject datasplash "0.6.6"
  :description "Clojure API for a more dynamic Google Cloud Dataflow and (hopefully) Apache BEAM"
  :url "https://github.com/ngrunwald/datasplash"
  :license {:name "Eclipse Public License"
            :url  "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [cheshire "5.10.0"]
                 [clj-stacktrace "0.2.8"]
                 [org.apache.beam/beam-sdks-java-core "2.20.0"]
                 [org.apache.beam/beam-sdks-java-io-elasticsearch "2.20.0"]
                 [org.apache.beam/beam-runners-direct-java "2.20.0"]
                 [org.apache.beam/beam-runners-google-cloud-dataflow-java "2.20.0"]
                 [org.apache.beam/beam-runners-core-java "2.20.0"]
                 [com.taoensso/nippy "2.14.0"]
                 [org.clojure/math.combinatorics "0.1.6"]
                 [org.clojure/tools.logging "1.1.0"]
                 [clj-time "0.15.2"]
                 [superstring "3.0.0"]]
  :source-paths ["src/clj"]
  :pedantic? false
  :java-source-paths ["src/java"]
  :profiles {:dev {:dependencies [[junit/junit "4.13"]
                                  [me.raynes/fs "1.4.6"]
                                  [org.hamcrest/hamcrest-all "1.3"]
                                  [org.slf4j/slf4j-api "1.7.30"]
                                  [org.slf4j/slf4j-jdk14 "1.7.30"]]
                   :aot  [clojure.tools.logging.impl datasplash.api-test datasplash.examples clj-time.core datasplash.core clojure.tools.reader.reader-types]}
             :uberjar {:aot :all}}
  :main datasplash.examples)

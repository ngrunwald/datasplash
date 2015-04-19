(defproject datasplash "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :source-paths ["src/clj"]
  :java-source-paths ["src/java"]
  :main datasplash.core
  :aot :all
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [com.google.cloud.dataflow/google-cloud-dataflow-java-sdk-all "0.4.150414"]
                 [com.taoensso/nippy "2.8.0"]])

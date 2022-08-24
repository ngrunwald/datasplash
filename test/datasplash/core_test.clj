(ns datasplash.core-test
  (:require
   [clojure.test :refer [deftest is]]
   [datasplash.core :as sut]))

(deftest args->cli-args-test
  (let [f (fn [str-args kw-args]
            (set (sut/args->cli-args str-args kw-args)))]
    (is (= #{"--appName=unknown"} (f {} {})))
    (is (= #{"--jobName=foo" "--appName=unknown" "--n=42"}
           (f {} {:job-name "foo" :n 42})))
    (is (= #{"--jobName=foo" "--appName=unknown" "--n=42" "--n=94"}
           (f {} {:job-name "foo" :n [42 94]})))
    (is (= #{"--jobName=foo" "--appName=unknown" "--n=42"
             "--jdkAddOpenModules=java.base/java.io=ALL-UNNAMED"
             "--jdkAddOpenModules=java.base/sun.nio.ch=ALL-UNNAMED"}
           (f {} {:job-name "foo" :n 42 :jdk-add-open-modules ["java.base/java.io=ALL-UNNAMED"
                                                               "java.base/sun.nio.ch=ALL-UNNAMED"]})))))

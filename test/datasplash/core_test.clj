(ns datasplash.core-test
  (:require
   [clojure.string :as str]
   [clojure.test :refer [deftest is]]
   [datasplash.core :as sut]
   [datasplash.testing :as dt]
   [datasplash.testing.assert :as dta]
   [tools.io :as tio]))

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

(defn- has-files?
  [adir {:keys [prefix suffix]}]
  (let [files (tio/list-files adir)
        escaped-suffix (str/replace suffix "." "\\.")
        pattern (re-pattern (str prefix "-.+" escaped-suffix "$"))]
    (or (and (seq files)
             (every? #(re-find pattern %) files))
        (throw (ex-info "Files do not match prefix & suffix!"
                        {:pattern pattern :files files})))))

(defmacro ^:private test-compression
  [compression file-ext]
  `(tio/with-tempdir [adir#]
     (let [lines# ["foo" "bar" "baz"]]
       (dt/with-test-pipeline [p# (dt/test-pipeline)]
         (->> (dt/generate lines# p#)
              (sut/write-text-file (str adir# "/lines")
                                   {:compression-type ~compression
                                    :suffix ".txt"
                                    :num-shards 1})))

       (is (has-files? adir# {:prefix "lines" :suffix (str ".txt." ~file-ext)}))

       (dt/with-test-pipeline [p# (dt/test-pipeline)]
         (-> (sut/read-text-file (str adir# "/lines*")
                                 {:compression-type ~compression} p#)
             (dta/as-iterable)
             (dta/contains-only lines#))))))

(deftest write-read-gzip-test
  (test-compression :gzip "gz"))

(deftest write-read-bzip2-test
  (test-compression :bzip2 "bz2"))

(deftest write-read-deflate-test
  (test-compression :deflate "deflate"))

;; Zip & Snappy do not work properly... ¯\_(ツ)_/¯
; (deftest write-read-zip-test
;   (test-compression :zip "zip"))
; (deftest write-read-snappy-test
;   (test-compression :snappy "snappy"))

(deftest write-read-zstd-test
  (test-compression :zstd "zst"))

(deftest write-read-lzo-test
  (test-compression :lzo "lzo_deflate"))

(deftest write-read-lzop-test
  (test-compression :lzop "lzo"))

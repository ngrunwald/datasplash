(ns datasplash.api
  (:refer-clojure :exclude [map filter mapcat group-by
                            distinct flatten concat juxt identity
                            max min])
  (:require [datasplash
             [core :as dt]
             [bq :as bq]]))

(def make-transit-coder dt/make-transit-coder)
(def generate-input dt/generate-input)
(def view dt/view)
(def pardo dt/pardo)
(def identity dt/didentity)
(def map dt/dmap)
(def filter dt/dfilter)
(def mapcat dt/dmapcat)
(def to-edn dt/to-edn)
(def from-edn dt/from-edn)
(def with-keys dt/with-keys)
(def group-by-key dt/group-by-key)
(def group-by dt/dgroup-by)
(def make-pipeline dt/make-pipeline)
(def cogroup dt/cogroup)
(def cogroup-by dt/cogroup-by)
(def join-by dt/join-by)
(def distinct dt/ddistinct)
(def sample dt/sample)
(def flatten dt/dflatten)
(def concat dt/dconcat)
(def combine dt/combine)
(def combine-by dt/combine-by)
(def combine-fn dt/combine-fn)
(def juxt dt/djuxt)
(def sum dt/sum)
(def max dt/dmax)
(def min dt/dmin)
(def mean dt/mean)

;;;;;;;;;;;;;
;; File IO ;;
;;;;;;;;;;;;;

(def read-text-file dt/read-text-file)
(def read-edn-file dt/read-edn-file)
(def write-text-file dt/write-text-file)
(def write-edn-file dt/write-edn-file)

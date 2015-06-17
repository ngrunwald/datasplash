(ns datasplash.api
  (:refer-clojure :exclude [map filter mapcat group-by
                            distinct flatten concat juxt identity
                            max min count frequencies])
  (:require [datasplash
             [core :as dt]
             [bq :as bq]
             [options :as opt]]))

;;;;;;;;;;;;
;; Coders ;;
;;;;;;;;;;;;

(intern *ns* (with-meta 'make-nippy-coder (meta #'dt/make-nippy-coder)) @#'dt/make-nippy-coder)

;;;;;;;;;;;;;
;; Options ;;
;;;;;;;;;;;;;

(intern *ns* (with-meta 'defoptions (meta #'opt/defoptions)) @#'opt/defoptions)
(intern *ns* (with-meta 'get-pipeline-configuration (meta #'dt/get-pipeline-configuration)) @#'dt/get-pipeline-configuration)

;;;;;;;;;;;;;;;
;; Operators ;;
;;;;;;;;;;;;;;;

(intern *ns* (with-meta 'view (meta #'dt/view)) @#'dt/view)
(intern *ns* (with-meta 'pardo (meta #'dt/pardo)) @#'dt/pardo)
(intern *ns* (with-meta 'identity (meta #'dt/didentity)) @#'dt/didentity)
(intern *ns* (with-meta 'map (meta #'dt/dmap)) @#'dt/dmap)
(intern *ns* (with-meta 'filter (meta #'dt/dfilter)) @#'dt/dfilter)
(intern *ns* (with-meta 'mapcat (meta #'dt/dmapcat)) @#'dt/dmapcat)
(intern *ns* (with-meta 'with-keys (meta #'dt/with-keys)) @#'dt/with-keys)
(intern *ns* (with-meta 'group-by-key (meta #'dt/group-by-key)) @#'dt/group-by-key)
(intern *ns* (with-meta 'group-by (meta #'dt/dgroup-by)) @#'dt/dgroup-by)
(intern *ns* (with-meta 'make-pipeline (meta #'dt/make-pipeline)) @#'dt/make-pipeline)
(intern *ns* (with-meta 'run-pipeline (meta #'dt/run-pipeline)) @#'dt/run-pipeline)
(intern *ns* (with-meta 'cogroup (meta #'dt/cogroup)) @#'dt/cogroup)
(intern *ns* (with-meta 'cogroup-by (meta #'dt/cogroup-by)) @#'dt/cogroup-by)
(intern *ns* (with-meta 'join-by (meta #'dt/join-by)) @#'dt/join-by)
(intern *ns* (with-meta 'distinct (meta #'dt/ddistinct)) @#'dt/ddistinct)
(intern *ns* (with-meta 'sample (meta #'dt/sample)) @#'dt/sample)
(intern *ns* (with-meta 'flatten (meta #'dt/dflatten)) @#'dt/dflatten)
(intern *ns* (with-meta 'concat (meta #'dt/dconcat)) @#'dt/dconcat)
(intern *ns* (with-meta 'combine (meta #'dt/combine)) @#'dt/combine)
(intern *ns* (with-meta 'combine-by (meta #'dt/combine-by)) @#'dt/combine-by)
(intern *ns* (with-meta 'combine-fn (meta #'dt/combine-fn)) @#'dt/combine-fn)
(intern *ns* (with-meta 'juxt (meta #'dt/djuxt)) @#'dt/djuxt)

;;;;;;;;;;;;;;;;;
;; Combinators ;;
;;;;;;;;;;;;;;;;;

(intern *ns* (with-meta 'sum (meta #'dt/sum)) @#'dt/sum)
(intern *ns* (with-meta 'max (meta #'dt/dmax)) @#'dt/dmax)
(intern *ns* (with-meta 'min (meta #'dt/dmin)) @#'dt/dmin)
(intern *ns* (with-meta 'mean (meta #'dt/mean)) @#'dt/mean)
(intern *ns* (with-meta 'count (meta #'dt/dcount)) @#'dt/dcount)
(intern *ns* (with-meta 'frequencies (meta #'dt/dfrequencies)) @#'dt/dfrequencies)


;;;;;;;;;;;;;
;; File IO ;;
;;;;;;;;;;;;;

(intern *ns* (with-meta 'read-text-file (meta #'dt/read-text-file)) @#'dt/read-text-file)
(intern *ns* (with-meta 'read-edn-file (meta #'dt/read-edn-file)) @#'dt/read-edn-file)
(intern *ns* (with-meta 'write-text-file (meta #'dt/write-text-file)) @#'dt/write-text-file)
(intern *ns* (with-meta 'write-edn-file (meta #'dt/write-edn-file)) @#'dt/write-edn-file)
(intern *ns* (with-meta 'generate-input (meta #'dt/generate-input)) @#'dt/generate-input)

;;;;;;;;;;;;;
;; Formats ;;
;;;;;;;;;;;;;

(intern *ns* (with-meta 'to-edn (meta #'dt/to-edn)) @#'dt/to-edn)
(intern *ns* (with-meta 'from-edn (meta #'dt/from-edn)) @#'dt/from-edn)

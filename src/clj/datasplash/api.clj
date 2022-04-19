(ns datasplash.api
  (:refer-clojure :exclude [map filter mapcat group-by
                            distinct flatten concat juxt identity
                            max min count frequencies key val partition-by
                            cond->> ->>])
  (:require
   [datasplash.core :as dt]
   [datasplash.options :as opt]))

;;;;;;;;;;;;
;; Coders ;;
;;;;;;;;;;;;

(intern *ns* (with-meta 'make-nippy-coder (meta #'dt/make-nippy-coder)) @#'dt/make-nippy-coder)
(intern *ns* (with-meta 'make-kv-coder (meta #'dt/make-kv-coder)) @#'dt/make-kv-coder)

;;;;;;;;;;;;;
;; Options ;;
;;;;;;;;;;;;;

(intern *ns* (with-meta 'defoptions (meta #'opt/defoptions)) @#'opt/defoptions)
(intern *ns* (with-meta 'get-pipeline-options (meta #'dt/get-pipeline-options)) @#'dt/get-pipeline-options)
(intern *ns* (with-meta 'get-pipeline-configuration (meta #'dt/get-pipeline-configuration)) @#'dt/get-pipeline-configuration)

;;;;;;;;;;;;;;;
;; Operators ;;
;;;;;;;;;;;;;;;

(intern *ns* (with-meta 'view (meta #'dt/view)) @#'dt/view)
(intern *ns* (with-meta 'pardo (meta #'dt/pardo)) @#'dt/pardo)
(intern *ns* (with-meta 'identity (meta #'dt/didentity)) @#'dt/didentity)
(intern *ns* (with-meta 'map (meta #'dt/dmap)) @#'dt/dmap)
(intern *ns* (with-meta 'map-kv (meta #'dt/map-kv)) @#'dt/map-kv)
(intern *ns* (with-meta 'filter (meta #'dt/dfilter)) @#'dt/dfilter)
(intern *ns* (with-meta 'mapcat (meta #'dt/dmapcat)) @#'dt/dmapcat)
(intern *ns* (with-meta 'with-keys (meta #'dt/with-keys)) @#'dt/with-keys)
(intern *ns* (with-meta 'group-by-key (meta #'dt/group-by-key)) @#'dt/group-by-key)
(intern *ns* (with-meta 'group-by (meta #'dt/dgroup-by)) @#'dt/dgroup-by)
(intern *ns* (with-meta 'make-pipeline (meta #'dt/make-pipeline)) @#'dt/make-pipeline)
(intern *ns* (with-meta 'run-pipeline (meta #'dt/run-pipeline)) @#'dt/run-pipeline)
(intern *ns* (with-meta 'wait-pipeline-result (meta #'dt/wait-pipeline-result)) @#'dt/wait-pipeline-result)
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
(intern *ns* (with-meta 'sfn (meta #'dt/sfn)) @#'dt/sfn)

(intern *ns* (with-meta 'partition-fn (meta #'dt/partition-fn)) @#'dt/partition-fn)
(intern *ns* (with-meta 'partition-by (meta #'dt/dpartition-by)) @#'dt/dpartition-by)
(intern *ns* (with-meta 'make-partition-mapping (meta #'dt/make-partition-mapping)) @#'dt/make-partition-mapping)

(intern *ns* (with-meta 'side-inputs (meta #'dt/side-inputs)) @#'dt/side-inputs)
(intern *ns* (with-meta 'side-outputs (meta #'dt/side-outputs)) @#'dt/side-outputs)
(intern *ns* (with-meta 'with-timestamp (meta #'dt/with-timestamp)) @#'dt/with-timestamp)
(intern *ns* (with-meta 'context (meta #'dt/context)) @#'dt/context)
(intern *ns* (with-meta 'state (meta #'dt/state)) @#'dt/state)
(intern *ns* (with-meta 'system (meta #'dt/system)) @#'dt/system)

(intern *ns* (with-meta 'frequencies (meta #'dt/dfrequencies)) @#'dt/dfrequencies)

(intern *ns* (with-meta 'fixed-windows (meta #'dt/fixed-windows)) @#'dt/fixed-windows)
(intern *ns* (with-meta 'sliding-windows (meta #'dt/sliding-windows)) @#'dt/sliding-windows)
(intern *ns* (with-meta 'session-windows (meta #'dt/session-windows)) @#'dt/session-windows)

;;;;;;;;;;;;;;;;;;;;;
;; Syntactic Sugar ;;
;;;;;;;;;;;;;;;;;;;;;

(intern *ns* (with-meta 'ptransform (meta #'dt/ptransform)) @#'dt/ptransform)
(intern *ns* (with-meta 'pt->> (meta #'dt/pt->>)) @#'dt/pt->>)
(intern *ns* (with-meta '->> (meta #'dt/pt->>)) @#'dt/pt->>)
(intern *ns* (with-meta 'cond->> (meta #'dt/pt-cond->>)) @#'dt/pt-cond->>)
(intern *ns* (with-meta 'make-kv (meta #'dt/make-kv)) @#'dt/make-kv)

(intern *ns* (with-meta 'key (meta #'dt/dkey)) @#'dt/dkey)
(intern *ns* (with-meta 'val (meta #'dt/dval)) @#'dt/dval)

(intern *ns* (with-meta 'safe-exec (meta #'dt/safe-exec)) @#'dt/safe-exec)
(intern *ns* (with-meta 'safe-exec-cfg (meta #'dt/safe-exec-cfg)) @#'dt/safe-exec-cfg)

;;;;;;;;;;;;;;;;
;; Combinators ;;
;;;;;;;;;;;;;;;;;

(intern *ns* (with-meta 'count-fn (meta #'dt/count-fn)) @#'dt/count-fn)
(intern *ns* (with-meta 'sum-fn (meta #'dt/sum-fn)) @#'dt/sum-fn)
(intern *ns* (with-meta 'mean-fn (meta #'dt/mean-fn)) @#'dt/mean-fn)
(intern *ns* (with-meta 'max-fn (meta #'dt/max-fn)) @#'dt/max-fn)
(intern *ns* (with-meta 'min-fn (meta #'dt/min-fn)) @#'dt/min-fn)
(intern *ns* (with-meta 'frequencies-fn (meta #'dt/frequencies-fn)) @#'dt/frequencies-fn)

;;;;;;;;;;;;;
;; File IO ;;
;;;;;;;;;;;;;

(intern *ns* (with-meta 'read-text-file (meta #'dt/read-text-file)) @#'dt/read-text-file)
(intern *ns* (with-meta 'read-text-files (meta #'dt/read-text-files)) @#'dt/read-text-files)
(intern *ns* (with-meta 'read-edn-file (meta #'dt/read-edn-file)) @#'dt/read-edn-file)
(intern *ns* (with-meta 'read-edn-files (meta #'dt/read-edn-files)) @#'dt/read-edn-files)
(intern *ns* (with-meta 'read-json-file (meta #'dt/read-json-file)) @#'dt/read-json-file)
(intern *ns* (with-meta 'read-json-files (meta #'dt/read-json-files)) @#'dt/read-json-files)
(intern *ns* (with-meta 'write-text-file (meta #'dt/write-text-file)) @#'dt/write-text-file)
(intern *ns* (with-meta 'write-edn-file (meta #'dt/write-edn-file)) @#'dt/write-edn-file)
(intern *ns* (with-meta 'write-json-file (meta #'dt/write-json-file)) @#'dt/write-json-file)
(intern *ns* (with-meta 'generate-input (meta #'dt/generate-input)) @#'dt/generate-input)
(intern *ns* (with-meta 'filename-policy (meta #'dt/filename-policy)) @#'dt/filename-policy)

;;;;;;;;;;;;;
;; Formats ;;
;;;;;;;;;;;;;

(intern *ns* (with-meta 'to-edn (meta #'dt/to-edn)) @#'dt/to-edn)
(intern *ns* (with-meta 'from-edn (meta #'dt/from-edn)) @#'dt/from-edn)

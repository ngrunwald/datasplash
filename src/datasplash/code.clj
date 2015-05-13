(ns datasplash.code
  (:require
   [clojure.java.io :as io])
  (:import [java.lang.reflect Method]
           [com.google.cloud.dataflow.sdk.values KV PCollection PCollectionList]))

(set! *warn-on-reflection* true)

(defmacro with-ns
  "Evaluates f within ns. Calls (require 'ns) first."
  [ns f]
  `(do
     (require '~ns)
     (binding [*ns* (find-ns '~ns)]
       (eval '~f))))

;; TODO add an option to make the default include/exclude configurable

(defn ^:private freezable? [v]
  (cond
    (fn? v) false
    (instance? PCollection v) false
    (sequential? v) (every? freezable? v)
    (map? v) (every? (partial every? freezable?) v)
    :else true))

;; TODO if something is freezable, but not EDN friendly, maybe use a base64 string?
                                        ;(nippy/freezeable? v) [k `(thaw (decode ~(encode (freeze v))))]
(defn ^:private make-binding [k v]
  (when (and (freezable? k)
             (freezable? v))
    [k `(quote ~v)]))

(defn resource-exists
  "Converts a ns to a resource with the specified ext and checks for existence"
  [ns ext]
  (as-> ns %
    (clojure.string/replace % "." "/")
    (clojure.string/replace % "-" "_")
    (str % ext)
    (clojure.java.io/resource %)))

(defn ns-exists
  "Returns the ns if it exists as a resource"
  [ns]
  (when (and ns (or (resource-exists ns ".clj")
                    (resource-exists ns "__init.class")))
    ns))

(defn build-requires [nss]
  (->> nss
       (filter ns-exists)
       ;; (cons 'pigpen.runtime)
       (distinct)
       (map (fn [r] `'[~r]))
       (cons 'clojure.core/require)))

(defn trap-locals [keys values f]
  (let [args (vec (mapcat make-binding keys values))]
    (if (not-empty args)
      `(let ~args ~f)
      f)))

(defn trap-ns [ns f]
  (if (ns-exists ns)
    `(with-ns ~ns ~f)
    f))

(defn trap* [keys values ns f]
  (->> f
       (trap-locals keys values)
       (trap-ns ns)))

(defmacro trap
  "Returns a form that, when evaluated, will reconsitiute f in namespace ns, in
the presence of any local bindings. If `ns` is not specified, the current
namespace, *ns*, is used.
  Examples:
    => (trap (fn [x] (* x x)))
    (pigpen.runtime/with-ns pigpen-demo.core
      (fn [x] (* x x)))
    => (let [y (* 21 2)]
         (trap
           (fn [x] (+ x y))))
    (pigpen.runtime/with-ns pigpen-demo.core
      (clojure.core/let [y (quote 42)]
        (fn [x] (+ x y))))
  Note: `ns` must exist as a file that will be in the final deployed uberjar.
        If you are in a temporary namespace in a REPL, it will not be included
        in the rewritten version of the expression.
"
  ([f] `(trap '~(ns-name *ns*) ~f))
  ([ns f]
   (let [keys# (vec (keys &env))]
     `(trap* '~keys# ~keys# ~ns '~f))))

(defn trap-values
  "Takes a sequence of options , converts them into a map (if not already), and
optionally traps specific values. The parameter quotable determines which ones
should be quoted and trapped."
  [quotable values]
  (let [values' (cond
                  (map? values) values
                  (sequential? values) (partition 2 values)
                  :else (throw (IllegalArgumentException. "Unknown values")))]
    (->> values'
         (map (fn [[k v]]
                (let [k (keyword k)]
                  [k (if (quotable k) `(trap ~v) v)])))
         (clojure.core/into {}))))

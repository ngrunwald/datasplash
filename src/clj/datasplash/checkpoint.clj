(ns datasplash.checkpoint
  (:require [taoensso.nippy :as nippy]
            [cheshire.core :as json])
  (:import [java.nio.file Paths Files StandardOpenOption LinkOption]
           [java.net URI]
           [java.util Iterator]
           [java.io DataInputStream EOFException]))

(def open-options {:create (StandardOpenOption/valueOf "CREATE")})

(def formats {:nippy {:read-fn nippy/thaw-from-in!
                      :coerce-path-fn
                      (fn [path] (DataInputStream.
                                  (Files/newInputStream path (make-array StandardOpenOption 0))))}
              :json  {:read-fn
                      (fn [^Iterator it]
                        (when (.hasNext it) (json/decode (.next it) true)))
                      :coerce-path-fn
                      (fn [path]
                        (.iterator (Files/lines path)))}})

(defn regular-file?
  [path]
  (Files/isRegularFile path (make-array LinkOption 0)))

(defn read-elements-from-paths
  ([{:keys [read-fn coerce-path-fn] :as format-fns} current-input other-paths]
   (if-let [element (try (read-fn current-input)
                         (catch EOFException e nil))]
     (cons element
           (lazy-seq (read-elements-from-paths format-fns current-input other-paths)))
     (do
       (.close current-input)
       (if-let [new-path (first other-paths)]
         (read-elements-from-paths format-fns (coerce-path-fn new-path) (rest other-paths))
         nil))))
  ([{:keys [coerce-path-fn] :as format-fns} paths]
   (read-elements-from-paths format-fns (coerce-path-fn (first paths)) (rest paths))))

(defn read-checkpoint
  [path {:keys [format]}]
  (let [p (Paths/get (URI/create path))
        ds (Files/newDirectoryStream p)
        paths (filter regular-file? (seq ds))]
    (read-elements-from-paths (formats format) paths)))

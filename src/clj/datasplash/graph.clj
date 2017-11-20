(ns datasplash.graph
  (:require [ubergraph
             [core :as ug]
             [alg :as uga]]))

(def g (ug/digraph [:input1 {:type :input :name "input1"}]
                   [:proc1 {:type :map :name "proc1"}]
                   [:join {:type :join :name "join1"}]
                   [:input2 {:type :input :name "input2"}]
                   [:output1 {:type :output :name "output1"}]
                   [:input1 :proc1]
                   [:proc1 :join]
                   [:input2 :join]
                   [:join :output1]))

(defrecord Topology [agraph end-node options])

(defn make-pipeline
  [options]
  (map->Topology {:agraph (atom (ug/digraph))
                  :end-node nil
                  :options options}))

(defn add-bolt
  [g label operation inputs params]
  (let [with-node (ug/add-nodes-with-attrs g [label {:inputs inputs
                                                     :operation operation}])
        with-inputs (reduce (fn [acc in]
                              (ug/add-directed-edges acc [in label]))
                            with-node inputs)]
    with-inputs))

(defmulti get-topo (fn [& args] (first args)))

(defmethod get-topo :input
  [_ params]
  (last params))

(defmethod get-topo :map
  [_ params]
  (last params))

(defmethod get-topo :output
  [_ params]
  (last params))

(defmulti get-inputs (fn [& args] (first args)))

(defmethod get-inputs :input
  [_ _]
  [])

(defmethod get-inputs :map
  [op params]
  [(:end-node (get-topo op params))])

(defmethod get-inputs :output
  [op params]
  [(:end-node (get-topo op params))])

(defn get-label
  [op params]
  (:name (last (butlast params))))

(defn get-options
  [op params]
  (last (butlast params)))

(defn add-operation!
  [operation params]
  (let [main-topo (get-topo operation params)
        inputs (get-inputs operation params)
        label (get-label operation params)
        options (get-options operation params)]
    (swap! (:agraph main-topo) #(add-bolt % label operation inputs params))
    (assoc main-topo :end-node label)))

(defn gmap
  [f opts pcoll]
  (add-operation! :map [f opts pcoll]))

(defn read-json
  [path opts topo]
  (add-operation! :input [path opts topo]))

(defn write-json
  [path opts pcoll]
  (add-operation! :output [path opts pcoll]))

(defn compile-topo
  [{:keys [agraph]}]
  (let [nodes (uga/topsort @agraph)]
    (println nodes)))

(ns datasplash.testing.assert
  "Namespace to expose `PAssert` based matchers to test pipelines.
   Because `PAssert` is made to be used with JUnit, assertions will only raise `AssertionError`."
  (:require
   [datasplash.core :as ds])
  (:import
   (datasplash.testing PredicateMatcher)
   (org.apache.beam.sdk.testing PAssert
                                PAssert$IterableAssert
                                PAssert$SingletonAssert)
   (org.apache.beam.sdk.transforms SerializableFunction)
   (org.hamcrest MatcherAssert)))

(set! *warn-on-reflection* true)

(defn as-iterable
  "Returns an `IterableAssert` for a given PCollection."
  ^PAssert$IterableAssert
  [pcoll]
  (PAssert/that pcoll))

(defn as-singleton-iterable
  "Returns an `IterableAssert` for a given PCollection.
   The provided PCollection must use an `IterableCoder` and have one element."
  ^PAssert$IterableAssert
  [pcoll]
  (PAssert/thatSingletonIterable pcoll))

(defn as-flattened
  "Returns an `IterableAssert` for a flattened PCollectionList."
  ^PAssert$IterableAssert
  [pcoll-list]
  (PAssert/thatFlattened pcoll-list))

(defn as-singleton
  "Returns a `SingletonAssert` for a given **one element** PCollection."
  ^PAssert$SingletonAssert
  [pcoll]
  (PAssert/thatSingleton pcoll))

(defn as-map
  "Returns a `SingletonAssert` for a given KV PCollection coerce as a map."
  ^PAssert$SingletonAssert
  [kv-pcoll]
  (PAssert/thatMap kv-pcoll))

(defn as-multimap
  "Returns a `SingletonAssert` for a given KV PCollection coerce as a multimap."
  ^PAssert$SingletonAssert
  [kv-pcoll]
  (PAssert/thatMultimap kv-pcoll))

(defprotocol AssertContainsOnly
  (contains-only
    [this expected]
    "Asserts element only contains expected **in any order**."))

(defprotocol AssertIsEmpty
  (is-empty [this]
    "Asserts element is empty."))

(defprotocol AssertEquals
  (equals-to [this expected]
    "Asserts element is equal to expected."))

(defprotocol AssertNotEquals
  (not-equals-to [this expected]
    "Asserts element is not equal to expected value."))

(defprotocol AssertSatisfies
  (satisfies [this pred]
    "Asserts element against a predicate function."))

(defn- satisfies-sfn
  ^SerializableFunction
  [pred]
  (ds/sfn
   (fn [v]
     (MatcherAssert/assertThat v (PredicateMatcher/satisfies pred)))))

(extend-type PAssert$IterableAssert
  AssertContainsOnly
  (contains-only [this ^Iterable expected]
    (.containsInAnyOrder this expected))

  AssertIsEmpty
  (is-empty [this]
    (.empty this))

  AssertSatisfies
  (satisfies [this pred]
    (.satisfies this (satisfies-sfn pred))))

(extend-type PAssert$SingletonAssert
  AssertEquals
  (equals-to [this expected]
    (.isEqualTo this expected))

  AssertNotEquals
  (not-equals-to [this expected]
    (.notEqualTo this expected))

  AssertSatisfies
  (satisfies [this pred]
    (.satisfies this (satisfies-sfn pred))))

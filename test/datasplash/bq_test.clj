(ns datasplash.bq-test
  (:require
   [clj-time.coerce :as c]
   [clojure.test :refer [are deftest is testing]]
   [datasplash.bq :as sut])
  (:import
   (com.google.api.services.bigquery.model TableRow
                                           TableSchema
                                           TimePartitioning)))

(deftest auto-parse-val-test
  (is (= 3134 (sut/auto-parse-val "3134"))
      "num-string->long?")
  (is (= "asd" (sut/auto-parse-val "asd"))
      "string->string?")
  (is (= 3134 (sut/auto-parse-val 3134))
      "other->other?"))

(deftest table-row->clj-test
  (let [table-row (-> {:id "30" :lang "fr" :entity-included? true :foo [{:a {:b 1}}]}
                      sut/clj->table-row)
        rslt (sut/table-row->clj table-row)]
    (is (= {:id "30" :lang "fr" :entity_included true :foo '({:a {:b 1}})}
           rslt))))

(deftest clean-name-test
  (are [expected v] (= expected (sut/clean-name v))
    "a" "a"
    "is_included" "is-included?"
    "232" 232))

(deftest coerce-by-bq-val-test
  (testing "set"
    (let [rslt (sut/coerce-by-bq-val #{1 2 3})]
      (is (list? rslt)
          "as list?")
      (is (= #{1 2 3}
             (set rslt)))))

  (testing "keyword"
    (let [rslt (sut/coerce-by-bq-val :test)]
      (is (string? rslt)
          "as name?")
      (is (= :test
             (keyword rslt)))))

  (testing "symbol"
    (let [rslt (sut/coerce-by-bq-val 'test)]
      (is (string? rslt)
          "as name?")
      (is (= 'test
             (symbol rslt)))))

  (testing "etc"
    (let [d {:a 1}]
      (is (= d (sut/coerce-by-bq-val d))))
    (let [d 1.0]
      (is (= d (sut/coerce-by-bq-val d)))))

  (testing "java.util.date"
    (let [d (c/to-date "2010-01-01")]
      (is (= "2010-01-01 00:00:00"
             (sut/coerce-by-bq-val d))))))

(deftest bqize-keys-test
  (testing "all strings"
    (is (= {"this" {"is" {"a_a" {"test_1" 1}
                          "b__" {"test_2" 2}}
                    "another" {"_c_" {"test" 3}}
                    "32" {"3" 1}}}
           (sut/bqize-keys {"this" {"is" {"a-a" {"test-1" 1}
                                          "b--" {"test-2" 2}}
                                    "another" {"-c-" {"test?" 3}}
                                    32 {3 1}}}))))
  (testing "keywords / other"
    (let [table [{:id "30"
                  :long-schema? true
                  :entities {:beverage {:type "machiatto"
                                        100 4}
                             :snack {:type "financier"
                                     100 24}}}]
          rslt (sut/bqize-keys table)]
      (is (= [{"id" "30"
               "long_schema" true
               "entities" {"beverage" {"type" "machiatto"
                                       "100" 4}
                           "snack" {"type" "financier"
                                    "100" 24}}}]
             rslt)))))

(deftest clj->table-row-test
  (let [rslt (-> {:id "30" :lang "fr" :entity-included? true}
                 sut/clj->table-row)]
    (is (instance? TableRow rslt))
    (is (= {"id" "30" "lang" "fr" "entity_included" true}
           rslt))))

(deftest ->schema-test
  (testing "nominal case"
    (let [schema [{:name "a_float" :type "FLOAT" :mode "NULLABLE"}
                  {:name "another_float" :type :float :mode "NULLABLE"}
                  {:name "a_string" :type "STRING" :mode :nullable}
                  {:name "a_record"
                   :type "RECORD"
                   :mode "REPEATED"
                   :fields
                   [{:name "first-name" :type "STRING" :mode :required}
                    {:name "last-name" :type "STRING" :mode "REQUIRED"}
                    {:name "dislikes-bechamel?" :type :bool :mode :required}]}]

          expected {"fields" [{"name" "a_float" "type" "FLOAT" "mode" "NULLABLE"}
                              {"name" "another_float" "type" "FLOAT" "mode" "NULLABLE"}
                              {"name" "a_string" "type" "STRING" "mode" "NULLABLE"}
                              {"name" "a_record"
                               "type" "RECORD"
                               "mode" "REPEATED"
                               "fields" [{"name" "first_name" "type" "STRING" "mode" "REQUIRED"}
                                         {"name" "last_name" "type" "STRING" "mode" "REQUIRED"}
                                         {"name" "dislikes_bechamel" "type" "BOOL" "mode" "REQUIRED"}]}]}
          rslt (sut/->schema schema)]
      (is (instance? TableSchema rslt))
      (is (= expected rslt))))

  (testing "optional values:"
    (let [schema {:name "a" :type :string}]
      (testing "description"
        (let [test-schema (assoc schema :description "desc")
              expected {"fields" [{"name" "a"
                                   "type" "STRING"
                                   "description" "desc"}]}
              rslt (sut/->schema [test-schema])]
          (is (= expected
                 rslt))))

      (testing "variable length options"
        (let [test-schema (assoc schema :maxLength 200)
              expected {"fields" [{"name" "a"
                                   "type" "STRING"
                                   "maxLength" 200}]}
              rslt (sut/->schema [test-schema])]
          (is (= expected
                 rslt)
              "int maxLength is added to STRING type"))

        (let [test-schema (assoc schema :type :bytes :maxLength 200)
              expected {"fields" [{"name" "a"
                                   "type" "BYTES"
                                   "maxLength" 200}]}
              rslt (sut/->schema [test-schema])]
          (is (= expected
                 rslt)
              "int maxLength is added to BYTES type"))

        (testing "invalid values"
          (let [expected {"fields" [{"name" "a"
                                     "type" "STRING"}]}]
            (is (= expected
                   (sut/->schema [(assoc schema :maxLength "30")]))
                "string maxLength is ignored")
            (is (= expected
                   (sut/->schema [(assoc schema :maxLength 3.1)]))
                "decimal maxLength is ignored")))

        (testing "n/a cases"
          (let [expected {"fields" [{"name" "a", "type" "NUMERIC"}]}]
            (is (= expected
                   (sut/->schema [(assoc schema :type :numeric :maxLength 20)]))
                "not addded to types not string or bytes"))))

      (testing "exact-numeric options"
        (let [test-schema (assoc schema :type :numeric)]
          (testing "nomical case"
            (let [expected {"fields" [{"name" "a"
                                       "type" "NUMERIC"
                                       "precision" 10}]}
                  rslt (sut/->schema [(assoc test-schema :precision 10)])]
              (is (= expected
                     rslt)
                  "precision value is added to numeric type"))

            (let [expected {"fields" [{"name" "a"
                                       "type" "NUMERIC"
                                       "precision" 10
                                       "scale" 3}]}
                  rslt (sut/->schema [(assoc test-schema :precision 10 :scale 3)])]
              (is (= expected
                     rslt)
                  "precision and scale are added to numeric type"))

            (let [expected {"fields" [{"name" "a"
                                       "type" "BIGNUMERIC"
                                       "precision" 10}]}
                  rslt (sut/->schema [(assoc test-schema :type :bignumeric :precision 10)])]
              (is (= expected
                     rslt)
                  "precision is added to bignumeric"))

            (let [expected {"fields" [{"name" "a"
                                       "type" "BIGNUMERIC"
                                       "precision" 10
                                       "scale" 3}]}
                  rslt (sut/->schema [(assoc test-schema :type :bignumeric :precision 10 :scale 3)])]
              (is (= expected
                     rslt)
                  "precision and scale are added to bignumeric")))

          (testing "invalid values"
            (let [expected {"fields" [{"name" "a"
                                       "type" "NUMERIC"}]}]

              (is (= expected (sut/->schema [(assoc test-schema :precision "30")]))
                  "string value is ignored")
              (is (= expected (sut/->schema [(assoc test-schema :precision 12.1)]))
                  "decimal value is ignored")

              (is (= expected (sut/->schema [(assoc test-schema :scale 3)]))
                  "scale is not added when precision is not set")))

          (testing "n/a cases"
            (let [expected {"fields" [{"name" "a"
                                       "type" "STRING"}]}]

              (is (= expected (sut/->schema [(assoc test-schema :type :string :precision 10)]))
                  "not added to types no numeric or bignumeric"))))))))

(deftest ->time-partitioning-test
  (testing "nominal case"
    (let [expected {"type" "DAY"}]
      (doseq [schema #{{:type :day}
                       {:type "DAY"}}
              :let [rslt (sut/->time-partitioning schema)]]
        (is (instance? TimePartitioning rslt))
        (is (= expected
               rslt)
            "with type opt"))

      (let [rslt (sut/->time-partitioning {})]
        (is (= expected
               rslt)
            "default to DAY partitioning"))))

  (testing "optional values:"
    (testing "expiration ms"
      (let [expected {"type" "DAY" "expirationMs" 400}
            rslt (sut/->time-partitioning {:expiration-ms 400})]
        (is (= expected
               rslt)
            "int expiration is added"))

      (let [expected {"type" "DAY"}]
        (doseq [bad-schema-opt #{{:expiration-ms "44"}
                                 {:expiration-ms 43.2}
                                 {:expiration-ms "bad"}
                                 {:expiration-ms nil}}]
          (is (= expected (sut/->time-partitioning bad-schema-opt))
              "is ignored"))))

    (testing "field"
      (let [expected {"type" "DAY" "field" "a field"}
            rslt (sut/->time-partitioning {:field "a field"})]
        (is (= expected
               rslt)
            "string field is added"))

      (let [expected {"type" "DAY"}]
        (doseq [bad-schema-opt #{{:field 123}
                                 {:field nil}
                                 {:field :ff}}]
          (is (= expected
                 (sut/->time-partitioning bad-schema-opt))
              "not string is ignored"))))

    (testing "require-partition-filter"
      (let [expected {"type" "DAY" "requirePartitionFilter" true}
            rslt (sut/->time-partitioning {:require-partition-filter true})]
        (is (= expected
               rslt)
            "when true is added"))

      (let [expected {"type" "DAY"}]
        (doseq [bad-schema-opt #{{:require-partition-filter 1}
                                 {:require-partition-filter nil}
                                 {:require-partition-filter :dsad}
                                 {:require-partition-filter 313.3}}]
          (is (= expected
                 (sut/->time-partitioning bad-schema-opt))
              "non-boolean value is ignored"))))))

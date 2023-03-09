(ns datasplash.bq-schema-test
  (:require
   [clj-time.coerce :as c]
   [clojure.test :refer [are deftest is testing]]
   [datasplash.bq :as sut])
  (:import
   (com.google.api.services.bigquery.model TableRow TableSchema TimePartitioning)))

(deftest auto-parse-val-test
  (is (= 100 (sut/auto-parse-val "100")))
  (is (= "abc" (sut/auto-parse-val "abc")))
  (is (= 100 (sut/auto-parse-val 100))))

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
  (are [expected v] (= expected (sut/coerce-by-bq-val v))

    "2010-01-01 00:00:00" (c/to-date "2010-01-01")
    '(2 1) #{1 2}
    "a" :a
    "a" 'a
    1.0 1.0
    "1.0" "1.0"))

(deftest bqize-keys-test
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
           rslt))))

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
    (let [expected {"type" "DAY"}
          rslt (sut/->time-partitioning {:type :day})]
      (is (instance? TimePartitioning rslt))
      (is (= expected
             rslt)
          "with type opt")

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
        (is (= expected (sut/->time-partitioning {:expiration-ms "44"}))
            "not int expiration is ignored")
        (is (= expected (sut/->time-partitioning {:expiration-ms 43.2}))
            "float expiration is ignored")))

    (testing "field"
      (let [expected {"type" "DAY" "field" "a field"}
            rslt (sut/->time-partitioning {:field "a field"})]
        (is (= expected
               rslt)
            "string field is added"))

      (let [expected {"type" "DAY"}
            rslt (sut/->time-partitioning {:field 123})]
        (is (= expected
               rslt)
            "not string is ignored")))

    (testing "require-partition-filter"
      (let [expected {"type" "DAY" "requirePartitionFilter" true}
            rslt (sut/->time-partitioning {:require-partition-filter true})]
        (is (= expected
               rslt)
            "when true is added"))

      (let [expected {"type" "DAY"}
            rslt (sut/->time-partitioning {:require-partition-filter 1})]
        (is (= expected
               rslt)
            "non-boolean value is ignored")))))

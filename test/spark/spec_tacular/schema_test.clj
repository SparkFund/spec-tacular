(ns spark.spec-tacular.schema-test
  (:refer-clojure :exclude [read-string read])
  (:use clojure.test
        spark.spec-tacular.spec
        [spark.spec-tacular :exclude [diff]]
        spark.spec-tacular.schema
        spark.spec-tacular.test-utils)
  (:require [datomic.api :as d]
            [clojure.java.io :as io]
            [spark.spec-tacular-test :as spt]))

;;;; check tests

(def schema
  [{:db/id 123124
    :db/ident :exspec/var1
    :db/unique :db.unique/identity
    :db/valueType :db.type/string}
   spec-tacular-map])

(def schema-by-value
  [{:db/id 123124
    :db/ident :exspec/var1
    :db/unique :db.unique/value
    :db/valueType :db.type/string}
   spec-tacular-map])

(def schema-alt-keys
  [{:db/id 123124
    :db/ident :exspec/var2
    :db/valueType :db.type/string}
   spec-tacular-map])

(def matching-spec
  (map->Spec
   {:name 'exspec
    :items
    (map map->Item
         [{:name :var1 :type [:one String] :identity? true :unique? true}])}))

(def not-matching-spec
  (map->Spec
   {:name 'exspec
    :items
    (map map->Item
         [{:name :var1 :type [:one String]}])}))

(defspec ExSpec
  [var1 :is-a :string :unique])

(deftest test-check
  (is (empty? (check schema matching-spec)))
  (is (= '("uniqueness for field :var1 in exspec is inconsistant")
         (check schema not-matching-spec)))
  (is (= '("uniqueness for field :var1 in exspec is inconsistant")
         (check schema-by-value matching-spec)))
  (is (= '("inconsistent keys between schema and spec. Diff: [#{:var2} #{:var1} nil]")
         (check schema-alt-keys matching-spec))))

(defspec P
  [name :is-a :string])

(defspec Abode
  [occupants :is-many :P])

(deftest test-schema-write
  (let [schema (from-specs [:P])]
    (check schema (get-spec :P))
    (let [s (with-out-str (write schema *out*))]
      (is (= (re-seq #":db/ident [^,}]*" s)
             [":db/ident :spec-tacular/spec"
              ":db/ident :p/name"]))
      (is (= (re-seq #":db/cardinality :db.cardinality/[^,}]*" s)
             [":db/cardinality :db.cardinality/one"
              ":db/cardinality :db.cardinality/one"]))
      (is (= (re-seq #":db/valueType :db.type/[^,}]*" s)
             [":db/valueType :db.type/keyword"
              ":db/valueType :db.type/string"])))))

(deftest test-normalize
  (let [schema (from-specs [:P :Abode])
        clean-schema (normalize schema)
        dirty-schema (normalize (from-database (to-database! schema)))]
    (is (every? #(and (contains? % :db/ident)
                      (contains? % :db/valueType)
                      (contains? % :db/cardinality)
                      (contains? % :db/unique)
                      (contains? % :db/doc)
                      (= (count %) 5))
                dirty-schema)
        "checks that normalized schemas only contain the fields 
         we use for comparison between schema and spec")
    (is (= clean-schema dirty-schema))))

(deftest test-delta
  (let [old [{:db/ident :foo}]
        new [{:db/ident :foo} {:db/ident :bar}]]
    (is (= (delta old new) [{:db/ident :bar}])
        "adding a new entry to schema"))
  
  (let [old [{:db/ident :foo}]
        new []]
    (is (thrown-with-msg? clojure.lang.ExceptionInfo
                          #"Deletion and renaming not supported"
                          (delta old new))
        "removing an entry from schema")))

(defspec Birthday
  [date :is-a :calendarday])

(def ns-schema (from-namespace *ns*))

(deftest test-from-namespace
  (let [[missing extra both]
        (diff [{:db/ident :p/name,
                :db/valueType :db.type/string,
                :db/cardinality :db.cardinality/one,
                :db/doc "",
                :db.install/_attribute :db.part/db}
               {:db/unique :db.unique/value,
                :db/ident :exspec/var1,
                :db/valueType :db.type/string,
                :db/cardinality :db.cardinality/one,
                :db/doc "",
                :db.install/_attribute :db.part/db}
               {:db/ident :abode/occupants,
                :db/valueType :db.type/ref,
                :db/cardinality :db.cardinality/many,
                :db/doc "",
                :db.install/_attribute :db.part/db}
               {:db/ident :birthday/date,
                :db/valueType :db.type/instant,
                :db/cardinality :db.cardinality/one,
                :db/doc ""}]
              ns-schema)]
    (is (nil? missing) "no missing entries")
    (is (nil? extra) "no extra entries")))

(deftest test-enums
  (is (= (set (normalize (from-specs [spt/IsEnum spt/HasEnum])))
         #{{:db/unique nil,
             :db/ident :hasenum/word,
             :db/valueType :db.type/ref,
             :db/cardinality :db.cardinality/one,
            :db/doc ""}
           {:db/unique nil,
            :db/ident :hasenum/words,
            :db/valueType :db.type/ref,
            :db/cardinality :db.cardinality/many,
            :db/doc ""}
           {:db/ident :IsEnum/how,
            :db/unique nil}
           {:db/ident :IsEnum/now,
            :db/unique nil}
           {:db/ident :IsEnum/brown,
            :db/unique nil}
           {:db/ident :IsEnum/cow,
            :db/unique nil}})))

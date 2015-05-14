(ns spark.sparkspec.typecheck-test
  (:use clojure.test)
  (:require [spark.sparkspec]
            [spark.sparkspec.datomic :as sd]
            [spark.sparkspec.test-specs :as ts]
            [clojure.core.typed :as t]))

(t/ann test-query-typecheck [sd/Database -> (t/Set Long)])
(defn test-query-typecheck
  "not a runtime unit test: but included in typechecking phase"
  [db]
  (sd/q :find ?a :in db :where [:Scm2 {:val1 ?a}]))

(t/ann test-multi-query-typecheck [sd/Database -> (t/Set (t/HVec [String Long]))])
(defn test-multi-query-typecheck
  "not a runtime unit test: but included in typechecking phase"
  [db]
  (sd/q :find [?a ?b] :in db :where [:Scm {:val1 ?a :val2 ?b}]))

(t/ann test-is-multi-vec ts/Scm)
(def test-is-multi-vec
  (ts/scm {:multi ["hi"]}))

(t/ann test-is-multi-list ts/Scm)
(def test-is-multi-list
  (ts/scm {:multi (list "hi")}))

(ns spark.spec-tacular.typecheck-test
  (:use clojure.test)
  (:require [spark.spec-tacular]
            [spark.spec-tacular.datomic :as sd]
            [spark.spec-tacular.test-specs :as ts]
            [clojure.core.typed :as t]))

(t/typed-deps spark.spec-tacular.datomic
              spark.spec-tacular.test-specs)

(t/ann test-query-typecheck [sd/Database -> (t/Set (t/HVec [Long]))])
(defn test-query-typecheck
  "not a runtime unit test: but included in typechecking phase"
  [db]
  (sd/q :find ?a :in db :where [:Scm2 {:val1 ?a}]))

(t/ann test-multi-query-typecheck [sd/Database -> (t/Set (t/HVec [String Long]))])
(defn test-multi-query-typecheck
  "not a runtime unit test: but included in typechecking phase"
  [db]
  (sd/q :find ?a ?b :in db :where [:Scm {:val1 ?a :val2 ?b}]))

(t/ann test-coll-query-typecheck [sd/Database -> (t/Set String)])
(defn test-coll-query-typecheck [db]
  (sd/q :find [?a ...] :in db :where [:Scm {:val1 ?a}]))

(t/ann test-is-multi-set ts/Scm)
(def test-is-multi-vec
  (ts/scm {:multi #{"hi"}}))

(t/ann test-coll-spec [sd/Database -> (t/Option ts/ScmEnum)])
(defn test-coll-spec [db]
  (-> (sd/q :find [:ScmOwnsEnum ...] :in db :where
            [% {:enum :Scm}])
      first :enum))

(t/ann test-get-all-by-spec-scmenum [sd/Database -> (t/ASeq ts/ScmEnum)])
(defn test-get-all-by-spec-scmenum [db]
  (sd/get-all-by-spec db :ScmEnum))

(t/ann test-huh [t/Any -> t/Bool])
(defn test-huh [x]
  (or (ts/scm? x)
      (ts/scmownsenum? x)
      (ts/animal? x)
      (ts/lensecolor? x)))

(t/ann ex-color ts/LenseColor)
(def ex-color :LenseColor/red)

(t/ann ex-spotlights (t/Vec ts/Spotlight))
(def ex-spotlights
  [(ts/spotlight {})
   (ts/spotlight {:color :LenseColor/red})
   (ts/spotlight {:color :LenseColor/green
                  :shaders #{:LenseColor/orange}})])

(t/ann test-color-enum [sd/Database -> (t/Set ts/LenseColor)])
(defn test-color-enum [db]
  (sd/q :find [:LenseColor ...] :in db :where
        [:Spotlight {:color %}]
        [:Spotlight {:shaders %}]))

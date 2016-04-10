(ns spark.spec-tacular.datomic.pull-test
  (:refer-clojure :exclude [assoc!])
  (:require [clojure.test :refer :all]
            [datomic.api :as d]
            [spark.spec-tacular :refer [refless=]]
            [spark.spec-tacular.datomic :refer :all :exclude [db]]
            [spark.spec-tacular.datomic.pull-helpers :refer :all]
            [spark.spec-tacular.schema :as schema]
            [spark.spec-tacular.test-specs :refer :all]
            [spark.spec-tacular.test-utils :refer [with-test-db *conn* db]]))

(def simple-schema
  (cons schema/spec-tacular-map
        (schema/from-namespace (the-ns 'spark.spec-tacular.test-specs))))

(deftest test-pull
  (testing "enums"
    (with-test-db simple-schema
      (let [spotlight (create! {:conn *conn*} (spotlight {:color :LenseColor/red}))]
        (is (= (pull (db) [:color] spotlight)
               {:color :LenseColor/red})))))

  (testing "unions"
    (with-test-db simple-schema
      (let [scm3 (create! {:conn *conn*} (scm3))
            scm2 (create! {:conn *conn*} (scm2 {:val1 123}))
            scm  (create! {:conn *conn*} (scm {:val1 "123" :val2 123}))
            soe  (create! {:conn *conn*} (scmownsenum {:enums [scm scm2 scm3]}))]
        (is (= (pull (db) [{:enums [:val1]}] soe)
               {:enums [{:val1 123} {:val1 "123"}]}))
        (is (= (pull (db) [{:enums [:val1 :val2]}] soe)
               {:enums [{:val1 123} {:val1 "123" :val2 123}]}))))))

(ns spark.spec-tacular.datomic.query-helpers-test
  (:refer-clojure :exclude [assoc!])
  (:require [clojure.test :refer :all]
            [datomic.api :as d]
            [spark.spec-tacular.datomic :refer :all :exclude [db]]
            [spark.spec-tacular.datomic.query-helpers :refer :all]
            [spark.spec-tacular.schema :as schema]
            [spark.spec-tacular.test-specs :refer :all]
            [spark.spec-tacular.test-utils :refer [with-test-db *conn* db]]))

(def simple-schema
  (cons schema/spec-tacular-map
        (schema/from-namespace (the-ns 'spark.spec-tacular.test-specs))))

(deftest test-datomify-where-clause
  (is (= (combine-where-clauses
          '[(and [1] [2] [3])
            (and [4] [5] [6])])
         (combine-where-clauses
          '[[1] [2] [3] (and [4] [5] [6])])
         '(and [1] [2] [3] [4] [5] [6])))
  (is (= (datomify-where-clause '(not [?scm {:spec-tacular/spec :Scm, :val2 5}]))
         '(not (and [?scm :spec-tacular/spec :Scm]
                    [?scm :scm/val2 5]))))
  (is (= (datomify-where-clause '(not-join [?scm]
                                   [?scm {:spec-tacular/spec :Scm, :val2 5}]))
         '(not-join [?scm]
            (and [?scm :spec-tacular/spec :Scm]
                 [?scm :scm/val2 5]))))
  (is (= (datomify-where-clause '[(.before ?date1 ?date2)])
         '[(.before ?date1 ?date2)]))
  (is (= (datomify-where-clause '[(ground :keyword) ?kw])
         '[(ground :keyword) ?kw]))
  (is (= (rest (combine-where-clauses
                (map datomify-where-clause
                     '([?scm {:spec-tacular/spec :Scm
                              :val2 ?val2}]
                       [(- ?val2 5) ?long]))))
         '([?scm :spec-tacular/spec :Scm]
           [?scm :scm/val2 ?val2]
           [(- ?val2 5) ?long])))
  (is (= (rest (combine-where-clauses
                (map datomify-where-clause
                     '([?scm {:spec-tacular/spec :Scm
                              :val2 5}]))))
         '([?scm :spec-tacular/spec :Scm]
           [?scm :scm/val2 5]))))

(deftest test-datomify-find-elems
  (with-test-db simple-schema
    (let [ex (create! {:conn *conn*} (scm {:val2 123}))
          ey (create! {:conn *conn*} (scm {:val2 456}))]
      (testing "scalar"
        (let [{:keys [datomic-find rebuild]} (datomify-find-elems '((instance :Scm ?ex) .))]
          (is (= datomic-find
                 '(?ex .)))
          (is (= (->> (d/q {:find datomic-find
                            :in '($)
                            :where '[[?ex :scm/val2 123]]}
                           (db))
                      (rebuild (db)))
                 ex)))
        (is (thrown? Exception (datomify-find-elems '(56 .)))))
      (testing "relation"
        (let [{:keys [datomic-find rebuild]} (datomify-find-elems '((instance :Scm ?ex)
                                                                    (instance :Scm ?ey)))]
          (is (= datomic-find
                 '(?ex ?ey)))
          (is (= (->> (d/q {:find datomic-find
                            :in '($)
                            :where '[[?ex :scm/val2 123]
                                     [?ey :scm/val2 456]]}
                           (db))
                      (rebuild (db)))
                 #{[ex ey]}))))
      (testing "collection"
        (let [{:keys [datomic-find rebuild]} (datomify-find-elems '([(instance :Scm ?ex) ...]))]
          (is (= datomic-find
                 '([?ex ...])))
          (is (= (->> (d/q {:find datomic-find
                            :in '($)
                            :where '[[?ex :spec-tacular/spec :Scm]]}
                           (db))
                      (rebuild (db)))
                 #{ex ey}))))
      (testing "tuple"
        (let [{:keys [datomic-find rebuild]} (datomify-find-elems '([(instance :Scm ?ex)
                                                                     (instance :Scm ?ey)]))]
          (is (= datomic-find
                 '([?ex ?ey])))
          (is (= (->> (d/q {:find datomic-find
                            :in '($)
                            :where '[[?ex :scm/val2 123]
                                     [?ey :scm/val2 456]]}
                           (db))
                      (rebuild (db)))
                 [ex ey]))))
      (testing "pull"
        (let [{:keys [datomic-find rebuild]} (datomify-find-elems
                                              '([(spec-pull ?ex :Scm [:val2])
                                                 (instance :Scm ?ey)]))]
          (is (= datomic-find
                 '([(pull ?ex [:scm/val2])
                    ?ey])))
          (is (= (->> (d/q {:find datomic-find
                            :in '($)
                            :where '[[?ex :scm/val2 123]
                                     [?ey :scm/val2 456]]}
                           (db))
                      (rebuild (db)))
                 [{:val2 123} ey]))))))
  )

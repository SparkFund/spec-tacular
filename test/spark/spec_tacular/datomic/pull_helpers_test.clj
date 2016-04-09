(ns spark.spec-tacular.datomic.pull-helpers-test
  (:refer-clojure :exclude [assoc!])
  (:require [clj-time.core :as time]
            [clojure.test :refer :all]
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

(deftest test-datomify-spec-pattern
  (let [{:keys [datomic-pattern rebuild]} (datomify-spec-pattern Scm2 [:val1])]
    (is (= datomic-pattern
           [:scm2/val1]))
    (is (= (rebuild nil {:scm2/val1 12345})
           {:val1 12345}))
    (is (= (rebuild nil {})
           {})))
  (let [{:keys [datomic-pattern rebuild]} (datomify-spec-pattern Scm [{:scm2 [:val1]}])]
    (is (= datomic-pattern
           [{:scm/scm2 [:scm2/val1]}]))
    (is (= (rebuild nil {:scm/scm2 {:scm2/val1 12345}})
           {:scm2 {:val1 12345}})))
  (let [{:keys [datomic-pattern rebuild]} (datomify-spec-pattern Scm [:val1])]
    (is (= datomic-pattern
           [:scm/val1]))
    (is (= (rebuild nil {:scm/val1 "12345"})
           {:val1 "12345"})))
  (let [{:keys [datomic-pattern rebuild]} (datomify-spec-pattern Scm [:val1 {:scm2 [:val1]}])]
    (is (= datomic-pattern
           [:scm/val1 {:scm/scm2 [:scm2/val1]}]))
    (is (= (rebuild nil {:scm/scm2 {:scm2/val1 12345}})
           {:scm2 {:val1 12345}}))
    (is (= (rebuild nil {:scm/val1 "12345" :scm/scm2 {:scm2/val1 12345}})
           {:val1 "12345" :scm2 {:val1 12345}})))
  (let [{:keys [datomic-pattern rebuild]} (datomify-spec-pattern Scm2 [(backwards :Scm :scm2)])]
    (is (= datomic-pattern 
           [:scm/_scm2]))
    (with-test-db simple-schema
      (let [scm (create! {:conn *conn*} (scm {:scm2 {:val1 12345}}))
            pulled (d/pull (db) [:scm/_scm2] (get-eid (db) (:scm2 scm)))]
        (is (= pulled
               {:scm/_scm2 [{:db/id (get-eid (db) scm)}]}))
        (is (= (rebuild (db) pulled)
               {(backwards :Scm :scm2) [scm]}))
        (is (= (get (rebuild (db) pulled)
                    (backwards :Scm :scm2))
               [scm])))))
  (let [{:keys [datomic-pattern rebuild]} (datomify-spec-pattern Scm2 [{(backwards :Scm :scm2) [:val1]}])]
    (is (= datomic-pattern
           [{:scm/_scm2 [:scm/val1]}]))
    (with-test-db simple-schema
      (let [scm (create! {:conn *conn*} (scm {:val1 "12345" :scm2 {:val1 12345}}))
            pulled (d/pull (db) [{:scm/_scm2 [:scm/val1]}] (get-eid (db) (:scm2 scm)))]
        (is (= pulled
               {:scm/_scm2 [{:scm/val1 "12345"}]}))
        (is (= (rebuild (db) pulled)
               {(backwards :Scm :scm2) [{:val1 "12345"}]})))))
  (let [{:keys [datomic-pattern rebuild]} (datomify-spec-pattern Scm3 [:val1])]
    (is (= datomic-pattern
           []))
    (is (= (rebuild nil nil)
           {}))
    (is (= (rebuild nil {})
           {})))
  (let [{:keys [datomic-pattern rebuild]} (datomify-spec-pattern ScmEnum [:val1])]
    (is (= datomic-pattern 
           [:scm2/val1 :scm/val1]))
    (is (= (rebuild nil {:scm2/val1 12345})
           {:val1 12345}))
    (is (= (rebuild nil {:scm/val1 "12345"})
           {:val1 "12345"})))
  (let [{:keys [datomic-pattern rebuild]} (datomify-spec-pattern ScmOwnsEnum [:enum {:enums [:val1]}])]
    (is (= datomic-pattern 
           [:scmownsenum/enum
            {:scmownsenum/enums [:scm2/val1
                                 :scm/val1]}]))
    (with-test-db simple-schema
      (let [soe (->> (scmownsenum {:enum (scm {:val2 123})
                                   :enums [(scm {:val1 "123"})
                                           (scm2 {:val1 123})
                                           (scm3)]})
                     (create! {:conn *conn*}))
            pulled (d/pull (db) datomic-pattern (get-eid (db) soe))]
        (is (= pulled
               {:scmownsenum/enum {:db/id (get-eid (db) (:enum soe))}
                :scmownsenum/enums [{:scm/val1 "123"} {:scm2/val1 123}]}))
        (is (= (rebuild (db) {:scmownsenum/enums [{:scm/val1 "123"} {:scm2/val1 123}]})
               {:enums [{:val1 "123"} {:val1 123}]}))
        (is (= (rebuild (db) pulled)
               {:enum (:enum soe)
                :enums [{:val1 "123"} {:val1 123}]})))))
  (with-test-db simple-schema
    (let [c (->> (container {:number 1
                             :one (container {:number 2})
                             :many [(container {:number 3})
                                    (container {:number 4})]})
                 (create! {:conn *conn*}))]
      (let [{:keys [datomic-pattern rebuild]} (datomify-spec-pattern Container [{(backwards :Container :one)
                                                                                 [:number]}])
            pulled {:container/_one {:container/number 1}}]
        (is (= datomic-pattern 
               [{:container/_one [:container/number]}]))
        (is (= (d/pull (db) datomic-pattern (get-eid (db) (:one c)))
               pulled))
        (is (= (rebuild (db) pulled)
               {(backwards :Container :one)
                {:number 1}}))
        (is (= (pull (db) [{(backwards :Container :one) [:number]}] (:one c))
               {(backwards :Container :one) {:number 1}})))
      (let [{:keys [datomic-pattern rebuild]} (datomify-spec-pattern Container [(backwards :Container :one)])
            pulled {:container/_one {:db/id (get-eid (db) c)}}]
        (is (= datomic-pattern
               [:container/_one]))
        (is (= (d/pull (db) datomic-pattern (get-eid (db) (:one c)))
               pulled))
        (is (= (rebuild (db) pulled)
               {(backwards :Container :one) c})))
      (let [{:keys [datomic-pattern rebuild]} (datomify-spec-pattern Container [:many])
            cs (sort-by :number (:many c))
            pulled {:container/many [{:db/id (get-eid (db) (first cs))
                                      :spec-tacular/spec :Container
                                      :container/number 3}
                                     {:db/id (get-eid (db) (second cs))
                                      :spec-tacular/spec :Container
                                      :container/number 4}]}]
        (is (= datomic-pattern
               [:container/many]))
        (is (= (set (:container/many (d/pull (db) datomic-pattern (get-eid (db) c))))
               (set (:container/many pulled))))
        (is (refless= (set (:many (rebuild (db) pulled)))
                      (:many c))
            "they aren't = anymore because they were pulled off the database at different times, but at least they're refless="))))
  (testing "enumeration"
    (let [{:keys [datomic-pattern rebuild]} (datomify-spec-pattern Spotlight [:color])]
      (is (= datomic-pattern 
             [{:spotlight/color [:db/ident]}]))
      (with-test-db simple-schema
        (let [spotlight (create! {:conn *conn*} (spotlight {:color :LenseColor/red}))
              pulled (d/pull (db) datomic-pattern (get-eid (db) spotlight))]
          (is (= pulled
                 {:spotlight/color {:db/ident :LenseColor/red}}))
          (is (= (rebuild (db) pulled)
                 {:color :LenseColor/red}))))))
  (testing "calendarday"
    (let [{:keys [datomic-pattern rebuild]} (datomify-spec-pattern Birthday [:date])]
      (is (= datomic-pattern 
             [:birthday/date]))
      (with-test-db simple-schema
        (let [b (create! {:conn *conn*} (birthday {:date (time/date-time 2015)}))
              pulled (d/pull (db) datomic-pattern (get-eid (db) b))]
          (is (= pulled
                 {:birthday/date #inst "2015-01-01"}))
          (is (= (rebuild (db) pulled)
                 {:date (time/date-time 2015 1)})))))))

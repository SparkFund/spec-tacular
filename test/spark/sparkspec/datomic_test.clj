(ns spark.sparkspec.datomic-test
  (:refer-clojure :exclude [remove read-string read assoc!])
  (:use clojure.test
        spark.sparkspec
        spark.sparkspec.spec
        spark.sparkspec.datomic
        spark.sparkspec.test-utils
        spark.sparkspec.generators)
  (:require [datomic.api :as db]
            [spark.sparkspec.datomic :as sd]
            [spark.sparkspec.schema :as schema]
            [clj-time.core :as time]
            [clojure.walk :as walk]
            [clojure.core.typed :as t]
            [clojure.tools.macro :as m]
            [clojure.test.check :as tc]
            [clojure.test.check.properties :as prop]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.clojure-test :as ct]
            [spark.sparkspec.test-specs :refer :all]))

(def simple-schema
  (cons schema/spec-tactular-map
        (schema/from-namespace (the-ns 'spark.sparkspec.test-specs))))

(deftest test-entity-coercion
  (with-test-db simple-schema
    @(db/transact *conn* [{:db/id (db/tempid :db.part/user)
                           :spec-tacular/spec :Scm
                           :scm/scm2 {:db/id (db/tempid :db.part/user)
                                      :spec-tacular/spec :Scm2
                                      :scm2/val1 42}}])
    (let [scm-eid  (ffirst (db/q '[:find ?v :where [?v :spec-tacular/spec :Scm]] (db)))
          scm-em   (db/entity (db) scm-eid)
          scm2-eid (ffirst (db/q '[:find ?v :where [?v :spec-tacular/spec :Scm2]] (db)))
          scm2-em  (db/entity (db) scm2-eid)]
      (is (= (:scm2 (database-coercion scm-em)) scm2-em))
      (is (= (:val1 (database-coercion scm2-em)) 42))
      (is (= (recursive-ctor :Scm2 (:scm2 (database-coercion scm-em)))
             (scm2 {:val1 42}))))))

(deftest test-transaction-data
  (testing "Scm2"
    (let [gs (gensym)
          si {:db-ref {:eid gs}}
          spec (get-spec :Scm2)
          td #(transaction-data nil spec %1 %2)]
      (testing "valid"
        (is (= (td si {:val1 125})
               [[:db/add gs :scm2/val1 125]]))
        (is (= (td si {:val1 nil})
               []))
        (is (= (td (assoc si :val1 1) {:val1 125})
               [[:db/add gs :scm2/val1 125]]))
        (is (= (td (assoc si :val1 1) {:val1 nil})
               [[:db/retract gs :scm2/val1 1]])))
      (testing "invalid")))

  (testing "Scm"
    (let [gs (gensym)
          si {:db-ref {:eid gs}}
          spec (get-spec :Scm)
          td #(transaction-data nil spec %1 %2)]
      (testing "valid"
        (is (= (td si {:val1 "125" :val2 125})
               [[:db/add gs :scm/val1 "125"]
                [:db/add gs :scm/val2 125]]))
        (is (= (td si {:multi ["125" "1"]})
               [[:db/add gs :scm/multi "1"]
                [:db/add gs :scm/multi "125"]]))
        (is (= (td si {:val1 nil})
               []))
        (is (= (td (assoc si :multi ["1"]) {:multi ["125"]})
               [[:db/retract gs :scm/multi "1"]
                [:db/add gs :scm/multi "125"]]))
        (is (= (td (assoc si :multi ["" "125"]) {:multi [""]})
               [[:db/retract gs :scm/multi "125"]]))
        (is (= (td (assoc si :multi ["" "125"]) {:multi [""]})
               [[:db/retract gs :scm/multi "125"]]))
        (is (= (td si {:scm2 {:db-ref {:eid 120} :val1 42}})
               [[:db/add gs :scm/scm2 120]])))
      (testing "invalid")))

  (testing "ScmOwnsEnum"
    (let [gs (gensym)
          si {:db-ref {:eid gs}}
          spec (get-spec :ScmOwnsEnum)
          td #(transaction-data nil spec %1 %2)
          a-scm3 (assoc (scm3) :db-ref {:eid 5})
          a-scm2 (assoc (scm2) :db-ref {:eid 120})]
      (testing "valid"
        (is (= (td si {:enum a-scm2})
               [[:db/add gs :scmownsenum/enum 120]]))
        (is (= (td si {:enums [a-scm3 a-scm2]})
               [[:db/add gs :scmownsenum/enums 5]
                [:db/add gs :scmownsenum/enums 120]]))
        (is (= (td si {:enum nil})
               []))
        (is (= (td si {:enums nil})
               []))
        (is (= (td (assoc si :enum a-scm2)
                   (assoc si :enum (assoc (scm2) :db-ref {:eid 121})))
               [[:db/add gs :scmownsenum/enum 121]]))
        (is (= (td (assoc si :enums [a-scm2])
                   (assoc si :enums [a-scm3]))
               [[:db/retract gs :scmownsenum/enums 120]
                [:db/add gs :scmownsenum/enums 5]]))
        (is (= (td (assoc si :enum a-scm2) {:enum nil})
               [[:db/retract gs :scmownsenum/enum 120]]))
        (is (= (td (assoc si :enums [a-scm2])
                   (assoc si :enums [(assoc a-scm2 :val1 6)]))
               [])
            "does not update links")
        (is (= (count (td si {:enum (scm2 {:val1 -1})})) 3)))
      (testing "invalid")))

  (testing "ScmLink"
    (let [gs (gensym)
          si {:db-ref {:eid gs}}
          spec (get-spec :ScmLink)
          td #(transaction-data nil spec %1 %2)
          a-scm  (assoc (scm {:val1 "hi"}) :db-ref {:eid 1})
          a-scmp (assoc (scmparent {:scm a-scm}) :db-ref {:eid 2})
          a-scml (assoc (scmlink {:val1 a-scmp}) :db-ref {:eid 3})]
      (testing "valid"
        (let [res (td si a-scml)]
          (is (= true (clojure.core.match/match [res]
                        [([[:db/add eid1 :scmlink/val1 eid2]
                           _
                           [:db/add eid3 :scmparent/scm 1]] :seq)]
                        (and (= eid1 gs) (= eid2 eid3) true)
                        :else res))))
        (let [res (td a-scml (assoc a-scml :val1 nil))]
          (is (= (get-in a-scml [:val1 :db-ref :eid]) 2))
          (is (= res [[:db/retract 3 :scmlink/val1 2]]))))
      (testing "invalid")))

  (testing "graph"
    (let [s (scm {:val1 "string"})
          eid (db/tempid :db.part/user)]
      (is (= (meta (transaction-data nil (get-spec :Scm) nil s
                                     (atom {s eid})))
             {:eid eid})))

    (let [s   (scm2 {:val1 42})
          eid (db/tempid :db.part/user)]
      (is (= (count (transaction-data nil (get-spec :ScmM) nil
                                      {:val s :vals [s]}
                                      (atom {})))
             (count '[[:db/add id1 :spec-tacular/spec :ScmM] ;; edited out ids
                      [:db/add id1 :scmm/val id2]
                      [:db/add id2 :spec-tacular/spec :Scm2]
                      [:db/add id2 :scm2/val1 42]
                      [:db/add id1 :scmm/vals id2]]))))
    (let [e1 (scmmwrap {:val nil, :name nil})
          tmps (atom [])]
      (with-test-db simple-schema
        (let [db (db/db *conn*)]
          (do (transaction-data db (get-spec e1) nil e1 tmps)
              (is (= (transaction-data db (get-spec e1) nil e1 tmps)
                     []))))))))

(deftest test-commit-sp-transactions!
  (let [scm-1 (scm {:val1 "hi" :val2 323 :scm2 (scm2 {:val1 125})})
        scm-non-nested (scm {:val1 "ho" :val2 56666})]
    (testing "new sp->transactions"
      (let [tx-info (with-test-db simple-schema
                      @(db/transact *conn* (sp->transactions (db) scm-1)))]
        (is (not (= (:db-before tx-info) (:db-after tx-info))))
        (is (every? :added (:tx-data tx-info))))
      (with-test-db simple-schema
        (let [tx1 (sp->transactions (db) scm-non-nested)]
          (is (= 1 (count tx1)))
          (is (thrown-with-msg?
               clojure.lang.ExceptionInfo #"is not in the spec"
               (sp->transactions (db) (scm {:extra-key 1})))))))

    (testing "update sp->transactions"
      (with-test-db simple-schema
        (let [tx1 (sp->transactions (db) scm-1)
              tx-info1 @(db/transact *conn* tx1)
              tx2 (sp->transactions (db) (assoc scm-1 :val2 555))
              tx-info2 @(db/transact *conn* tx2)]
          (is (= (ffirst (db/q '[:find ?val2
                                 :where [?eid :scm/val1 "hi"] [?eid :scm/val2 ?val2]]
                               (db)))
                 555)))))

    (testing "db->sp"
      (with-test-db simple-schema
        @(db/transact *conn* (sp->transactions (db) scm-1))
        (let [query (db/q '[:find ?eid :where [?eid :scm/val1 "hi"]] (db))
              e (db/entity (db) (ffirst query))]
          (is (scm? (db->sp (db) e :Scm))))))

    (testing "commit-sp-transactions!"
      (with-test-db simple-schema
        (let [tx1 (sp->transactions (db) scm-1)
              eid (commit-sp-transactions! {:conn *conn*} tx1)]
          (is (scm? (db->sp (db) (db/entity (db) eid) :Scm))
              "The eid we get back should be tied to what we put in."))))))

(defn recursive-expand
  "Returns a completely fleshed out map from the given entity."
  [entity]
  (cond (instance? datomic.Entity entity)        
        (let [em (select-keys entity (keys entity))]
          (reduce (fn [m k] (assoc m k (recursive-expand (k m)))) em (keys em)))
        (seq? entity)
        (map recursive-expand entity)
        (set? entity)
        (set (map recursive-expand entity))        
        :else entity))

(defn inspect-eids
  [eids]
  (->> eids
       (map #(recursive-expand (db/entity (db) (first %))))
       (into #{})))

(deftest nested-is-many
  (testing "can create an object with several is-many children."
    (is (= #{{:scmm/vals #{{:scm2/val1 3 :spec-tacular/spec :Scm2} 
                           {:scm2/val1 4 :spec-tacular/spec :Scm2}}
              :spec-tacular/spec :ScmM}}
           (with-test-db simple-schema
             (let [txs (sp->transactions (db) (recursive-ctor :ScmM {:vals [{:val1 3} {:val1 4}]}))
                   _ @(db/transact *conn* txs)
                   res (db/q '[:find ?eid
                               :where
                               [?eid :scmm/vals ?es]
                               [?es  :scm2/val1 ?v1]] (db))]
               (inspect-eids res)))))))

(deftest several-nested
  (testing "can create multiple distinct objects."
    (with-test-db simple-schema
      (let [txs (concat
                 (sp->transactions (db) (recursive-ctor :ScmM {:vals [{:val1 1}]}))
                 (sp->transactions (db) (recursive-ctor :ScmM {:vals [{:val1 2}]})))
            _   @(db/transact *conn* txs)
            res (db/q '[:find ?eid
                        :where
                        [?eid :scmm/vals ?es]
                        [?es  :scm2/val1 ?v1]] (db))
            res (inspect-eids res)]
        (is (= #{{:scmm/vals #{{:scm2/val1 1, :spec-tacular/spec :Scm2}},
                  :spec-tacular/spec :ScmM}
                 {:scmm/vals #{{:scm2/val1 2, :spec-tacular/spec :Scm2}},
                  :spec-tacular/spec :ScmM}}
               res))))))

(deftest identity-add-one
  (testing "setting an is-many valued object updates the set to exactly the new set"
    (with-test-db simple-schema
      (let [tx1 (sp->transactions
                 (db)
                 (scmm {:identity "myident" :vals [{:val1 1}]}))
            _   @(db/transact *conn* tx1)
            tx2 (sp->transactions
                 (db)
                 (scmm {:identity "myident" :vals [{:val1 2}]}))
            _   @(db/transact *conn* tx2)
            res (db/q '[:find ?eid :where [?eid :scmm/identity _]] (db))
            res (inspect-eids res)]
        (is (= #{{:scmm/identity "myident",
                  :scmm/vals #{{:scm2/val1 2 :spec-tacular/spec :Scm2}}
                  :spec-tacular/spec :ScmM}}
               res))))))

(deftest identity-edit-one-is-many
  (with-test-db simple-schema
    (let [tx1 (sp->transactions
               (db)
               (recursive-ctor :ScmM {:identity "myident"
                                      :vals [{:val1 1}]})) ; initially 1
          _   @(db/transact *conn* tx1)
          child-eid (ffirst (db/q '[:find ?child
                                    :where
                                    [?eid :scmm/vals ?child]] (db))) ; remember the item we added
          tx2 (sp->transactions
               (db)
               (recursive-ctor :ScmM {:identity "myident"
                                      :vals [{:db-ref {:eid child-eid}
                                              :val1 2}]})) ; setting to 2 (fixing via child-eid)
          _   @(db/transact *conn* tx2)
          res (db/q '[:find ?eid
                      :where
                      [?eid :scmm/vals ?_]] (db))
          res (inspect-eids res)]
      (is (= #{{:scmm/identity "myident",
                :scmm/vals #{{:scm2/val1 2 :spec-tacular/spec :Scm2}}
                :spec-tacular/spec :ScmM}}
             res)
          "fixing a unique attribute on parent AND is-many child will upsert that item, and not add a new one."))))

(deftest removed-data-should-reflect-in-db
  (testing "removing simple data"
    (with-test-db simple-schema
      (let [scm-a (scm {:val1 "name" :val2 123124})
            scm-b (scm {:val1 "name"})]
        @(db/transact *conn* (sp->transactions (db) scm-a))
        @(db/transact *conn* (sp->transactions (db) scm-b))
        (let [eid (ffirst (db/q '[:find ?eid :where [?eid :scm/val1 "name"]] (db)))
              entity (db/entity (db) eid)]
          (is (nil? (:scm/val2 entity)))))))
  (testing "removing multi data"
    (with-test-db simple-schema
      (let [scm-a (scm {:val1 "name" :val2 123124 :multi ["hi" "ho"]})
            scm-b (scm {:val1 "name"})
            scm-c (scm {:val1 "name" :multi ["hi"]})]
        @(db/transact *conn* (sp->transactions (db) scm-a))
        @(db/transact *conn* (sp->transactions (db) scm-b))
        (let [eid (ffirst (db/q '[:find ?eid :where [?eid :scm/val1 "name"]] (db)))
              entity (db/entity (db) eid)]
          (is (nil? (:scm/multi entity))))
        @(db/transact *conn* (sp->transactions (db) scm-c))
        (let [eid (ffirst (db/q '[:find ?eid :where [?eid :scm/val1 "name"]] (db)))
              entity (db/entity (db) eid)]
          (is (= (:scm/multi entity) #{"hi"})))))))

(deftest sp-filter-with-mask-test
  (is (= (sp-filter-with-mask {:val2 true} :Scm (scm {:val1 "1" :val2 1}))
         (scm {:val2 1})))
  (is (= (sp-filter-with-mask {:val2 true} :Scm (scm {:db-ref {:eid 123} :val1 "1" :val2 1}))
         (scm {:val2 1 :db-ref {:eid 123}}))
      "eids come along for the ride")
  (is (= (sp-filter-with-mask true :Scm (scm {:db-ref {:eid 123} :val1 "1" :val2 1}))
         (scm {:db-ref {:eid 123}}))
      "only eids on a 'true' mask")
  (is (= (sp-filter-with-mask {:scm2 true} :Scm (scm {:db-ref {:eid 123} :val1 "1" :val2 1 :scm2 (scm2 {:val1 1 :db-ref {:eid 321}})}))
         (scm {:db-ref {:eid 123} :scm2 (scm2 {:db-ref {:eid 321}})}))
      "only eids on a 'true' mask, nested.")

  (let [original (scmownsenum {:enums [(scm2 {:val1 42})]})
        updates  {:enums [(scm {:val2 53})]}
        expected (merge original updates)
        mask     (item-mask :ScmOwnsEnum expected)]
    (is (= mask {:enums {:Scm {:val2 true}}}))
    (is (doall (sp-filter-with-mask mask :ScmOwnsEnum original)))))

(deftest update-tests
  (with-test-db simple-schema
    (let [a1 (scm2 {:val1 1})
          a2 (scm2 {:val1 2})
          a1id (create-sp! {:conn *conn*} a1)
          a2id (create-sp! {:conn *conn*} a2)
          a1db (get-by-eid (db) a1id)
          a2db (get-by-eid (db) a2id)
          b1 (scm {:val1 "b" :scm2 a1db :multi ["1" "2"]})
          b1eid (create-sp! {:conn *conn*} b1)
          b1db (get-by-eid (db) b1eid)
          _ (is (= 1 (:val1 (:scm2 b1db)))
                "create compound objects referring to a1db")
          _ (update-sp! {:conn *conn*}
                        b1db
                        (assoc b1db :scm2 a2db))
          b1db (get-by-eid (db) b1eid)
          _ (is (= 2 (:val1 (:scm2 b1db)))
                "succesfully switched sub-object to refer to the a2db.")
          _ (update-sp! {:conn *conn*}
                        b1db
                        (assoc b1db :scm2 (assoc a2 :val1 666)))
          b1db (get-by-eid (db) b1eid)
          _ (is (= 666 (:val1 (:scm2 b1db)))
                "We've created a new :scm2 w/ 666 -- we assoc'd to the a2 which had not been added to the db yet.")
          _ (update-sp! {:conn *conn*}
                        b1db
                        (assoc b1db :val1 "c"))
          _ (update-sp! {:conn *conn*} ; intentionally did not fetch this write to val1
                        (dissoc b1db :val1)
                        (assoc (dissoc b1db :val1) :scm2 a2db))
          b1db (get-by-eid (db) b1eid)
          _ (is (= 2 (:val1 (:scm2 b1db)))
                "We can update if there is a concurrent write to a key we don't care about")
          _ (update-sp! {:conn *conn*}
                        b1db
                        (assoc b1db :multi []))
          b1db (get-by-eid (db) b1eid)
          _ (is (= [] (:multi b1db))
                "Can update to delete lists ")
          b2eid (create-sp! {:conn *conn*} (scmm {:vals [(scm2 {:val1 1})
                                                         (scm2 {:val1 2})]}))
          b2db (get-by-eid (db) b2eid)
          _ (update-sp! {:conn *conn*}
                        b2db
                        (assoc b2db :vals []))
          b2db (get-by-eid (db) b2eid)
          _ (is (= [] (:vals b2db))
                "Can delete non-primitive is-manys too")
          b2eid (create-sp! {:conn *conn*} (scmm {:vals [(scm2 {:val1 1})
                                                         (scm2 {:val1 2})]}))
          b3db (get-by-eid (db) b2eid)
          _ (is (= (item-mask :ScmM (assoc b3db :vals nil))
                   (item-mask :ScmM (assoc b3db :vals []))))
          _ (update-sp! {:conn *conn*}
                        b3db
                        (assoc b3db :vals nil))
          b3db (get-by-eid (db) b2eid)
          _ (is (= [] (:vals b3db))
                "Can delete non-primitive is-manys via nil too")
          scmreq-eid (create-sp! {:conn *conn*} (scmreq {:name "blah"}))
          scmreq-db (get-by-eid (db) scmreq-eid)
          #_(is (thrown-with-msg? clojure.lang.ExceptionInfo #"attempt to delete a required field"
                                    (update-sp! {:conn *conn*}
                                                scmreq-db
                                                (assoc scmreq-db :name nil))))])))

(deftest enum-tests
  (with-test-db simple-schema
    (let [tx1 (sp->transactions (db) (scmownsenum {:enum (scm3)}))
          tx-info @(db/transact *conn* tx1)]
      (is (= 1 (count tx1)))
      (is (= 2 (count (:tempids tx-info))))))
  (with-test-db simple-schema
    (let [tx1 (sp->transactions (db) (scmownsenum {:enum (scm3)}))
          tx-info1 @(db/transact *conn* tx1)
          tx2 (sp->transactions (db) (assoc (scmownsenum {:enum (scm3)}) :enum (scm2 {:val1 123})))
          tx-info2 @(db/transact *conn* tx2)]
      (is (= (db/q '[:find ?val
                     :where
                     [?owner :scmownsenum/enum ?eid]
                     [?eid :scm2/val1 ?val] ]
                   (db))
             #{[123]})
          "Can update to change an enum field from one to another")))
  (with-test-db simple-schema
    (let [tx1 (sp->transactions (db) (scmownsenum {:enums [(scm3) (scm3) (scm2 {:val1 123})]}))
          eid (commit-sp-transactions! {:conn *conn*} tx1)
          sp (db->sp (db) (db/entity (db) eid))
          _ (is (= (count (db/q '[:find ?eid
                                  :where
                                  [?owner :scmownsenum/enums ?eid]]
                                (db)))
                   3)
                "can store a list of enums")
          tx2 (sp->transactions (db) (update-in sp [:enums] concat [(scm3) (scm3)]))
          tx-info2 @(db/transact *conn* tx2)]
      (is (= (count (db/q '[:find ?eid
                            :where
                            [?owner :scmownsenum/enums ?eid]]
                          (db)))
             5)
          "can append more enums to a list of enums")))
  (with-test-db simple-schema
    (let [tx1 (sp->transactions (db) (scmownsenum {:enums [(scm2 {:val1 1}) (scm2 {:val1 2})]}))
          eid (commit-sp-transactions! {:conn *conn*} tx1)
          sp (db->sp (db) (db/entity (db) eid))
          tx2 (sp->transactions (db) (assoc sp :enums [(scm2 {:val1 1})]))
          eid2 (commit-sp-transactions! {:conn *conn*} tx2)
          ]
      (is (= (count (:enums (db->sp (db) (db/entity (db) eid2))))
             1)
          "can delete enums from a list of enums"))))

(deftest mask-tests
  (with-test-db simple-schema
    (let [a1 (scm2 {:val1 1})
          a2 (scm2 {:val1 2})
          a1id (create-sp! {:conn *conn*} a1)
          a2id (create-sp! {:conn *conn*} a2)
          a1db (get-by-eid (db) a1id)
          a2db (get-by-eid (db) a2id)
          b1 (scm {:val1 "b" :scm2 a1db})
          b1eid (create-sp! {:conn *conn*} b1)
          b1db (get-by-eid (db) b1eid)
          _ (is (= 1 (:val1 (:scm2 b1db)))
                "create compound objects referring to a1db")
          b1eid (masked-update-sp! {:conn *conn*}
                                   (assoc b1db :scm2 a2db)
                                   {:scm2 (new-components-mask a2 :Scm2)})
          b1db (get-by-eid (db) b1eid)
          _ (is (= 2 (:val1 (:scm2 b1db)))
                "succesfully switched sub-object to refer to the a2db.")
          b1eid (masked-update-sp! {:conn *conn*}
                                   (assoc b1db :scm2 a1db)
                                   {})
          b1db (get-by-eid (db) b1eid)
          _ (is (= 2 (:val1 (:scm2 b1db)))
                "still a2 -- our mask didn't mention we would change :scm2")
          b1eid (masked-update-sp! {:conn *conn*}
                                   (assoc b1db :scm2 (assoc a2 :val1 666))
                                   {:scm2 (new-components-mask a2 :Scm2)})
          b1db (get-by-eid (db) b1eid)
          _ (is (= 666 (:val1 (:scm2 b1db)))
                "We've created a new :scm2 w/ 666 -- we assoc'd to the a2 which had not been added to the db yet.")
          b1eid (masked-update-sp! {:conn *conn*}
                                   (assoc b1db :scm2 (assoc a2db :val1 666))
                                   {:scm2 (new-components-mask a2db :Scm2)})
          b1db (get-by-eid (db) b1eid)
          _  (is (= 2 (:val1 (:scm2 b1db)))
                 "switched back to 2, NOT 666. our mask says we aren't editing the values in from-db values")
          c1eid (create-sp! {:conn *conn*} (scmm {:identity "c1"}))
          c1db (get-by-eid (db) c1eid)
          _ (masked-update-sp! {:conn *conn*}
                               (assoc c1db :vals [a1db])
                               {:vals true})
          c1db (get-by-eid (db) c1eid)
          _ (is (= #{1}
                   (->> (:vals c1db)
                        (map :val1)
                        (into #{})))
                "Can add an entity by ref when masked as 'true'")
          _ (masked-update-sp! {:conn *conn*}
                               (update-in c1db [:vals] conj a2db)
                               {:vals true})
          c1db (get-by-eid (db) c1eid)
          _ (is (= #{1 2}
                   (->> (:vals c1db)
                        (map :val1)
                        (into #{})))
                "can point to more entities via an :is-many by ref only.")
          _ (masked-update-sp! {:conn *conn*}
                               (assoc c1db :vals [a2db])
                               {:vals true})
          c1db (get-by-eid (db) c1eid)
          _ (is (= #{2}
                   (->> (:vals c1db)
                        (map :val1)
                        (into #{})))
                "can delete entities by ref via an :is-many masked as 'true'")
          _ (masked-update-sp! {:conn *conn*}
                               (assoc c1db :vals [(assoc a2db :val1 666)])
                               {:vals true})
          c1db (get-by-eid (db) c1eid)
          _ (is (= #{2}
                   (->> (:vals c1db)
                        (map :val1)
                        (into #{})))
                "can't edit a sub-thing via an :is-many masked as 'true'")])))

(deftest item-mask-test
  (is (= (item-mask :Scm {:val1 "b" :val2 nil})
         {:val1 true, :val2 true})
      "explicitly nil (but present) keyvals should be true in the mask")
  (is (= (item-mask :ScmOwnsEnum (scmownsenum {:enums [(scm2 {:val1 1})
                                                       (scm3)]}))
         {:enums {:Scm3 {}, :Scm2 {:val1 true}}}))
  (is (= (item-mask :ScmOwnsEnum (scmownsenum {:enums [(scm2 {:db-ref {:eid 1}})
                                                       (scm3 {:db-ref {:eid 2}})
                                                       (scm3 {:db-ref {:eid 4}})]}))
         {:enums {:Scm3 true, :Scm2 true}})
      "we collapse the is-many items properly")
  (is (= (item-mask :ScmM {:vals [] :identity nil})
         {:identity true, :vals true})
      "empty lists result in a 'true' mask value.")
  (is (= (item-mask :ScmOwnsEnum (scmownsenum {:enums []}))
         {:enums true})
      "empty lists of enums are 'true' as well")
  (is (= (item-mask :ScmOwnsEnum (assoc (scmownsenum) :enums nil))
         {:enums true})
      "nil lists of enums are 'true' as well")
  (with-test-db simple-schema
    (let [a1 (scm2 {:val1 1})
          a2 (scm2 {:val1 2})
          a1id (create-sp! {:conn *conn*} a1)
          a2id (create-sp! {:conn *conn*} a2)
          a1db (get-by-eid (db) a1id)
          a2db (get-by-eid (db) a2id)
          b1 (scm {:val1 "b" :scm2 a1db})
          b1eid (masked-create-sp! {:conn *conn*} b1 (item-mask :Scm b1))
          b1db (get-by-eid (db) b1eid)
          _ (is (= 1 (:val1 (:scm2 b1db)))
                "create compound objects referring to a1db")
          _ (let [b2 (assoc b1db :scm2 (scm2 {:db-ref {:eid (get-in a2db [:db-ref :eid])}}))]
              (masked-update-sp! {:conn *conn*}
                                 b2
                                 (item-mask :Scm b2)))
          b1db (get-by-eid (db) b1eid)
          _ (is (= 2 (:val1 (:scm2 b1db)))
                "succesfully switched sub-object to refer to the a2db.")
          _ (let [b2 (assoc b1db :scm2 (assoc a2db :val1 3))]
              (masked-update-sp! {:conn *conn*}
                                 b2
                                 (item-mask :Scm b2)))
          b1db (get-by-eid (db) b1eid)
          _ (is (= 3 (:val1 (:scm2 b1db)))
                "Can update a sub-value if we want")
          a2db (get-by-eid (db) a2id)
          _ (is (= 3 (:val1 a2db))
                "Can update a sub-value if we want, and it changes the sub-thing")
          c1id (create-sp! {:conn *conn*} (scmownsenum {:enum (scm2 {:val1 4})}))
          c1db (get-by-eid (db) c1id)
          _ (let [c2 (assoc c1db :enum (scm3))]
              (masked-update-sp! {:conn *conn*} c2 (item-mask :ScmOwnsEnum c2)))
          c1db (get-by-eid (db) c1id)
          _ (is (= (scm3) (dissoc (:enum c1db) :db-ref))
                "can swich to a new enum value")
          _ (let [c2 (assoc c1db :enum (scm2 {:db-ref {:eid (get-in a1db [:db-ref :eid])}}))]
              (masked-update-sp! {:conn *conn*} c2 (item-mask :ScmOwnsEnum c2)))
          c1db (get-by-eid (db) c1id)
          _ (is (= 1 (:val1 (:enum c1db)))
                "can swich to a new enum value via only eid")
          ])))

(deftest transaction-log-test
  (with-test-db simple-schema
    (let [_ (create-sp! {:conn *conn* :transaction-log (scm {:val1 "log1"})}
                        (scm2 {:val1 1234}))
          r (db/q '[:find ?e ?v
                    :where
                    [?e :scm/val1 ?v]]
                  (db))
          [e v] (first r)
          _ (is (= 1 (count r)) "only one thing logged at this point")
          _ (is (= "log1" v))
          r2 (db/q '[:find ?v
                     :in $ ?e
                     :where
                     [?e :db/txInstant ?v]]
                   (db) e)
          _ (is (instance? java.util.Date (ffirst r2)) "we annotated a transaction object with a txnInstant")
          r3 (db/q '[:find ?v
                     :in $ ?e
                     :where
                     [?scm2 :scm2/val1 ?v ?e]]
                   (db) e)
          _ (is (= #{[1234]} r3) "we annotated the transaction that created the thing with val1 1234")
          _ (create-sp! {:conn *conn* :transaction-log (scm {:val1 "log2"})}
                        (scm2 {:val1 5678}))
          _ (is (= 2 (count (get-all-by-spec (db) :Scm))) "now have 2 logs")
          [e2] (first (db/q '[:find ?e
                              :in $ ?v
                              :where
                              [?e :scm/val1 ?v]]
                            (db) "log2"))
          r4 (db/q '[:find ?scm2
                     :in $ ?v ?e
                     :where
                     [?scm2 :scm2/val1 ?v ?e]]
                   (db) 5678 e2)
          _ (is (= 1 (count r4)) "we've annotated the other one like we expect.")])))

(deftest query-tests
  (is (= (parse-query '(:find ?a :in (db) :where [:ScmParent {:scm {:val2 ?a}}]))
         {:f '[?a] :db '(db) :wc '[[:ScmParent {:scm {:val2 ?a}}]] :coll? false}))

  (testing "primitive data"
    (with-test-db simple-schema
      (is (= #{} (q :find ?a :in (db) :where [:ScmParent {:scm {:val2 ?a}}]))
          "nothing returned on fresh db.")

      (let [a1 (scmparent {:scm (scm {:val1 "a" :val2 1})})
            a2 (scmparent {:scm (scm {:val1 "b" :val2 2})})]
        (create-sp! {:conn *conn*} a1)
        (create-sp! {:conn *conn*} a2))
  
      (is (= #{[1] [2]}
             (q :find ?a :in (db) :where
                [:ScmParent {:scm {:val2 ?a}}]))
          "simple one-attribute returns (a ?-prefixed symbol isn't needed- just idiomatic cf datomic)")
      (is (= #{[1 "a"] [2 "b"]}
             (q :find ?a ?b :in (db) :where
                [:ScmParent {:scm {:val1 ?b :val2 ?a}}]))
          "multiple attribute returns")
      (is (= #{[1]}
             (q :find ?a :in (db) :where
                [:ScmParent {:scm {:val1 "a" :val2 ?a}}]))
          "can use literals in the pattern to fix values")
      (is (= #{["b"]}
             (let [two 2]
               (q :find ?a :in (db) :where 
                  [:ScmParent {:scm {:val1 ?a :val2 two}}])))
          "can use regular variables to fix values")
      (is (= #{["b"]}
             (q :find ?a :in (db) :where
                [:ScmParent {:scm {:val1 ?a :val2 (let [?a 2] ?a)}}]))
          "return variables respect lexical scope and don't clobber lets")
      (is (= #{["b"]}
             (q :find ?a :in (db) :where
                [:ScmParent {:scm {:val1 ?a :val2 ((fn [?a] ?a) 2)}}]))
          "return variables respect lexical scope and don't clobber fns")))

  (testing "compound data"
    (with-test-db simple-schema
      (let [e-scm2 (scm2 {:val1 5})
            e-scm  (scm {:val2 5 :scm2 e-scm2})
            e-scmp (scmparent {:scm e-scm})]
        (create-sp! {:conn *conn*} e-scmp)

        (let [a-scm2 (->> (q :find :Scm2 :in (db) :where
                             [:Scm {:scm2 %}])
                          ffirst)]
          (is (= (:val1 a-scm2) 5)
              "can use keywords on returned entities")
          (is (not (:bad-kw a-scm2)))
          (is (and (map? (:db-ref a-scm2))
                   (instance? java.lang.Long (:eid (:db-ref a-scm2))))
              "allow :db-ref keyword access"))
        
        (let [a-scm (ffirst (q :find :Scm :in (db) :where [% {:scm2 [:Scm2 {:val1 5}]}]))]
          (testing "equality on returned entities"
            (is (= a-scm e-scm))
            (is (= e-scm a-scm))))
        
        (let [[a-scm a-scm2]
              ,(->> (q :find :Scm :Scm2 :in (db) :where
                       [%1 {:scm2 %2}])
                    first)]
          (testing "equality on returned sub-entities"
            (is (= (:scm2 a-scm) a-scm2))
            (is (= a-scm2 e-scm2))
            (is (= a-scm  e-scm)))
          (is (not (:val1 a-scm)))
          (is (map? (:db-ref (:scm2 a-scm)))
              "allow :db-ref keyword access on sub-entities"))

        (testing "is-many"
          (let [e-scmm (scmm {:identity "hi" :vals [(scm2 {:val1 42}) (scm2 {:val1 7})]})
                scmm-eid (create-sp! {:conn *conn*} e-scmm)
                a-scmm1 (ffirst (q :find :ScmM :in (db) :where [% {:identity "hi"}]))
                a-scmm2 (recursive-ctor :ScmM (db/entity (db) scmm-eid))]
            (is (= a-scmm1 e-scmm))
            (is (= a-scmm2 e-scmm)))

          (let [esw (scmmwrap 
                     {:name "scmwrap"
                      :val (scmm {:identity "hi" :vals [(scm2 {:val1 42}) (scm2 {:val1 7})]})})
                esw-id (create-sp! {:conn *conn*} esw)
                asw1 (ffirst (q :find :ScmM :in (db) :where [:ScmMWrap {:name "scmwrap" :val %}]))
                asw2 (:val (recursive-ctor :ScmMWrap (db/entity (db) esw-id)))]
            ;; (is (= asw1 esw) "returned from query equality")
            (testing "lazy-ctor"
              #_(is (instance? spark.sparkspec.test_specs.l_ScmM asw2) "type") ;; TODO
              (is (:identity asw2) "keyword access")
              (is (= (type (:vals asw2)) clojure.lang.PersistentVector) "keyword access")
              ;; (is (= asw2 esw) "equality")
              ))))
      (testing "coll"
          (let [ex (create! {:conn *conn*} (scmmwrap {:val {:val (scm {:val1 "foobar"})}}))]
            (is (contains? (sd/q :find [:Scm ...] :in (db) :where
                                 [:ScmMWrap {:val [:ScmM {:val [% {:val1 "foobar"}]}]}])
                           (get-in ex [:val :val])))))
      (testing "absent field access"
        (let [eid (create-sp! {:conn *conn*} (scm2))
              a-scm2 (recursive-ctor :Scm2 (db/entity (db) eid))]
          (let [b (not (:val1 (scm2 a-scm2)))] ;; lol printing it out draws an early error
            (is b))
          #_(is (not (:val1 (scm2 a-scm2))))))))

  (testing "complex dispatch"
    (with-test-db simple-schema
      (create! {:conn *conn*} (scm {:scm2 (scm2 {:val1 22})}))
      (is (= (q :find ?type :in (db) :where
                [:Scm {:scm2 {:spec-tacular/spec ?type}}])
             #{[:Scm2]}))
      (let [soe (create! {:conn *conn*}
                         (scmownsenum {:enum (scm2 {:val1 42})}))]
        (is (= (q :find ?type ?any :in (db) :where
                  [:ScmOwnsEnum {:enum [?type ?any]}])
               #{[:Scm2 (:enum soe)]}))
        (is (= (q :find [?type ...] :in (db) :where
                  [:ScmOwnsEnum {:enum {:spec-tacular/spec ?type}}])
               #{:Scm2}))
        (let [type :Scm2]
          (is (= (q :find :ScmOwnsEnum :in (db) :where
                    [% {:enum {:spec-tacular/spec type}}])
                 #{[soe]})))
        (is (= (q :find ?type :in (db) :where
                  [:ScmOwnsEnum {:enum {:spec-tacular/spec ?type}}])
               (db/q '[:find ?type :in $ :where
                       [?scmownsenum :spec-tacular/spec :ScmOwnsEnum]
                       [?scmownsenum :scmownsenum/enum ?tmp]
                       [?tmp :spec-tacular/spec ?type]]
                     (db))))
        (is (= (let [si (:enum soe)]
                 (q :find :ScmOwnsEnum :in (db) :where
                    [% {:enum si}]))
               (q :find :ScmOwnsEnum :in (db) :where
                  [% {:enum (:enum soe)}])
               #{[soe]}))))

    (with-test-db simple-schema
      (create! {:conn *conn*} (dog {:name "jack"}))
      (create! {:conn *conn*} (cat {:name "zuzu"}))
      (is (= (q :find :Animal :in (db) :where
                [% {:name "zuzu"}])
             #{[(cat {:name "zuzu"})]}))
      (create! {:conn *conn*} (cat {:name "jack"}))
      (is (= (q :find [:Animal ...] :in (db) :where
                [% {:name "jack"}])
             #{(cat {:name "jack"})
               (dog {:name "jack"})}))))

  (testing "bad syntax" ; fully qualify for command line
    (with-test-db simple-schema
      (is (thrown-with-msg?
           clojure.lang.ExceptionInfo #"Invalid clause rhs"
           (macroexpand '(spark.sparkspec.datomic/q :find :Scm2 :in (db)
                                                    :where [:Scm :scm2])))
          "using a (non-spec) keyword as a rhs")
      (is (thrown-with-msg?
           clojure.lang.ExceptionInfo #"could not infer type"
           (macroexpand '(spark.sparkspec.datomic/q :find ?x :in (db)
                                                    :where [?x {:y 5}])))
          "impossible to determine spec of x")
      (is (thrown-with-msg?
           clojure.lang.ExceptionInfo #"unsupported ident"
           (macroexpand '(spark.sparkspec.datomic/q :find ?x :in (db) :where
                                                    ["?x" {:y 5}])))
          "using a string as an ident")
      (is (thrown-with-msg?
           clojure.lang.ExceptionInfo #"could not find item"
           (macroexpand '(spark.sparkspec.datomic/q :find :Scm :in (db) :where
                                                    [% {:y 5}])))
          "trying to specify a field that is not in the spec")
      (is (thrown-with-msg?
           clojure.lang.ExceptionInfo #"not supported"
           (macroexpand '(spark.sparkspec.datomic/q :find ?val :in (db) :where
                                                    [:ScmEnum {:val1 ?val}])))
          "trying to pull out a field from an enum with different field types")

      (is (thrown-with-msg?
           clojure.lang.ExceptionInfo #"nil"
           (let [nil-val nil] (q :find :Scm :in (db) :where [% {:val1 nil-val}])))
          "having a runtime nil field is unacceptabbbllee")))

  (testing "bad data" ; db goes to shit after this -- should be last test
    (with-test-db simple-schema
      (let [id (get-in (create! {:conn *conn*} (scm {:val1 "baz"})) [:db-ref :eid])]
        (assert @(db/transact *conn* [[':db/add id :scm/scm2 123]]))
        (is (= id (ffirst (db/q '[:find ?scm :in $ :where [?scm :scm/scm2 123]] (db))))
            "insertion of bad scm2 ref should work")

        (is (thrown-with-msg?
             clojure.lang.ExceptionInfo #"bad entity in database"
             (:scm2 (recursive-ctor :Scm (db/entity (db) id))))
            "cant get an Scm2 out of it")

        (is (thrown-with-msg?
             clojure.lang.ExceptionInfo #"bad entity in database"
             (q :find :Scm2 :in (db) :where [:Scm {:scm2 %}]))
            "cant directly get the Scm2 either")

        (assert @(db/transact *conn*
                              [{:db/id (db/tempid :db.part/user -100)
                                :spec-tacular/spec :Scm2
                                :scm/val1 "5"}
                               [:db/add id :scm/scm2 (db/tempid :db.part/user -100)]]))

        (is (thrown-with-msg?
             clojure.lang.ExceptionInfo #"bad entity in database"
             (q :find :Scm2 :in (db) :where [:Scm {:scm2 %}])))
        
        ;; TODO: add enum tests here
        ))))

;; TODO bad syntax
#_(sd/q :find :Transfer :in db :where
        [% {:status [:TransferTransacted (-> txn :db-ref :eid)]}])
#_(ffirst (q :find :ScmM :in (db) :where [% {}]))

(deftest test-create!1
  (with-test-db simple-schema
    (let [e-soe (scmownsenum {:enums [(scm3) (scm2 {:val1 123})]})
          data (instance-transaction-data {:conn *conn*} e-soe)
          _ (is (= (count data) 6))
          _ (is (= (keys (meta data)) [:tmpid :spec]))
          _ (is (= (:spec (meta data)) (get-spec :ScmOwnsEnum)))
          a-soe (create! {:conn *conn*} e-soe)]
      (is (not (empty? (:enums a-soe))))))
  (with-test-db simple-schema
    (let [soe (create! {:conn *conn*}
                       (assoc (scmownsenum {:enum (scm3)})
                              :enum (scm2 {:val1 123})))]
      (is (= (q :find :long :in (db) :where
                [:ScmOwnsEnum {:enum [:Scm2 {:val1 %}]}])
             #{[123]})
          "can update to change an enum field from one to another")))
  (testing "many enums"
    (with-test-db simple-schema
      (let [se1 (scm2 {:val1 5})
            se2 (scm2 {:val1 120})
            se3 (scm3)
            se4 (scm2 {:val1 42})
            se5 (scm3)
            se6 (-> (scm {})
                    (merge {:scm2 (create! {:conn *conn*} (scm2 {:val1 7}))}))
            se7 (scm2 {:val1 51})
            soe (scmownsenum)

            ;; make one enum first
            e-soe (assoc soe :enums (map #(create! {:conn *conn*} %) [se1]))
            a-soe (create! {:conn *conn*} e-soe)
            _ (is (= (first (:enums a-soe)) se1))
            _ (is (= a-soe e-soe))

            ;; make all enums first
            e-soe (assoc soe :enums (map #(create! {:conn *conn*} %) [se2 se3]))
            a-soe (create! {:conn *conn*} e-soe)
            _ (is (= a-soe e-soe))

            ;; dont make any enum first
            e-soe (scmownsenum {:enums [se4]})
            a-soe (create! {:conn *conn*} e-soe)
            _ (is (= (first (:enums a-soe)) se4))
            _ (is (= a-soe e-soe))

            ;; dont make any enum first
            e-soe (scmownsenum {:enums [se5 se6]})
            a-soe (create! {:conn *conn*} e-soe)
            _ (is (= a-soe e-soe))

            ;; lazy seq
            e-soe (-> (scmownsenum {})
                      (assoc :enums (for [s [se7]] (create! {:conn *conn*} s))))
            _ (is (= (type (:enums e-soe))
                     clojure.lang.PersistentVector))
            a-soe (create! {:conn *conn*} e-soe)
            _ (is (get-in (first (:enums a-soe)) [:db-ref :eid]))])))
  (testing "errors"
    (with-test-db simple-schema
      (let [s (scm {:val1 "5"})]
        (create! {:conn *conn*} s)
        (is (thrown-with-msg?
             clojure.lang.ExceptionInfo #"entity already in database"
             (create! {:conn *conn*} s)))
        (is (thrown-with-msg?
             clojure.lang.ExceptionInfo #"entity already in database"
             (create! {:conn *conn*} (assoc s :val2 4))))))))

(deftest test-create!
  (with-test-db simple-schema
    (let [exs [(scm2 {:val1 1})
               (scm {:val1 "1" :scm2 (scm2 {:val1 1})})
               (scm {:val1 "2" :scm2 {:val1 4}})
               (scmparent {:scm (scm {:val1 "3" :scm2 {:val1 4}})})
               (scmparent {:scm {:val1 "4" :scm2 {:val1 4}}})
               (scmlink {:val1 (scmparent {:scm (scm {:val1 "5" :scm2 {:val1 4}})})})
               (scmlink {:link1 (scm {:val1 "6" :scm2 (scm2 {:val1 1})})
                         :val1  (scmparent {:scm (scm {:val1 "7.235" :scm2 {:val1 4}})})})
               (scmlink {:link2 [(scm2 {:val1 2}) (scm2 {:val1 3})]})
               (scmlink {:link1 (scm {:val1 "7" :scm2 (scm2 {:val1 1})})
                         :link2 [(scm2 {:val1 2}) (scm2 {:val1 3})]
                         :val1  (scmparent {:scm (scm {:val1 "8" :scm2 {:val1 4}})})})
               (scmlink{:val1 (scmparent{:scm (scm {:multi ["$" "J~"]  :val2 3 :val1 "K"})})
                        :link1 (scm {:scm2 (scm2 {}) :multi [] :val2 -5 :val1 "Z"})})]]
      (doseq [ex exs] (is (= (create! {:conn *conn*} ex) ex) (str ex))))))

(deftest test-update!
  (with-test-db simple-schema
    (let [exs [{:original (scm {:multi ["" "N"]})
                :updates  {:multi [""]}
                :expected (scm {:multi [""]})}
               {:original (scm {:val1 "C" :val2 -40 :multi [""] :scm2 {:val1 -7}})
                :updates {:val2 9, :multi ["" "NN"]}
                :expected (scm {:val1 "C" :val2 9 :multi ["" "NN"] :scm2 {:val1 -7}})}
               {:original (scmownsenum {:enum nil :enums nil})
                :updates {:enum (scm2 {:val1 -1})}
                :expected (scmownsenum {:enum (scm2 {:val1 -1}) :enums nil})}
               {:original (scmlink {:val1 (scmparent {:scm (scm {:val1 "!"})})})
                :updates {:val1 nil}
                :expected (scmlink {})}
               {:original (scmm {:identity " !" :vals []})
                :updates  {:identity nil}
                :expected (scmm {:vals []})}
               {:original (scmownsenum {:enums [(scm2 {:val1 42})]})
                :updates  {:enums [(scm {:val2 53})]}
                :expected (scmownsenum {:enums [(scm {:val2 53})]})}]]
      (doseq [{:keys [original updates expected] :as ex} exs]
        (let [actual (create! {:conn *conn*} original)]
          (is (= actual original)
              (str "create!\n" (with-out-str (clojure.pprint/pprint ex))))
          (let [actual (update! {:conn *conn*} actual updates)]
            (is (= actual expected)
                (str "update!\n" (with-out-str (clojure.pprint/pprint ex))))))))))

(deftest test-link
  (with-test-db simple-schema
    (let [;; set up a ScmLink
          sl (scmlink {:link1 (scm {:val1 "1" :scm2 (scm2 {:val1 1})})
                       :link2 [(scm2 {:val1 2}) (scm2 {:val1 3})]
                       :val1 (scmparent {:scm (scm {:val1 "2" :scm2 {:val1 4}})})})
          sl-db (create! {:conn *conn*} sl)
          refresh-sl (fn [] (refresh {:conn *conn*} sl-db))
          ;; sl-db (refresh-sl)

          ;; set up an ScmParent
          scmp (scmparent {:scm (scm {:val1 "3" :scm2 {:val1 5}})})
          scmp-db (create! {:conn *conn*} scmp)
          _ (is (thrown-with-msg? clojure.lang.ExceptionInfo #"already in database"
                                  (assoc! {:conn *conn*} sl-db :val1 scmp-db))
                "adding another scm with the same identity errors -- tried to copy")

          ;; link a new Scm into :link1 -- should be passed by ref
          s (scm {:val1 "5" :scm2 {:val1 5}})
          s-db (create! {:conn *conn*} s)
          s-db-ref (:db-ref s-db)
          ;; s-db (get-by-eid (db) s-eid)
          sl-db (assoc! {:conn *conn*} sl-db :link1 s-db)
          _ (is (= (:db-ref (:link1 sl-db))
                   s-db-ref))
          _ (is (= (:db-ref (:link1 sl-db))
                   (:db-ref s-db)))

          ;; changing the Scm also changes the Scm in :link1
          _ (assoc! {:conn *conn*} s-db :val1 "6")
          sl-db (refresh-sl)
          _ (is (= (:val1 (:link1 sl-db)) "6"))

          ;; assoc!ing an absurd field should throw an error
          _ (is (thrown-with-msg? clojure.lang.ExceptionInfo #"keys not in the spec"
                                  (assoc! {:conn *conn*} s-db :blah 5)))])))

(defn- unique-db-refs [inst]
  (let [objs (atom (list))
        add-if-unique!
        ,(fn [x]
           (assert (not (instance? datomic.query.EntityMap x)))
           (when (get-spec x)
             (when-not (some #(= (get-in x [:db-ref :eid])
                                 (get-in % [:db-ref :eid]))
                             @objs)
               (swap! objs conj x))))]
    (do (walk/prewalk #(do (add-if-unique! %) %) inst)
        @objs)))

(defn- unique-objs [inst]
  (let [objs (atom (list))
        add-if-unique!
        ,(fn [x]
           (when (get-spec x)
             (when-not (some #(identical? % x) @objs)
               (swap! objs conj x))))]
    (do (walk/prewalk #(do (add-if-unique! %) %) inst)
        @objs)))

(defn- total-objs [inst]
  (let [objs (atom (list))
        add-if-unique!
        ,(fn [x]
           (when (get-spec x)
             (swap! objs conj x)))]
    (do (walk/prewalk #(do (add-if-unique! %) %) inst)
        (count @objs))))

(deftest test-create-graph!
  (testing "unique-objs"
    (let [e1 (scmmwrap {:val (scmm {:identity "G__195328)" :val (scm2 {:val1 1})})})]
      (is (= (unique-objs [e1 e1])
             (unique-objs [e1]))))
    (let [e2 (scmm {:val (scm3 {})})]
      (is (= (count (unique-objs (scmmwrap {:name "eg%Wnva" :val e2})))
             (count (unique-objs (scmmwrap {:val e2}))))))
    (let [e1 (scmmwrap #_"i_ScmMWrap@8628e9e2" {})
          e2 (scmmwrap #_"i_ScmMWrap@f4df349a" {:name "ve?|xZP,c!dkj[S["})
          e3 (scm2 #_"i_Scm2@4a7309ea" {:val1 4})
          e4 (scmm #_"i_ScmM@9e816645"
                   {:val e3,
                    :identity "G__135311acmw>a&-"})
          e5 (scmm #_"i_ScmM@c0fd11a2" {:val e3})
          e6 (scmm #_"i_ScmM@873018c8" {})
          e7 (scmmwrap #_"i_ScmMWrap@6006f507" {:name "esmkN&D}[.7"})
          e8 (scmmwrap #_"i_ScmMWrap@eba78425" {:val e4 :name ""})
          e9 (scmmwrap #_"i_ScmMWrap@538f19a"  {:val e6})]
      (= (count (unique-objs
                 [e1 e7 e1 e2 e1 e1 e8 e7 e9 e2
                  (scmmwrap #_"i_ScmMWrap@8628e9a4" {:name "B"})
                  (scmmwrap #_"i_ScmMWrap@a685f2a5" {:val e5 :name "oHRXw@wYjc"})
                  (scmmwrap #_"i_ScmMWrap@456bfa84" {:val e5})
                  e9 e1 e8]))
         12)))

  (testing "unique-db-refs"
    (is (= (-> [(scmmwrap {:db-ref {:eid 1}})
                (scmmwrap {:db-ref {:eid 2}})
                (scmmwrap {:db-ref {:eid 3}
                           :name "7+6"
                           :val (scmm {:db-ref {:eid 4},
                                       :val
                                       (scm {:db-ref {:eid 5},
                                             :val1 "G__92146",
                                             :val2 -1,
                                             :scm2 (scm2 {:db-ref {:eid 6}})}),
                                       :vals
                                       [(scm2 {:db-ref {:eid 6}})
                                        (scm2 {:db-ref {:eid 7} :val1 0})]})})]
               unique-db-refs count)
           7))
    (let [e1 (scmmwrap {})
          expected [(scmmwrap {:val (scmm {:val (scm {:scm2 (scm2 {})})})})
                    (scmmwrap {:val (scmm {:val (scm3 {})})})
                    e1
                    (scmmwrap {})
                    (scmmwrap {})
                    e1]]
      (with-test-db simple-schema
        (is (= (-> (create-graph! {:conn *conn*} expected)
                   unique-db-refs count)
               (-> [(scmmwrap {:db-ref {:eid 1}
                               :val (scmm {:db-ref {:eid 2}
                                           :val (scm {:db-ref {:eid 3}
                                                      :scm2 (scm2 {:db-ref {:eid 4}})})})})
                    (scmmwrap {:db-ref {:eid 5}
                               :val (scmm {:db-ref {:eid 6}
                                           :val (scm3 {:db-ref {:eid 7}})})})
                    (scmmwrap {:db-ref {:eid 8}})
                    (scmmwrap {:db-ref {:eid 9}})
                    (scmmwrap {:db-ref {:eid 10}})
                    (scmmwrap {:db-ref {:eid 8}})]
                   unique-db-refs count)
               10)))))
  
  (let [exs [(let [e (scm2 {:val1 1})]
               {:expected [e e]})
             (let [e1 (scmmwrap {:val (scmm {:identity "G__195328)"
                                             :val (scm2 {:val1 1})})})
                   e2 (scmm {:val (scm3 {})})]
               {:expected [e1
                           e1
                           (scmmwrap {:name "eg%Wnva" :val e2})
                           (scmmwrap {:val e2})]})

             (let [e0 (scm {:multi [""] :val2 -5})
                   e1 (scmmwrap {})
                   e2 (scmmwrap {:val (scmm {:val e0 :identity "G__57958jHwJNU"})})
                   e3 (scm2 {})]
               {:expected
                [(scmmwrap {:name "d"})
                 e2
                 (scmmwrap {:val (scmm {})})
                 e2 e1 e1
                 (scmmwrap {:val
                            (scmm {:vals [e3]
                                   :val e0
                                   :identity "G__57885?-*k@xO"})
                            :name ""})
                 e1
                 (scmmwrap {:val (scmm {:val e3 :identity "G__57884e"})})]})
             (let [e1 (scmmwrap {})]
               {:expected
                [(scmmwrap {:val (scmm {:val (scm {:scm2 (scm2 {})})})})
                 (scmmwrap {:val (scmm {:val (scm3 {})})})
                 e1
                 (scmmwrap {})
                 (scmmwrap {})
                 e1]})
             #_(let [e0 (scmparent {})
                     e3 (scm2 {})
                     e1 (scmlink {:val1 e0
                                  :link2 [e3]
                                  :link1 (scm {:multi ["l\"[" "\"pCS"],
                                               :val1 "G__34694"})})
                     e2 (scmlink {:val1 e0,:link2 [e3]})]
                 {:expected [(scmlink {}) e1 e2 e1 e2]})]]
    (doseq [{:keys [expected]} exs]
      (with-test-db simple-schema
        (let [conn-ctx {:conn *conn*}
              actual (create-graph! conn-ctx (seq expected))
              urefs  (unique-db-refs actual)
              uobjs  (unique-objs expected)]
          (is (= {:count (count urefs) :entity actual}
                 {:count (count uobjs) :entity expected})))))))

(deftest test-calendar-day
  (with-test-db simple-schema
    (let [bday (create! {:conn *conn*} (birthday {:date (time/date-time 2015 7 24)}))]
      (is (= (:date bday) (time/date-time 2015 7 24)))
      (is (= (q :find ?date :in (db) :where
                [:Birthday {:date ?date}])
             #{[(time/date-time 2015 7 24)]})))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; random testing

(defn check-create! [conn-ctx original]
  (let [actual (create! conn-ctx original)]
    (if (= actual original) actual
        (throw (ex-info "creation mismatch: output doesn't reflect input" 
                        {:actual actual :expected original})))))

(defn check-update!
  "update-subset is a subset of key-vals from original-db-value to try updating.
  returns {:ok true} or {:error {reasons}}"
  [conn-ctx original updates]
  (let [expected (merge original updates)
        actual   (update! conn-ctx original updates)]
    (if (= expected actual) actual
        (throw (ex-info "update mismatch, output is not equivalent to input"
                        {:original original :updates updates :actual actual})))))

(defn prop-check-components
  "property for verifying that check-component!, create!, and update! work correctly"
  [spec-key]
  (with-test-db simple-schema
    (let [spec     (get-spec spec-key)
          fields   (map :name (:items spec))
          conn-ctx {:conn *conn*}]
      (prop/for-all [{:keys [original updates]} (instance-generator spec)]
        (and (every? #(check-component! spec % (get original %)) fields)
             (when-let [created (check-create! conn-ctx original)]
               (or (= created :skip) (check-update! conn-ctx created updates))))))))

(ct/defspec gen-Scm2 10 (prop-check-components :Scm2))
(ct/defspec gen-ScmOwnsEnum 10 (prop-check-components :ScmOwnsEnum))
(ct/defspec gen-ScmM 10 (prop-check-components :ScmM))
(ct/defspec gen-ScmParent 10 (prop-check-components :ScmParent))
(ct/defspec gen-ScmMWrap 10 (prop-check-components :ScmMWrap))
(ct/defspec gen-Scm 20 (prop-check-components :Scm))
(ct/defspec gen-ScmLink 50 (prop-check-components :ScmLink))

(defn prop-create-graph [spec-key]
  (let [spec (get-spec spec-key)
        has-repeats? #(> (total-objs %) (count (unique-objs %)))
        gen (graph-generator spec)]
    (prop/for-all [{:keys [expected]} (gen/such-that has-repeats? gen 20)]
      (with-test-db simple-schema
        (let [conn-ctx {:conn *conn*}
              actual (create-graph! conn-ctx expected)]
          (is (= {:count (count (unique-db-refs actual)) :entity actual}
                 {:count (count (unique-objs expected)) :entity expected})))))))

(ct/defspec graph-ScmOwnsEnum 10 (prop-create-graph :ScmOwnsEnum))
(ct/defspec graph-ScmM 10 (prop-create-graph :ScmM))
(ct/defspec graph-ScmMWrap 20 (prop-create-graph :ScmMWrap))

#_(let [se (scm {:val1 "abc"})] ;; TODO test
    (clojure.pprint/pprint
     (macroexpand
      '(q :find [:ScmOwnsEnum ...] :in db :where [% {:enum se}]))))

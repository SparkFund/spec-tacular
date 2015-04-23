(ns spark.sparkspec.datomic-test
  (:refer-clojure :exclude [remove])
  (:use clojure.test
        spark.sparkspec
        spark.sparkspec.spec
        spark.sparkspec.datomic
        spark.sparkspec.test-utils
        spark.sparkspec.test-specs)
  (:require [datomic.api :as db]
            [spark.sparkspec.datomic :as sd]
            [clojure.core.typed :as t]
            [clojure.tools.macro :as m]))

;;;; check-schema tests

(def schema
  [{:db/id 123124
    :db/ident :spec/var1
    :db/unique :db.unique/identity
    :db/valueType :db.type/string}
   datomic-spec-schema])

(def schema-by-value
  [{:db/id 123124
    :db/ident :spec/var1
    :db/unique :db.unique/value
    :db/valueType :db.type/string}
   datomic-spec-schema])

(def schema-alt-keys
  [{:db/id 123124
    :db/ident :spec/var2
    :db/valueType :db.type/string}
   datomic-spec-schema])

(def matching-spec
  (map->Spec
   {:name 'spec
    :items
    (map map->Item
         [{:name :var1 :type [:one String] :identity? true :unique? true}])}))

(def not-matching-spec
  (map->Spec
   {:name 'spec
    :items
    (map map->Item
         [{:name :var1 :type [:one String]}])}))

(deftest matching-spec-test
  (is (empty? (check-schema schema matching-spec))))

(deftest not-matching-spec-test
  (is (= '("uniqueness for field :var1 in spec is inconsistant")
         (check-schema schema not-matching-spec))))

(deftest value-consistent-test
  (is (= '("uniqueness for field :var1 in spec is inconsistant")
         (check-schema schema-by-value matching-spec))))

(deftest value-consistent-test-2
  (is (= '("inconsistent keys between schema and spec. Diff: [#{:var2} #{:var1} nil]")
         (check-schema schema-alt-keys matching-spec))))


;;;; sp->transactions tests

;; TODO: make datomic utilities from these

(def simple-schema
  [{:db/id (db/tempid :db.part/db)
    :db/ident :scm/val1
    :db/unique :db.unique/identity
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one
    :db/doc ""
    :db.install/_attribute :db.part/db}
   {:db/id (db/tempid :db.part/db)
    :db/ident :scm/val2
    :db/valueType :db.type/long
    :db/cardinality :db.cardinality/one
    :db/doc ""
    :db.install/_attribute :db.part/db}
   {:db/id (db/tempid :db.part/db)
    :db/ident :scm/scm2
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/one
    :db/doc ""
    :db.install/_attribute :db.part/db}
   {:db/id (db/tempid :db.part/db)
    :db/ident :scm/multi
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/many
    :db/doc ""
    :db.install/_attribute :db.part/db}
   {:db/id (db/tempid :db.part/db)
    :db/ident :scm2/val1
    :db/valueType :db.type/long
    :db/cardinality :db.cardinality/one
    :db/doc ""
    :db.install/_attribute :db.part/db}
   {:db/id (db/tempid :db.part/db)
    :db/ident :scmm/identity
    :db/unique :db.unique/identity
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one
    :db/doc ""
    :db.install/_attribute :db.part/db}
   {:db/id (db/tempid :db.part/db)
    :db/ident :scmownsenum/enum
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/one
    :db/doc ""
    :db.install/_attribute :db.part/db}
   {:db/id (db/tempid :db.part/db)
    :db/ident :scmownsenum/enums
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/many
    :db/doc ""
    :db.install/_attribute :db.part/db}
   {:db/id (db/tempid :db.part/db)
    :db/ident :scmm/vals
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/many
    :db/doc ""
    :db.install/_attribute :db.part/db}
   {:db/id (db/tempid :db.part/db)
    :db/ident :scmparent/scm
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/one
    :db/doc ""
    :db.install/_attribute :db.part/db}
   datomic-spec-schema])

(def scm-1 (scm {:val1 "hi" :val2 323 :scm2 (scm2 {:val1 125})}))
(def scm-non-nested (scm {:val1 "ho" :val2 56666}))

(deftest new-transaction-tests
  (let [tx-info (with-test-db simple-schema
                  @(db/transact *conn* (sp->transactions (db) scm-1)))]
    (is (not (= (:db-before tx-info) (:db-after tx-info))))
    (is (every? :added (:tx-data tx-info))))
  (with-test-db simple-schema
    (let [tx1 (sp->transactions (db) scm-non-nested)]
      (is (= 1 (count tx1)))
      (is (thrown? java.lang.AssertionError
                   (sp->transactions (db) (scm {:extra-key 1})))))))

(deftest update-transaction-tests
  (with-test-db simple-schema
    (let [tx1 (sp->transactions (db) scm-1)
          tx-info1 @(db/transact *conn* tx1)
          tx2 (sp->transactions (db) (assoc scm-1 :val2 555))
          tx-info2 @(db/transact *conn* tx2)]
      (is (= (ffirst (db/q '[:find ?val2
                             :where [?eid :scm/val1 "hi"] [?eid :scm/val2 ?val2]]
                           (db)))
             555)))))

(deftest db->sp-tests
  (with-test-db simple-schema
    @(db/transact *conn* (sp->transactions (db) scm-1))
    (let [query (db/q '[:find ?eid :where [?eid :scm/val1 "hi"]] (db))
          e (db/entity (db) (ffirst query))]
      (is (scm? (db->sp (db) e :Scm))))))

(deftest commit-sp-transactions-tests
  (with-test-db simple-schema
    (let [tx1 (sp->transactions (db) scm-1)
          eid (commit-sp-transactions {:conn *conn*} tx1)]
      (is (scm? (db->sp (db) (db/entity (db) eid) :Scm))
          "The eid we get back from commit-sp-transactions should be
          tied to what we put in."))))

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
  (->> 
   eids
   (map #(recursive-expand (db/entity (db) (first %))))
   (into #{})
   doall))

(deftest nested-is-many
  (testing "can create an object with several is-many children."
    (is (= #{{:scmm/vals #{{:scm2/val1 3
                            :spec-tacular/spec :Scm2} 
                           {:scm2/val1 4
                            :spec-tacular/spec :Scm2}}
              :spec-tacular/spec :ScmM}}
           (with-test-db simple-schema
             (let [txs (sp->transactions
                        (db)
                        (recursive-ctor :ScmM {:vals [{:val1 3} {:val1 4}]}))
                   _ @(db/transact *conn* txs)
                   res (db/q '[:find ?eid
                               :where
                               [?eid :scmm/vals ?es]
                               [?es  :scm2/val1 ?v1]] (db))]
               (inspect-eids res)))))))

(deftest several-nested
  (testing "can create multiple distinct objects."
    (is (= #{{:scmm/vals #{{:scm2/val1 1, :spec-tacular/spec :Scm2}},
              :spec-tacular/spec :ScmM}
             {:scmm/vals #{{:scm2/val1 2, :spec-tacular/spec :Scm2}},
              :spec-tacular/spec :ScmM}}
           (with-test-db simple-schema
             (let [txs (concat
                        (sp->transactions
                         (db)
                         (recursive-ctor :ScmM {:vals [{:val1 1}]}))
                        (sp->transactions
                         (db)
                         (recursive-ctor :ScmM {:vals [{:val1 2}]})))
                   _ @(db/transact *conn* txs)
                   res (db/q '[:find ?eid
                               :where
                               [?eid :scmm/vals ?es]
                               [?es  :scm2/val1 ?v1]] (db))]
               (inspect-eids res)))))))

(deftest identity-add-one
  (testing "setting an is-many valued object updates the set to exactly the new set"
    (is (= #{{:scmm/identity "myident",
              :scmm/vals #{{:scm2/val1 2
                            :spec-tacular/spec :Scm2}}
              :spec-tacular/spec :ScmM}}
           (with-test-db simple-schema
             (let [tx1 (sp->transactions
                        (db)
                        (scmm {:identity "myident" :vals [{:val1 1}]}))
                   _ @(db/transact *conn* tx1)
                   tx2 (sp->transactions
                        (db)
                        (scmm {:identity "myident" :vals [{:val1 2}]}))
                   _ @(db/transact *conn* tx2)
                   res (db/q '[:find ?eid
                               :where
                               [?eid :scmm/identity _]] (db))]
               (inspect-eids res)))))))

(deftest identity-edit-one-is-many
  (testing
      "fixing a unique attribute on parent AND is-many child will
      upsert that item, and not add a new one."
    (is (= #{{:scmm/identity "myident",
              :scmm/vals #{{:scm2/val1 2
                            :spec-tacular/spec :Scm2}}
              :spec-tacular/spec :ScmM}}
           (with-test-db simple-schema
             (let [tx1 (sp->transactions
                        (db)
                        (recursive-ctor :ScmM {:identity "myident"
                                               :vals [{:val1 1}]})) ;initially 1
                   _ @(db/transact *conn* tx1)
                   child-eid (ffirst (db/q '[:find ?child
                                             :where
                                             [?eid :scmm/vals ?child]] (db))) ; remember the item we added
                   tx2 (sp->transactions
                        (db)
                        (recursive-ctor :ScmM {:identity "myident"
                                               :vals [{:db-ref {:eid child-eid}
                                                       :val1 2}]})) ; setting to 2 (fixing via child-eid)
                   _ @(db/transact *conn* tx2)
                   res (db/q '[:find ?eid
                               :where
                               [?eid :scmm/vals ?_]] (db))]
               (inspect-eids res)))))))

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
      "only eids on a 'true' mask, nested."))

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
          _ (update-sp! {:conn *conn*}
                        b3db
                        (assoc b3db :vals nil))
          b3db (get-by-eid (db) b2eid)
          _ (is (= [] (:vals b3db))
                "Can delete non-primitive is-manys via nil too")
          ])))

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
          eid (commit-sp-transactions {:conn *conn*} tx1)
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
          eid (commit-sp-transactions {:conn *conn*} tx1)
          sp (db->sp (db) (db/entity (db) eid))
          tx2 (sp->transactions (db) (assoc sp :enums [(scm2 {:val1 1})]))
          eid2 (commit-sp-transactions {:conn *conn*} tx2)
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
          _ (is (= 2 (count (get-all-of-type (db) (get-spec :Scm)))) "now have 2 logs")
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
  (with-test-db simple-schema
    (is (= #{} (->> (q :find ?a :in (db) :where
                       [:ScmParent {:scm {:val2 ?a}}])))
        "nothing returned on fresh db.")

    (let [a1 (scmparent {:scm (scm {:val1 "a" :val2 1})})
          a2 (scmparent {:scm (scm {:val1 "b" :val2 2})})]
      (create-sp! {:conn *conn*} a1)
      (create-sp! {:conn *conn*} a2))

    (testing "primitive data"
      (is (= #{[1] [2]}
             (q :find [?a] :in (db) :where
                [:ScmParent {:scm {:val2 ?a}}]))
          "simple one-attribute returns (a ?-prefixed symbol isn't needed- just idiomatic cf datomic)")
      (is (= #{[1 "a"] [2 "b"]}
             (q :find [?a ?b] :in (db) :where
                [:ScmParent {:scm {:val1 ?b :val2 ?a}}]))
          "multiple attribute returns")
      (is (= #{[1]}
             (q :find [?a] :in (db) :where
                [:ScmParent {:scm {:val1 "a" :val2 ?a}}]))
          "can use literals in the pattern to fix values")
      (is (= #{["b"]}
             (let [two 2]
               (q :find [?a] :in (db) :where 
                  [:ScmParent {:scm {:val1 ?a :val2 two}}])))
          "can use regular variables to fix values")
      (is (= #{["b"]}
             (q :find [?a] :in (db) :where
                [:ScmParent {:scm {:val1 ?a :val2 (let [?a 2] ?a)}}]))
          "return variables respect lexical scope and don't clobber lets")
      (is (= #{["b"]}
             (q :find [?a] :in (db) :where
                [:ScmParent {:scm {:val1 ?a :val2 ((fn [?a] ?a) 2)}}]))
          "return variables respect lexical scope and don't clobber fns"))

    (testing "compound data"
      (let [e-scm2 (scm2 {:val1 5})
            e-scm  (scm {:val2 5 :scm2 e-scm2})
            e-scmp (scmparent {:scm e-scm})]
        (create-sp! {:conn *conn*} e-scmp)

        (let [a-scm2 (->> (q :find :Scm2 :in (db) :where
                             [:Scm {:scm2 %}])
                          first)]
          (is (= (:val1 a-scm2) 5)
              "can use keywords on returned entities"))
        
        (let [[a-scm a-scm2]
              ,(->> (q :find [:Scm :Scm2] :in (db) :where
                       [%1 {:scm2 %2}])
                    first)]
          (testing "equality on returned entities"
            (is (= (:scm2 a-scm) a-scm2))
            (is (= a-scm2 e-scm2))
            (is (= a-scm  e-scm))))

        (let [ex-scm (first (q :find :Scm :in (db) :where [% {:scm2 :Scm2}]))]
          (time (dorun (for [x (range 100000)] (:scm2 ex-scm)))))))

    (testing "bad syntax" ; fully qualify for command line
      (is (thrown-with-msg?
           clojure.lang.ExceptionInfo #"invalid map"
           (->> '(spark.sparkspec.datomic/q :find :Scm2 :in (db) :where [:Scm :scm2])
                clojure.core/macroexpand prn)))
      (is (thrown-with-msg?
           clojure.lang.ExceptionInfo #"could not infer type"
           (->> '(spark.sparkspec.datomic/q :find ?x :in (db) :where [?x {:y 5}])
                clojure.core/macroexpand prn)))
      (is (thrown-with-msg?
           clojure.lang.ExceptionInfo #"unsupported ident"
           (->> '(spark.sparkspec.datomic/q :find ?x :in (db) :where ["?x" {:y 5}])
                clojure.core/macroexpand prn)))
      (is (thrown-with-msg?
           clojure.lang.ExceptionInfo #"could not find sub-spec"
           (->> '(spark.sparkspec.datomic/q :find :Scm :in (db) :where [% {:y 5}])
                clojure.core/macroexpand prn))))

    (testing "bad data" ; db goes to shit after this -- should be last test
      (let [id (ffirst (db/q '[:find ?scm :in $ :where [?scm :scm/val2 5]] (db)))]
        (assert @(db/transact *conn* [[':db/add id :scm/scm2 123]]))
        (is (= id (ffirst (db/q '[:find ?scm :in $ :where [?scm :scm/scm2 123]] (db))))
            "insertion of bad scm2 ref should work")

        (let [data (try (q :find :Scm2 :in (db) :where [:Scm {:scm2 %}])
                        (catch clojure.lang.ExceptionInfo e (ex-data e)))]
          (is (= :Scm2 (:expected-spec data))
              "should be an error to use bad scm2 ref as an :Scm2"))

        (assert @(db/transact *conn*
                              [{:db/id (db/tempid :db.part/user -100)
                                :spec-tacular/spec :Scm2
                                :scm/val1 "5"}
                               [:db/add id :scm/scm2 (db/tempid :db.part/user -100)]]))
        
        (let [data (try (q :find :Scm2 :in (db) :where [:Scm {:scm2 %}])
                        (catch clojure.lang.ExceptionInfo e (ex-data e)))]
          (is (= [:spec-tacular/spec :scm/val1] (:actual-keys data))
              "should be an error to have an :Scm2 with :scm/val1 key"))))))

(deftest type-tests
  (with-out-str 
    (do (t/check-ns 'spark.sparkspec.datomic)
        (t/check-ns 'spark.sparkspec.datomic-test :collect-only true)
        (t/check-ns 'spark.sparkspec.test-specs :collect-only true)))
  (testing "types" ; fully qualify for command line
    (t/cf (spark.sparkspec.datomic/q :find [:Scm :Scm2] 
                                     :in (spark.sparkspec.datomic-test/db)
                                     :where [%1 {:scm2 %2}])
          (clojure.core.typed/Set 
           (clojure.core.typed/HVec 
            [spark.sparkspec.test-specs/Scm
             spark.sparkspec.test-specs/Scm2])))))


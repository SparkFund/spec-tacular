(ns spark.sparkspec.datomic-test
  (:use clojure.test
        spark.sparkspec
        spark.sparkspec.spec
        spark.sparkspec.datomic
        spark.test-utils)
  (:require [datomic.api :as db]))



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
    :db/ident :scmm/vals
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/many
    :db/doc ""
    :db.install/_attribute :db.part/db}
   datomic-spec-schema])

(defspec Scm
  [val1 :is-a :string :unique :identity]
  [val2 :is-a :long]
  [multi :is-many :string]
  [scm2 :is-a :Scm2])

(defspec Scm2
  [val1 :is-a :long :unique :identity])

(defspec ScmM
  [identity :is-a :string :unique :identity]
  [vals :is-many :Scm2])

(def scm-1 (scm {:val1 "hi" :val2 323 :scm2 (scm2 {:val1 125})}))
(def scm-non-nested (scm {:val1 "ho" :val2 56666}))

(deftest new-transaction-tests
  (let [tx-info (with-test-db simple-schema
                  @(db/transact *conn* (sp->transactions (db) scm-1)))]
    (is (not (= (:db-before tx-info) (:db-after tx-info))))
    (is (every? :added (:tx-data tx-info))))
  (with-test-db simple-schema
    (let [tx1 (sp->transactions (db) scm-non-nested)]
      (is (= 1 (count tx1))))))

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
          eid (commit-sp-transactions *conn* tx1)]
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
      (let [scm-a (scmm {:val1 "name" :val2 123124 :multi ["hi" "ho"]})
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

; TODO test ":ref" types adding/enums , eg :user/role

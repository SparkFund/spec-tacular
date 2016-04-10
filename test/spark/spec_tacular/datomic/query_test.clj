(ns spark.spec-tacular.datomic.query-test
  "tests sd/q and sd/query"
  (:refer-clojure :exclude [assoc!])
  (:require [clojure.test :refer :all]
            [datomic.api :as d]
            [spark.spec-tacular :refer [refless= recursive-ctor]]
            [spark.spec-tacular.datomic :refer :all :exclude [db]]
            [spark.spec-tacular.datomic.query-helpers :refer :all]
            [spark.spec-tacular.schema :as schema]
            [spark.spec-tacular.test-specs :refer :all]
            [spark.spec-tacular.test-utils :refer [with-test-db *conn* db]]))

(def simple-schema
  (cons schema/spec-tacular-map
        (schema/from-namespace (the-ns 'spark.spec-tacular.test-specs))))

(deftest test-query
  (with-test-db simple-schema
    (is (= (query
            {:find (list '?a),
             :where
             (list ['?scmparent {:spec-tacular/spec :ScmParent, :scm '?scm}]
                   ['?scm {:spec-tacular/spec :Scm, :val2 '?a}]),
             :in (cons '$ (list))}
            (db))
           #{}))
    (is (= (query {:find (list (list 'instance :Animal '?animal))
                   :in (cons '$ (list))
                   :where (list ['?animal {:spec-tacular/spec :Animal, :name "zuzu"}])}
                  (db))
           #{}))))

(deftest test-query-pull
  (with-test-db simple-schema
    (let [ex (create! {:conn *conn*} (scm {:val1 "123" :val2 123}))]
      (is (= (query {:find '((spec-pull ?scm :Scm [:val1]) .)
                     :in '($)
                     :where '([?scm {:spec-tacular/spec :Scm
                                     :val2 ?val2}]
                              [(- ?val2 5) ?long])}
                    (db))
             {:val1 "123"}))
      (is (= (query {:find '((pull ?scm [:scm/val1]) .)
                     :in '($)
                     :where '([?scm {:spec-tacular/spec :Scm
                                     :val2 ?val2}]
                              [(- ?val2 5) ?long])}
                    (db))
             {:scm/val1 "123"})))))

(defn- ex-aggregate [x] 5)

(deftest test-query-aggregation
  (with-test-db simple-schema
    (let [ex (create! {:conn *conn*} (scm {:val1 "123" :val2 123}))
          ey (create! {:conn *conn*} (scm {:val1 "456" :val2 456}))]
      (is (= (query {:find '((min ?long))
                     :in '($)
                     :where '([?scm {:spec-tacular/spec :Scm
                                     :val2 ?val2}]
                              [(- ?val2 5) ?long])}
                    (db))
             #{[118]}))))
  (with-test-db simple-schema
    (let [ex (create! {:conn *conn*} (scm {:val1 "123" :val2 123}))
          ey (create! {:conn *conn*} (scm {:val1 "456" :val2 456}))]
      (is (= (query {:find '((spark.spec-tacular.datomic.query-test/ex-aggregate ?long) .)
                     :in '($)
                     :where '([?scm {:spec-tacular/spec :Scm
                                     :val2 ?val2}]
                              [(- ?val2 5) ?long])}
                    (db))
             5))
      (is (= (query {:find '((spark.spec-tacular.datomic.query-test/ex-aggregate ?long) .)
                     :in '($)
                     :where '([?scm {:spec-tacular/spec :Scm
                                     :val2 ?val2}]
                              [(- ?val2 5) ?long])}
                    (db))
             5)))))

(deftest test-query-union
  (with-test-db simple-schema
    (let [f1 (create! {:conn *conn*} (ferret {:name "catsnake"}))
          m1 (create! {:conn *conn*} (mouse {:name "zuzu"}))
          _ (is (query {:find '([?animal ...])
                        :in '($)
                        :where '([?animal {:spec-tacular/spec :Animal
                                           :name "zuzu"}])}
                       (db))
                #{[(mouse {:name "zuzu"})]})
          m2 (create! {:conn *conn*} (mouse {:name "catsnake"}))
          _ (is (= (q :find [:Animal ...] :in (db) :where
                      [% {:name "catsnake"}])
                   #{m2 f1}))])))

(deftest test-q-primitive-data
  (with-test-db simple-schema
    (is (= #{} (q :find ?a :in (db) :where [:ScmParent {:scm {:val2 ?a}}]))
        "nothing returned on fresh db.")

    (let [a1 (scmparent {:scm (scm {:val1 "a" :val2 1})})
          a2 (scmparent {:scm (scm {:val1 "b" :val2 2})})]
      (create! {:conn *conn*} a1)
      (create! {:conn *conn*} a2))
    
    (is (= (q :find ?a :in (db) :where
              [:ScmParent {:scm {:val2 ?a}}])
           #{[1] [2]})
        "simple one-attribute returns (a ?-prefixed symbol isn't needed- just idiomatic cf datomic)")
    (is (= (q :find ?a ?b :in (db) :where
              [:ScmParent {:scm {:val1 ?b :val2 ?a}}])
           #{[1 "a"] [2 "b"]})
        "multiple attribute returns")
    (is (contains? #{[1 "a"] [2 "b"]}
                   (q :find [?a ?b] :in (db) :where
                      [:ScmParent {:scm {:val1 ?b :val2 ?a}}]))
        "tuple")
    (is (contains? #{1 2}
                   (q :find ?a . :in (db) :where
                      [:ScmParent {:scm {:val2 ?a}}]))
        "scalar")
    (is (= (q :find ?a :in (db) :where
              [:ScmParent {:scm {:val1 "a" :val2 ?a}}])
           #{[1]})
        "can use literals in the pattern to fix values")
    (is (= (let [two 2]
             (q :find ?a :in (db) :where 
                [:ScmParent {:scm {:val1 ?a :val2 two}}]))
           #{["b"]})
        "can use regular variables to fix values")
    (is (= (q :find ?a :in (db) :where
              [:ScmParent {:scm {:val1 ?a :val2 (let [?a 2] ?a)}}])
           #{["b"]})
        "return variables respect lexical scope and don't clobber lets")
    (is (= (q :find ?a :in (db) :where
              [:ScmParent {:scm {:val1 ?a :val2 ((fn [?a] ?a) 2)}}])
           #{["b"]})
        "return variables respect lexical scope and don't clobber fns"))
  (with-test-db simple-schema
    (let [ex (create! {:conn *conn*} (scmkw {:item :test}))]
      (is (= (q :find :ScmKw :in (db) :where
                [% {:item :test}])
             #{[ex]}))
      (is (= (q :find :keyword :in (db) :where
                [:ScmKw {:item [:keyword %]}])
             #{[:test]})))))

(deftest test-q-compound-data
  (with-test-db simple-schema
    (let [e-scm2 (create! {:conn *conn*} (scm2 {:val1 5}))
          e-scm  (create! {:conn *conn*} (scm {:val2 5 :scm2 e-scm2}))
          e-scmp (create! {:conn *conn*} (scmparent {:scm e-scm}))]
      (testing "ref back"
        (= (q :find ?scm2 . :in (db) :where
              [?scm [:Scm {:scm2 ?scm2}]]
              [?scm :scm/scm2 ?scm2])
           (q :find ?scm2 . :in (db) :where
              [_ [:Scm {:scm2 ?scm2}]])
           (get-in e-scm2 [:db-ref :eid])))
      (testing "simple instance back"
        (let [a-scm2 (->> (q :find :Scm2 :in (db) :where
                             [:Scm {:scm2 %}])
                          ffirst)]
          (is (= (:val1 a-scm2) 5)
              "can use keywords on returned entities")
          (is (= (select-keys a-scm2 [:db-ref])
                 (select-keys e-scm2 [:db-ref]))
              "allow :db-ref keyword access")))
      (let [a-scm (ffirst (q :find :Scm :in (db) :where [% {:scm2 [:Scm2 {:val1 5}]}]))]
        (testing "equality on returned entities"
          (is (refless= a-scm e-scm))
          (is (refless= e-scm a-scm))))
      
      (testing "equality on returned sub-entities"
        (let [[a-scm a-scm2]
              ,(->> (q :find :Scm :Scm2 :in (db) :where
                       [%1 {:scm2 %2}])
                    first)]
          (is (= (:scm2 a-scm) a-scm2))
          (is (refless= a-scm2 e-scm2))
          (is (refless= a-scm  e-scm))
          (is (not (:val1 a-scm)))
          (is (map? (:db-ref (:scm2 a-scm)))
              "allow :db-ref keyword access on sub-entities")))))

  (with-test-db simple-schema
    (testing "is-many"
      (let [e-scmm (scmm {:identity "hi" :vals [(scm2 {:val1 42}) (scm2 {:val1 7})]})
            scmm-eid (create! {:conn *conn*} e-scmm)
            a-scmm1 (ffirst (q :find :ScmM :in (db) :where [% {:identity "hi"}]))
            a-scmm2 (recursive-ctor :ScmM (d/entity (db) (get-eid (db) scmm-eid)))]
        (is (refless= a-scmm1 e-scmm))
        (is (refless= a-scmm2 e-scmm)))

      (let [esw (scmmwrap 
                 {:name "scmwrap"
                  :val (scmm {:identity "hi" :vals [(scm2 {:val1 42}) (scm2 {:val1 7})]})})
            esw-id (create! {:conn *conn*} esw)
            asw1 (ffirst (q :find :ScmM :in (db) :where [:ScmMWrap {:name "scmwrap" :val %}]))
            asw2 (:val (recursive-ctor :ScmMWrap (d/entity (db) (get-eid (db) esw-id))))]
        )))
  (with-test-db simple-schema
    (testing "coll"
      (let [ex (create! {:conn *conn*} (scmmwrap {:val {:val (scm {:val1 "foobar"})}}))]
        (is (contains? (q :find [:Scm ...] :in (db) :where
                          [:ScmMWrap {:val [:ScmM {:val [% {:val1 "foobar"}]}]}])
                       (get-in ex [:val :val]))))))
  (with-test-db simple-schema
    (testing "absent field access"
      (let [eid (create! {:conn *conn*} (scm2))
            a-scm2 (recursive-ctor :Scm2 (d/entity (db) (get-eid (db) eid)))]
        (let [b (not (:val1 (scm2 a-scm2)))] ;; lol printing it out draws an early error
          (is b))
        #_(is (not (:val1 (scm2 a-scm2))))))))

(deftest test-q-complex-dispatch
  (with-test-db simple-schema
    (create! {:conn *conn*} (scm {:scm2 (scm2 {:val1 22})}))
    (let [soe (create! {:conn *conn*}
                       (scmownsenum {:enum (scm2 {:val1 42})}))]
      (is (= (let [si (:enum soe)]
               (q :find :ScmOwnsEnum :in (db) :where
                  [% {:enum si}]))
             (q :find :ScmOwnsEnum :in (db) :where
                [% {:enum (:enum soe)}])
             #{[soe]})
          "can pull db-ref out of object if it exists")))
  (with-test-db simple-schema
    (create! {:conn *conn*} (ferret {:name "catsnake"}))
    (create! {:conn *conn*} (mouse {:name "zuzu"}))
    (is (refless= (q :find :Animal :in (db) :where
                     [% {:name "zuzu"}])
                  #{[(mouse {:name "zuzu"})]}))
    (create! {:conn *conn*} (mouse {:name "catsnake"}))
    (is (refless= (q :find [:Animal ...] :in (db) :where
                     [% {:name "catsnake"}])
                  #{(mouse {:name "catsnake"})
                    (ferret {:name "catsnake"})}))))

(deftest test-q-bad-data
  (with-test-db simple-schema
    (is (thrown-with-msg?
         clojure.lang.ExceptionInfo #"nil"
         (let [nil-val nil] (q :find :Scm :in (db) :where [% {:val1 nil-val}])))
        "having a runtime nil in a map gets caught before Datomic")
    
    (let [id (get-in (create! {:conn *conn*} (scm {:val1 "baz"})) [:db-ref :eid])]
      (assert @(d/transact *conn* [[':db/add id :scm/scm2 123]]))
      (is (= id (ffirst (d/q '[:find ?scm :in $ :where [?scm :scm/scm2 123]] (db))))
          "insertion of bad scm2 ref should work")

      (is (thrown-with-msg?
           clojure.lang.ExceptionInfo #"bad entity in database"
           (:scm2 (recursive-ctor :Scm (d/entity (db) id))))
          "cant get an Scm2 out of it")

      (assert @(d/transact *conn*
                           [{:db/id (d/tempid :db.part/user -100)
                             :spec-tacular/spec :Scm2
                             :scm/val1 "5"}
                            [:db/add id :scm/scm2 (d/tempid :db.part/user -100)]]))

      (is (thrown? clojure.lang.ExceptionInfo
                   (q :find :Scm2 :in (db) :where [:Scm {:scm2 %}])))
      )))

(deftest test-q-pull
  (with-test-db simple-schema
    (let [spotlight (create! {:conn *conn*} (spotlight {:color :LenseColor/red}))]
      (is (= (q :find (pull :Spotlight [:color]) .
                :in (db)
                :where
                [% {:color :LenseColor/red}])
             {:color :LenseColor/red}))
      (is (= (q :find (pull :Spotlight [:color])
                :in (db)
                :where
                [% {:color :LenseColor/red}])
             #{[{:color :LenseColor/red}]}))
      (is (= (q :find [(pull :Spotlight [:color]) ...]
                :in (db)
                :where
                [% {:color :LenseColor/red}])
             #{{:color :LenseColor/red}}))
      (is (= (q :find [(pull :Spotlight [:color]) :LenseColor]
                :in (db)
                :where
                [%1 {:color %2}])
             [{:color :LenseColor/red} :LenseColor/red]))
      )))

(deftest test-q-aggregate
  (with-test-db simple-schema
    (let [ex (create! {:conn *conn*} (scm {:val1 "123" :val2 123}))
          ey (create! {:conn *conn*} (scm {:val1 "456" :val2 456}))]
      (is (= (q :find (min :long)
                :in (db)
                :where
                [:Scm {:val2 ?val2}]
                [(- ?val2 5) %])
             #{[118]}))))
  (with-test-db simple-schema
    (let [ex (create! {:conn *conn*} (scm {:val1 "123" :val2 123}))
          ey (create! {:conn *conn*} (scm {:val1 "456" :val2 456}))]
      (is (= (q :find (spark.spec-tacular.datomic.query-test/ex-aggregate :long) .
                :in (db)
                :where
                [:Scm {:val2 ?val2}]
                [(- ?val2 5) %1])
             5)))))

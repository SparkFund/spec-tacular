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
             5)))))

(deftest query-tests
  (testing "primitive data"
    (with-test-db simple-schema
      (is (= #{} (q :find ?a :in (db) :where [:ScmParent {:scm {:val2 ?a}}]))
          "nothing returned on fresh db.")

      (let [a1 (scmparent {:scm (scm {:val1 "a" :val2 1})})
            a2 (scmparent {:scm (scm {:val1 "b" :val2 2})})]
        (create! {:conn *conn*} a1)
        (create! {:conn *conn*} a2))
      
      (is (= #{[1] [2]}
             (q :find ?a :in (db) :where
                [:ScmParent {:scm {:val2 ?a}}]))
          "simple one-attribute returns (a ?-prefixed symbol isn't needed- just idiomatic cf datomic)")
      (is (= #{[1 "a"] [2 "b"]}
             (q :find ?a ?b :in (db) :where
                [:ScmParent {:scm {:val1 ?b :val2 ?a}}]))
          "multiple attribute returns")
      (is (contains? #{[1 "a"] [2 "b"]}
                     (q :find [?a ?b] :in (db) :where
                        [:ScmParent {:scm {:val1 ?b :val2 ?a}}]))
          "tuple")
      (is (contains? #{1 2}
                     (q :find ?a . :in (db) :where
                        [:ScmParent {:scm {:val2 ?a}}]))
          "scalar")
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
          "return variables respect lexical scope and don't clobber fns"))
    (with-test-db simple-schema
      (create! {:conn *conn*} (scmkw {:item :test}))
      (is (refless= #{[(scmkw {:item :test})]}
                    (q :find :ScmKw :in (db) :where
                       [% {:item :test}])))
      (is (refless= #{[:test]}
                    (q :find :keyword :in (db) :where
                       [:ScmKw {:item [:keyword %]}])))))

  (testing "compound data"
    (with-test-db simple-schema
      (let [e-scm2 (scm2 {:val1 5})
            e-scm  (scm {:val2 5 :scm2 e-scm2})
            e-scmp (scmparent {:scm e-scm})]
        (create! {:conn *conn*} e-scmp)

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
            (is (refless= a-scm e-scm))
            (is (refless= e-scm a-scm))))
        
        (let [[a-scm a-scm2]
              ,(->> (q :find :Scm :Scm2 :in (db) :where
                       [%1 {:scm2 %2}])
                    first)]
          (testing "equality on returned sub-entities"
            (is (= (:scm2 a-scm) a-scm2))
            (is (refless= a-scm2 e-scm2))
            (is (refless= a-scm  e-scm)))
          (is (not (:val1 a-scm)))
          (is (map? (:db-ref (:scm2 a-scm)))
              "allow :db-ref keyword access on sub-entities"))

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
            ;; (is (= asw1 esw) "returned from query equality")
            (testing "lazy-ctor"
              #_(is (instance? spark.spec_tacular.test_specs.l_ScmM asw2) "type") ;; TODO
              (is (:identity asw2) "keyword access")
              (is (= (type (:vals asw2)) clojure.lang.PersistentHashSet) "keyword access")
              ;; (is (= asw2 esw) "equality")
              ))))
      (testing "coll"
        (let [ex (create! {:conn *conn*} (scmmwrap {:val {:val (scm {:val1 "foobar"})}}))]
          (is (contains? (q :find [:Scm ...] :in (db) :where
                            [:ScmMWrap {:val [:ScmM {:val [% {:val1 "foobar"}]}]}])
                         (get-in ex [:val :val])))))
      (testing "absent field access"
        (let [eid (create! {:conn *conn*} (scm2))
              a-scm2 (recursive-ctor :Scm2 (d/entity (db) (get-eid (db) eid)))]
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
               (d/q '[:find ?type :in $ :where
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

  (testing "bad syntax" ; fully qualify for command line
    (with-test-db simple-schema
      (is (thrown-with-msg?
           clojure.lang.ExceptionInfo #"Invalid clause rhs"
           (macroexpand '(spark.spec-tacular.datomic/q :find :Scm2 :in (db)
                                                       :where [:Scm :scm2])))
          "using a (non-spec) keyword as a rhs")
      (is (thrown-with-msg?
           clojure.lang.ExceptionInfo #"could not infer type"
           (macroexpand '(spark.spec-tacular.datomic/q :find ?x :in (db)
                                                       :where [?x {:y 5}])))
          "impossible to determine spec of x")
      (is (thrown-with-msg?
           clojure.lang.ExceptionInfo #"unsupported ident"
           (macroexpand '(spark.spec-tacular.datomic/q :find ?x :in (db) :where
                                                       ["?x" {:y 5}])))
          "using a string as an ident")
      (is (thrown-with-msg?
           clojure.lang.ExceptionInfo #"could not find item"
           (macroexpand '(spark.spec-tacular.datomic/q :find :Scm :in (db) :where
                                                       [% {:y 5}])))
          "trying to specify a field that is not in the spec")
      (is (thrown-with-msg?
           clojure.lang.ExceptionInfo #"not supported"
           (macroexpand '(spark.spec-tacular.datomic/q :find ?val :in (db) :where
                                                       [:ScmEnum {:val1 ?val}])))
          "trying to pull out a field from an enum with different field types")

      (is (thrown-with-msg?
           clojure.lang.ExceptionInfo #"nil"
           (let [nil-val nil] (q :find :Scm :in (db) :where [% {:val1 nil-val}])))
          "having a runtime nil field is unacceptabbbllee")))

  (testing "bad data" ; db goes to shit after this -- should be last test
    (with-test-db simple-schema
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

        (is (thrown-with-msg?
             clojure.lang.ExceptionInfo #"bad entity in database"
             (q :find :Scm2 :in (db) :where [:Scm {:scm2 %}])))
        
        ;; TODO: add enum tests here
        ))))

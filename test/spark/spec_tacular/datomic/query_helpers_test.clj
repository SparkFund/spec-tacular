(ns spark.spec-tacular.datomic.query-helpers-test
  "tests helpers for sd/q and sd/query"
  (:refer-clojure :exclude [assoc!])
  (:require [clj-time.core :as time]
            [clojure.set :refer [map-invert]]
            [clojure.test :refer :all]
            [datomic.api :as d]
            [spark.spec-tacular :refer [get-item get-spec]]
            [spark.spec-tacular.datomic :refer [create!]]
            [spark.spec-tacular.datomic.query-helpers :refer :all]
            [spark.spec-tacular.schema :as schema]
            [spark.spec-tacular.test-specs :refer :all]
            [spark.spec-tacular.test-utils :refer [with-test-db *conn* db]]))

(def simple-schema
  (cons schema/spec-tacular-map
        (schema/from-namespace (the-ns 'spark.spec-tacular.test-specs))))

;; ---------------------------------------------------------------------------------------------------
;; static

(deftest test-keyword-as-find-variable
  #_(q :find :ScmKw :in (db) :where
       [% {:item :test}])
  (let [tenv (atom {}), uenv (atom {})]
    (expand-find-elems tenv uenv '(:ScmKw))
    (is (contains? @uenv '%1))
    (is (contains? (map-invert @tenv) :ScmKw))))

(deftest test-spec-cast
  #_(q :find :keyword :in (db) :where
       [:ScmKw {:item [:keyword %]}])
  (let [tenv (atom {})]
    (is (= (expand-item tenv :ScmKw :item '[:keyword ?x])
           {:map-entry [[:item `'~'?x]]}))
    (is (= (get @tenv '?x)
           :keyword))))

(deftest test-find-var-spec-cast
  #_(q :find [:Scm ...] :in (db) :where
       [:ScmMWrap {:val [:ScmM {:val [% {:val1 "foobar"}]}]}])
  (is (= (count (expand-spec-where-clause (atom {'?scm :Scm})
                                          '[:ScmMWrap {:val [:ScmM {:val [?scm {:val1 "foobar"}]}]}]))
         3)))

(deftest test-union-spec
  #_(q :find :Animal :in (db) :where [% {:name "zuzu"}])
  (is (= (count (expand-spec-where-clause (atom {'?animal :Animal})
                                          '[?animal {:name "zuzu"}]))
         1)))

(deftest test-parse-query
  (is (parse-query '(:find :Animal :in (db) :where [% {:name "zuzu"}])))
  (is (parse-query '(:find :Scm2 :in (db) :where [:Scm {:scm2 %}])))
  (is (= (-> (parse-query '(:find :House, :Person :in db :where
                                  [%1 {:occupants %2 :mailbox {:has-mail? true}}]))
             :where-expr (nth 2) rest)
         [:spec-tacular/spec :Person]))
  (is (parse-query '(:find ?scm2 . :in (db) :where [?scm [:Scm {:scm2 ?scm2}]] [?scm :scm/scm2 ?scm2])))
  (is (parse-query '(:find [:Scm ?direction] :in db :where
                           (or (and [% {:val2 5}]
                                    [(ground :incoming) ?direction])
                               (and [% {:val2 6}]
                                    [(ground :outgoing) ?direction])))))

  (testing "bad syntax"
    (is (thrown-with-msg?
         clojure.lang.ExceptionInfo #"invalid where clause"
         (parse-query '(:find :Scm2 :in (db) :where [:Scm :scm2])))
        "using a (non-spec) keyword as a rhs")
    (is (thrown-with-msg?
         clojure.lang.ExceptionInfo #"could not infer type"
         (parse-query '(:find ?x :in (db) :where [?x {:y 5}])))
        "impossible to determine spec of x")
    (is (thrown-with-msg?
         clojure.lang.ExceptionInfo #"invalid where clause" ; could specifically mention string being impossible
         (parse-query '(:find ?x :in (db) :where ["?x" {:y 5}])))
        "using a string as an ident")
    (is (thrown-with-msg?
         clojure.lang.ExceptionInfo #"keyword not in spec"
         (parse-query '(:find :Scm :in (db) :where [% {:y 5}])))
        "trying to specify a field that is not in the spec")
    (is (thrown-with-msg?
         clojure.lang.ExceptionInfo #"incompatible"
         (parse-query '(:find ?val :in (db) :where [:ScmEnum {:val1 ?val}])))
        "trying to pull out a field from an enum with different field types")))

;; ---------------------------------------------------------------------------------------------------
;; dynamic

(deftest test-combine-where-clauses
  (is (= (combine-where-clauses '(and [1] [2] [3])
                                '(and [4] [5] [6]))
         (combine-where-clauses [1]
                                [2]
                                [3]
                                '(and [4] [5] [6]))
         '(and [1] [2] [3] [4] [5] [6])))
  (is (= (combine-where-clauses '(or (and [1])
                                     (and [2])))
         '(or (and [1])
              (and [2])))))

(deftest test-datomify-spec-where-clause
  (is (= (datomify-spec-where-clause :Animal
                                     '?animal
                                     '{:spec-tacular/spec :Animal
                                       :name "zuzu"})
         '(or (and [?animal :spec-tacular/spec :Ferret]
                   [?animal :ferret/name "zuzu"])
              (and [?animal :spec-tacular/spec :Mouse]
                   [?animal :mouse/name "zuzu"])))))

(deftest test-datomify-where-clause
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
  (testing "calendarday"
    (is (= (datomify-where-clause ['?date {:spec-tacular/spec :Birthday
                                           :date (time/date-time 2015)}])
           '(and
             [?date :spec-tacular/spec :Birthday]
             [?date :birthday/date #inst "2015-01-01T00:00:00.000-00:00"]))))
  (testing "passing in an instance"
    (is (= (rest (last (datomify-where-clause ['scmp {:spec-tacular/spec :ScmParent
                                                      :scm (scm {:val1 "3"})}])))
           '[:scm/val1 "3"])
        "pulls the 3 out of the given scm")
    (is (= (datomify-where-clause ['scmp {:spec-tacular/spec :ScmParent
                                          :scm (scm {:db-ref {:eid 1}})}])
           '(and [scmp :spec-tacular/spec :ScmParent]
                 [scmp :scmparent/scm 1]))
        "just uses the db-ref of the scm")
    (is (= (datomify-where-clause ['scmp {:spec-tacular/spec :ScmParent
                                          :scm '?scm}])
           '(and [scmp :spec-tacular/spec :ScmParent]
                 [scmp :scmparent/scm ?scm]))
        "but it's still ok to get one as output"))
  (testing "unions"
    (is (= (datomify-where-clause ['?animal {:spec-tacular/spec :Animal, :name "zuzu"}])
           '(or (and [?animal :spec-tacular/spec :Ferret]
                     [?animal :ferret/name "zuzu"])
                (and [?animal :spec-tacular/spec :Mouse]
                     [?animal :mouse/name "zuzu"])))))
  (testing "lots of and clauses"
    (is (= (datomify-where-clause '(or (and [?scm73984 {:spec-tacular/spec :Scm, :val2 5}]
                                            [(ground :incoming) ?direction])
                                       (and [?scm73984 {:spec-tacular/spec :Scm, :val2 6}]
                                            [(ground :outgoing) ?direction])))
           '(or (and [?scm73984 :spec-tacular/spec :Scm]
                     [?scm73984 :scm/val2 5]
                     [(ground :incoming) ?direction])
                (and [?scm73984 :spec-tacular/spec :Scm]
                     [?scm73984 :scm/val2 6]
                     [(ground :outgoing) ?direction]))))
    (is (= (datomify-where-clause '(not (and [?scm73984 {:spec-tacular/spec :Scm, :val2 5}]
                                             [(ground :incoming) ?direction])
                                        (and [?scm73984 {:spec-tacular/spec :Scm, :val2 6}]
                                             [(ground :outgoing) ?direction])))
           '(not (and [?scm73984 :spec-tacular/spec :Scm]
                      [?scm73984 :scm/val2 5]
                      [(ground :incoming) ?direction]
                      [?scm73984 :scm/val2 6]
                      [(ground :outgoing) ?direction]))))))

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
                 [{:val2 123} ey])))))))

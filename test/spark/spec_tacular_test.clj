(ns spark.spec-tacular-test
  (:use spark.spec-tacular
        clojure.test
        spark.spec-tacular.generators)
  (:require [clojure.core.typed :as t]
            [clojure.test.check :as tc]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [clojure.test.check.clojure-test :as ct]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; defspec

(defspec TestSpec1
  [val1 :is-a :long :required]
  [val2 :is-a :string]
  [val3 :is-a :long :default-value 3]
  [val4 :is-a :keyword :default-value (fn [] :val)]
  [val5 :is-a :TestSpec3])

(deftest test-TestSpec1
  (testing "valid"
    (is (some? (get-spec :TestSpec1)))
    (is (some? (get-spec :TestSpec1 :TestSpec1)))
    (is (some? (get-spec {:spec-tacular/spec :TestSpec1})))
    (is (some? (get-spec (get-spec :TestSpec1))))
    
    (let [good (testspec1 {:val1 3 :val2 "hi"})]
      (is (testspec1? good))
      (is (= (:val1 good) 3))
      (is (= (:val2 good) "hi"))
      (is (= (:val3 good) 3))
      (is (= (:val4 good) :val))

      (is (= (keys good) [:val1 :val2 :val3 :val4]))

      (testing "has-spec?"
        (is (has-spec? good))
        (is (not (has-spec? 5)))))

    (is (= (count (into #{} [(testspec1 {:val1 5}) (testspec1 {:val1 5})])) 1)))

  (testing "invalid"
    (is (not (testspec1? {:val1 3 :val2 "hi"}))
        "not a record")
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"required" 
                          (testspec1 {:val1 nil}))
        "missing required field")
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"required"
                          (testspec1 {:val2 1}))
        "missing required field")
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"invalid type" 
                          (testspec1 {:val1 0 :val2 1}))
        "wrong type")
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #""
                          (testspec1 {:val1 3 :extra-key true}))
        "extra key")))

(defspec TestSpec2
  (:link [ts1 :is-a :TestSpec1]))
(defspec TestSpec3)

(deftest test-TestSpec2
  (is (doall (testspec2 {:ts1 (testspec1 {:val1 42})})))

  (testing "order of spec definition does not matter"
    (is (testspec1? (testspec1 {:val1 1 :val5 (testspec3)}))))

  (testing "links are not checked"
    (let [ts1 (i_TestSpec1. {::bad-key true} (atom {}) nil)]
      (is (testspec2 {:ts1 ts1})))))

(defspec TestSpec4
  [val1 :is-a :boolean])

(deftest test-TestSpec4
  (testing "valid"
    (is (some? (check-component! (get-spec :TestSpec4) :val1 false)))
    (is (testspec4? (testspec4 {:val1 false})))))

(defspec TestSpec5
  [name :is-a :string :required])

(deftest test-TestSpec5
  (testing "empty string"
    (is (some? (check-component! (get-spec :TestSpec5) :name "")))))

;; forward references
(defspec A [b :is-a :B])
(defspec B [a :is-a :A])

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; unions

(defunion testunion :TestSpec2 :TestSpec3)
(defspec ES [foo :is-a :testunion])
(defspec ESParent [es :is-a :ES])

(deftest test-defunion
  (is (some? (get-spec :testunion)))
  (is (= (get-spec :testunion {:spec-tacular/spec :TestSpec2})
         (get-spec :TestSpec2)))
  
  (is (testunion? (testspec2 {})))
  (is (instance? spark.spec_tacular.spec.UnionSpec (get-spec testunion)))
  (is (check-component! (get-spec :ES) :foo (testspec2 {})))
  (is (thrown? clojure.lang.ExceptionInfo (check-component! (get-spec :ES) :foo :nope)))
  (is (thrown? clojure.lang.ExceptionInfo (es (testspec1 {:val1 1}))))
  (is (thrown-with-msg? clojure.lang.ExceptionInfo #""
                        (get-spec :testunion (testspec1 {:val1 1}))))
  (is (thrown-with-msg? clojure.lang.ExceptionInfo #""
                        (esparent {:es {:foo (testspec1 {:val1 1})}}))
      "nested ctor fails properly with unions")
  (is (thrown-with-msg? clojure.lang.ExceptionInfo #""
                        (es {:foo (a {})}))))

;; forward union reference
(defunion UnionFoo :UnionForward)
(defspec UnionForward)

(defspec TestSpec6
  [union :is-many :UnionFoo])

(defspec Link
  (:link
   [ts1 :is-a :TestSpec1]
   [ts2 :is-many :TestSpec2])
  [ts3 :is-a :TestSpec3]
  [ts4 :is-many :TestSpec4]
  [s1 :is-many :string])

(deftest test-link
  (let [many [(testspec2) (testspec2 {:ts1 (testspec1 {:val1 42})})]
        l (link {:ts1 (testspec1 {:val1 42})
                 :ts2 many
                 :ts3 (testspec3)
                 :ts4 [(testspec4 {:val1 false})]})]
    (is (link? l))
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"not a map"
                          (recursive-ctor :TestSpec2 many)))
    (is (= (:ts2 l) (set many)))
    (is (doall (with-out-str (prn l)))))

  (let [l (link {:s1 ["a" "b" "c"]})]
    (is (link? l)))

  (let [l (link {:ts3 nil})]
    (is (link? l))
    (is (not (:ts3 l))))

  (let [l1 (link {:ts3 (assoc (testspec3) :db-ref 1)})]
    (is (= (refless l1)
           (link {:ts3 (testspec3)}))))
  
  (let [l1 (link {:ts3 (testspec3 {:db-ref 1}) :db-ref 3})
        l2 (link {:ts3 (testspec3 {:db-ref 2}) :db-ref 4})]
    (is (refless= [[[l1]]] [[[l2]]]) "refless equality"))

  (testing "is-many"
    (is (not= (link {:ts4 [(testspec4 {:val1 true}) (testspec4 {:val1 false})]})
              (link {:ts4 [(testspec4 {:val1 true}) (testspec4 {:val1 true})]})))
    (is (not= (link {:ts4 [(testspec4 {:val1 true}) (testspec4 {:val1 true})]})
              (link {:ts4 [(testspec4 {:val1 true}) (testspec4 {:val1 false})]})))))

(defspec TestSpec7
  [nums :is-many :long])

(deftest test-is-many
  (is (= (testspec7 {:nums  [1 2 3 4]})
         (testspec7 {:nums #{1 2 3 4}}))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; random testing

(defn prop-check-components
  "property for verifying that check-component!, create!, and update! work correctly"
  [spec-key]
  (let [sp-gen (mk-spec-generator spec-key)
        spec   (get-spec spec-key)
        gen    (gen/bind sp-gen gen/return)
        fields (map :name (:items spec))]
    (prop/for-all [instance gen]
      (and (every? #(check-component! spec % (get instance %)) fields)
           (do (with-out-str (prn instance)) true)))))

(ct/defspec gen-TestSpec3 100 (prop-check-components :TestSpec3))
(ct/defspec gen-TestSpec1 100 (prop-check-components :TestSpec1))
(ct/defspec gen-TestSpec2 100 (prop-check-components :TestSpec2))
(ct/defspec gen-TestSpec4 100 (prop-check-components :TestSpec4))
(ct/defspec gen-TestSpec5 100 (prop-check-components :TestSpec5))
(ct/defspec gen-testunion 100 (prop-check-components :testunion))
(ct/defspec gen-ES        100 (prop-check-components :ES))
(ct/defspec gen-ESParent  100 (prop-check-components :ESParent))
(ct/defspec gen-TestSpec6 100 (prop-check-components :TestSpec6))
(ct/defspec gen-Link      100 (prop-check-components :Link))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; diff

(defspec Human
  [name :is-a :string]
  [age :is-a :long]
  [pets :is-many :Animal])

(deftest test-diff
  (let [peter (human {:name "Peter" :age 17})
        paul  (human {:name "Paul"  :age 18})
        #_mary  #_(human {:name "Mary"  :age 18 :pets [(dog {:name "George"}) (cat {:name "Ringo"})]})]
    (is (= (diff peter paul)
           [{:name "Peter" :age 17} {:name "Paul" :age 18} {}]))
    (is (= (diff peter (human {:name "Peter" :age 25}))
           [{:age 17} {:age 25} {:name "Peter"}]))
    (is (= (diff peter (human {:age 25}))
           [{:name "Peter" :age 17} {:age 25} {}]))

    #_(is (= (diff mary (human {:name "Mary" :age 18 :pets [(cat {:name "Ringo"}) (dog {:name "George"})]}))
             [{} {} {:age 18,
                     :name "Mary",
                     :pets #{(cat {:name "Ringo"}) (dog {:name "George"})}}]))))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; ns

(def ns-specs (namespace->specs *ns*))
(deftest test-namespace->specs
  (let [[a b both] (clojure.data/diff
                    (into #{} (map :name ns-specs))
                    #{:TestSpec1 :TestSpec2 :TestSpec3 :TestSpec4 :TestSpec5
                      :testunion :ES :ESParent :UnionFoo :UnionForward :A :B
                      :Link :Human :TestSpec6 :TestSpec7})]
    (is (nil? b) "no missing specs")
    (is (nil? a) "no extra specs")))

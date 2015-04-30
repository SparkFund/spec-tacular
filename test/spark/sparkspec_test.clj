(ns spark.sparkspec-test
  {:core.typed {:collect-only true}}
  (:require [clojure.core.typed :as t])
  (:use spark.sparkspec
        clojure.test))

(defspec TestSpec1
  [val1 :is-a :long :required]
  [val2 :is-a :string]
  [val3 :is-a :long :default-value 3]
  [val4 :is-a :keyword :default-value (fn [] :val)]
  [val5 :is-a :TestSpec2])

(defspec TestSpec2
  [sillyval :is-a :TestSpec1])

(defspec TestSpec3)

(defspec TestSpec4
  [val1 :is-a :boolean])

(defspec TestSpec5
  [name :is-a :string :required])

(def good-spec (testspec1 {:val1 3 :val2 "hi"}))

(deftest defspec-tests
  (testing "Valid specs"
    (is (testspec1? good-spec)
        "Valid specs should conform to huh-forms"))

  (testing "Invalid specs"
    (is (not (testspec1? {:val1 3 :val2 "hi"}))
        "Specs only care about types.")
    (is (thrown-with-msg? java.lang.AssertionError #"is required" 
                          (testspec1 {:val1 nil})))
    (is (thrown-with-msg? java.lang.AssertionError #"is required"
                          (testspec1 {:val2 1})))
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"invalid type" 
                          (testspec1 {:val1 0 :val2 1}))))

  (testing "Default values"
    (is (= 3 (:val3 good-spec)))
    (is (= :val (:val4 good-spec))))

  (testing "has-spec?"
    (is (has-spec? good-spec))
    (is (not (has-spec? 5))))

  (testing "get-spec"
    (is (= (get-basis :TestSpec1) [:val1 :val2 :val3 :val4 :val5]))
    (is (= (get-basis good-spec) [:val1 :val2 :val3 :val4 :val5])))

  (testing "false"
    (is (some? (check-component! (get-spec :TestSpec4) :val1 false)))
    (is (testspec4? (testspec4 {:val1 false}))))

  (testing "required"
    (is (thrown-with-msg? java.lang.AssertionError #"is required"
                          (check-component! (get-spec :TestSpec5) :name nil)))
    (is (some? (check-component! (get-spec :TestSpec5) :name "")))))

(deftest recursive-tests
  (testing "order of spec definition does not matter"
    (is (testspec1? (testspec1 {:val1 1 :val5 (testspec2 {})})))))

(defenum testenum :TestSpec2 :TestSpec3)

(defspec ES [foo :is-a :testenum])
(defspec ESParent [es :is-a :ES])

(deftest defenum-tests
  (is (testenum? (testspec2 {})))
  (is (instance? spark.sparkspec.spec.EnumSpec (get-spec testenum)))
  (is (check-component! (get-spec :ES) :foo (testspec2 {})))
  (is (thrown? clojure.lang.ExceptionInfo (check-component! (get-spec :ES) :foo :nope)))
  (is (= (es {:foo (testspec3)})
         #spark.sparkspec_test.s_ES{:foo #spark.sparkspec_test.s_TestSpec3{}}))
  (is (thrown? clojure.lang.ExceptionInfo (es (testspec1 {:val1 1}))))
  (is (= (esparent {:es {:foo (testspec3)}})
         #spark.sparkspec_test.s_ESParent{:es #spark.sparkspec_test.s_ES{:foo #spark.sparkspec_test.s_TestSpec3{}}})
      "nested ctor works with enums")
  (is (thrown? clojure.lang.ExceptionInfo
               (esparent {:es {:foo (testspec1 {:val1 1})}}))
      "nested ctor fails properly with enums"))

(deftest lazy-tests
  (let [bad-test1 {:val1 3 :val2 2}
        ts2 ((get-lazy-ctor :TestSpec2) {:sillyval bad-test1})]
    (is (testspec2? ts2))
    (is (testspec1? (:sillyval ts2)))
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"invalid type" (:val2 (:sillyval ts2))))
    (is (= 3 (:val1 (:sillyval ts2)))))
  (let [l-ts4 ((get-lazy-ctor :TestSpec4) {:val1 true})]
    (is (= (testspec4 l-ts4)
           (testspec4 {:val1 true})))))


;; recursive and forward-references
(defenum EnumFoo :EnumForward)
(defspec EnumForward)

(defspec A [b :is-a :B])
(defspec B [a :is-a :A])

(def ns-specs (namespace->specs *ns*))
(deftest test-namespace->specs
  (let [[a b both] (clojure.data/diff 
                    (into #{} (map :name ns-specs))
                    #{:TestSpec1 :TestSpec2 :TestSpec3 :TestSpec4 :TestSpec5
                      :testenum :ES :ESParent :EnumFoo :EnumForward :A :B})]
    (is (nil? b) "no missing specs")
    (is (nil? a) "no extra specs")))

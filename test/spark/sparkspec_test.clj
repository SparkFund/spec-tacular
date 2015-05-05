(ns spark.sparkspec-test
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
  [ts1 :is-a :TestSpec1])

(defspec TestSpec3)

(defspec TestSpec4
  [val1 :is-a :boolean])

(defspec TestSpec5
  [name :is-a :string :required])

(deftest test-defspec
  (let [good (testspec1 {:val1 3 :val2 "hi"})]
    (testing "valid specs"
      (is (testspec1? good)
          "should conform to huh-forms"))

    (testing "invalid specs"
      (is (not (testspec1? {:val1 3 :val2 "hi"}))
          "not a record")
      (is (thrown-with-msg? java.lang.AssertionError #"is required" 
                            (testspec1 {:val1 nil}))
          "missing required field")
      (is (thrown-with-msg? java.lang.AssertionError #"is required"
                            (testspec1 {:val2 1}))
          "missing required field")
      (is (thrown-with-msg? clojure.lang.ExceptionInfo #"invalid type" 
                            (testspec1 {:val1 0 :val2 1}))
          "wrong type"))

    (testing "default values"
      (is (= 3 (:val3 good)))
      (is (= :val (:val4 good))))

    (testing "has-spec?"
      (is (has-spec? good))
      (is (not (has-spec? 5))))

    (testing "get-basis"
      (is (= (get-basis :TestSpec1) 
             [:val1 :val2 :val3 :val4 :val5]))
      (is (= (get-basis good)
             [:val1 :val2 :val3 :val4 :val5])))

    (testing "false"
      (is (some? (check-component! (get-spec :TestSpec4) :val1 false)))
      (is (testspec4? (testspec4 {:val1 false})))) ;; TODO: more false tests

    (testing "empty string"
      (is (some? (check-component! (get-spec :TestSpec5) :name ""))))))

(deftest recursive-tests
  (testing "order of spec definition does not matter"
    (is (testspec1? (testspec1 {:val1 1 :val5 (testspec2 {})})))))

(defenum testenum :TestSpec2 :TestSpec3)

(defspec ES [foo :is-a :testenum])
(defspec ESParent [es :is-a :ES])

(deftest test-defenum
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

;; recursive and forward-references
(defenum EnumFoo :EnumForward)
(defspec EnumForward)

(defspec A [b :is-a :B])
(defspec B [a :is-a :A])

(deftest test-lazy-ctor
  (let [bad-test1 {:val1 3 :val2 2}
        ts2 ((get-lazy-ctor :TestSpec2) {:ts1 bad-test1})]
    (is (testspec2? ts2))
    (is (testspec1? (:ts1 ts2)))
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"invalid type" (:val2 (:ts1 ts2))))
    (is (= 3 (:val1 (:ts1 ts2)))))
  (let [l-ts4 ((get-lazy-ctor :TestSpec4) {:val1 true})]
    (is (= (testspec4 l-ts4)
           (testspec4 {:val1 true}))))

  (testing "enum equality"
    (is (= ((get-lazy-ctor :EnumForward) {})
           ((get-ctor :EnumForward) {})))))

(def ns-specs (namespace->specs *ns*))
(deftest test-namespace->specs
  (let [[a b both] (clojure.data/diff 
                    (into #{} (map :name ns-specs))
                    #{:TestSpec1 :TestSpec2 :TestSpec3 :TestSpec4 :TestSpec5
                      :testenum :ES :ESParent :EnumFoo :EnumForward :A :B})]
    (is (nil? b) "no missing specs")
    (is (nil? a) "no extra specs")))

(defspec TestSpec6
  [enum :is-many :EnumFoo])

(deftest test-is-many
  (let [f (get-lazy-ctor :TestSpec6)]
    (testing "is-many"
      (is (checked-lazy-access
           (get-spec :TestSpec6)
           :enum [(enumforward) (enumforward)])
          "checked-lazy-access")
      (is (= [(enumforward) (enumforward)]
             [(enumforward) (enumforward)]))
      (is (= (:enum (f {:enum [(enumforward) (enumforward)]}))
             (:enum (testspec6 {:enum [(enumforward) (enumforward)]})))
          "equality"))))

(defspec Link
  (:link
   [ts1 :is-a :TestSpec1]
   [ts2 :is-many :TestSpec2])
  [ts3 :is-a :TestSpec3]
  [ts4 :is-many :TestSpec4])

(deftest test-link
  (is (link? (link {:ts1 (testspec1 {:val1 42})
                    :ts2 [(testspec2) (testspec2 {:ts1 (testspec1 {:val1 42})})]
                    :ts3 (testspec3)
                    :ts4 [(testspec4 {:val1 false})]}))))

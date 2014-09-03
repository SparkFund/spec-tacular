(ns spark.sparkspec-test
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

(def good-spec (testspec1 {:val1 3 :val2 "hi"}))

(deftest defspec-tests
  (testing "Valid specs"
    (is (testspec1? good-spec)
        "Valid specs should conform to huh-forms"))

  (testing "Invalid specs"
    (is (not (testspec1? {:val1 3 :val2 "hi"}))
        "Specs only care about types.")
    (is (thrown? java.lang.AssertionError (testspec1 {})))
    (is (thrown? java.lang.AssertionError (testspec1 {:val2 "hi"})))
    (is (thrown? java.lang.AssertionError (testspec1 {:val1 0 :val2 1}))))

  (testing "Default values"
    (is (= 3 (:val3 good-spec)))
    (is (= :val (:val4 good-spec))))

  (testing "has-spec?"
    (is (has-spec? good-spec))
    (is (not (has-spec? 5))))

  (testing "get-spec"
    (is (= (get-basis :TestSpec1) [:val1 :val2 :val3 :val4 :val5]))
    (is (= (get-basis good-spec) [:val1 :val2 :val3 :val4 :val5]))
    (is (= (get-basis TestSpec1) [:val1 :val2 :val3 :val4 :val5]))))

(deftest recursive-tests
  (testing "order of spec definition does not matter"
    (is (testspec1? (testspec1 {:val1 1 :val5 (testspec2 {})})))))

(defenum testenum [foo bar baz])

(defspec ES [foo :is-a :testenum])
(defspec ESParent [es :is-a :ES])

(deftest defenum-tests
  (is (testenum? :foo))
  (is (instance? spark.sparkspec.spec.EnumSpec (get-spec testenum)))
  (is (true? (check-component! (get-spec ES) :foo :bar)))
  (is (thrown? java.lang.AssertionError (check-component! (get-spec ES) :foo :nope)))
  (is (= (es {:foo :bar})
         #spark.sparkspec_test.ES{:foo :bar}))
  (is (thrown? java.lang.AssertionError (es {:foo :nope})))
  (is (= (esparent {:es {:foo :bar}})
         #spark.sparkspec_test.ESParent{:es #spark.sparkspec_test.ES{:foo :bar}})
      "nested ctor works with enums")
  (is (thrown? java.lang.AssertionError
               (esparent {:es {:foo :nope}}))
      "nested ctor fails properly with enums"))

(defrecord FakeContact [phones])

;; TODO: Test recursive construction

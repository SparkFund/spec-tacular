(ns spark.sparkspec-test
  (:require [clojure.core.typed :as t])
  (:use spark.sparkspec
        clojure.test))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; defspec

(defspec TestSpec1
  [val1 :is-a :long :required]
  [val2 :is-a :string]
  [val3 :is-a :long :default-value 3]
  [val4 :is-a :keyword :default-value (fn [] :val)]
  [val5 :is-a :TestSpec2])

(deftest test-TestSpec1
  (testing "valid"
    (let [good (testspec1 {:val1 3 :val2 "hi"})]
      (is (testspec1? good))
      (is (= (:val1 good) 3))
      (is (= (:val2 good) "hi"))
      (is (= (:val3 good) 3))
      (is (= (:val4 good) :val))

      (testing "has-spec?"
        (is (has-spec? good))
        (is (not (has-spec? 5))))
      (testing "get-basis"
        (is (= (get-basis :TestSpec1) 
               [:val1 :val2 :val3 :val4 :val5]))
        (is (= (get-basis good)
               [:val1 :val2 :val3 :val4 :val5])))))
  (testing "invalid"
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
        "wrong type")
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #""
                          (testspec1 {:val1 3 :extra-key true}))
        "extra key")))

(defspec TestSpec2
  (:link [ts1 :is-a :TestSpec1]))

(deftest test-TestSpec2
  (is (doall (testspec2 {:ts1 (testspec1 {:val1 42})})))

  (testing "order of spec definition does not matter"
    (is (testspec1? (testspec1 {:val1 1 :val5 (testspec2 {})}))))

  (testing "links are not checked"
    (let [ts1 (i_TestSpec1. {::bad-key true})]
      (is (testspec2 {:ts1 ts1})))))

(defspec TestSpec3)

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
;; enums

(defenum testenum :TestSpec2 :TestSpec3)
(defspec ES [foo :is-a :testenum])
(defspec ESParent [es :is-a :ES])

(deftest test-defenum
  (is (testenum? (testspec2 {})))
  (is (instance? spark.sparkspec.spec.EnumSpec (get-spec testenum)))
  (is (check-component! (get-spec :ES) :foo (testspec2 {})))
  (is (thrown? clojure.lang.ExceptionInfo (check-component! (get-spec :ES) :foo :nope)))
  (is (thrown? clojure.lang.ExceptionInfo (es (testspec1 {:val1 1}))))
  (is (thrown-with-msg? clojure.lang.ExceptionInfo #""
                        (esparent {:es {:foo (testspec1 {:val1 1})}}))
      "nested ctor fails properly with enums"))

;; forward enum reference
(defenum EnumFoo :EnumForward)
(defspec EnumForward)

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

#_(deftest test-is-many
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

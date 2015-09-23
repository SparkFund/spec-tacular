(ns spark.spec-tacular.meta-test
  {:core.typed {:collect-only true}
   :spec-tacular {:ctor-name-fn  (fn [s] (str "mk-" (clojure.string/upper-case s)))
                  :huh-name-fn   (fn [s] (str (clojure.string/upper-case s) "?"))
                  :alias-name-fn (fn [s] (clojure.string/upper-case s))}}
  (:require [spark.spec-tacular :refer :all]
            [clojure.test :refer :all]
            [clojure.core.typed :as t]))

(defspec Keyboard
  [type :is-a :KeyboardVendor]
  [warranty :is-a :KeyboardWarranty])

(defunion KeyboardWarranty
  :TwoYearsNoRepairs
  :FiveYearsPartsLabor)

(defspec TwoYearsNoRepairs)
(defspec
  ^{:ctor-name  "BestDeal"
    :huh-name   false ;; fall back to default
    :alias-name "TwelveYearsDungeon"}
  FiveYearsPartsLabor
  [company :is-a :string])

(defenum KeyboardVendor
  Razer TrulyErgonomic Das Kinesis)

(deftest test-caps-lock-key-got-stuck
  (testing "ctor-name"
    (is mk-KEYBOARD)
    (is mk-TWOYEARSNOREPAIRS)
    (is BestDeal))

  (testing "huh-name"
    (is (KEYBOARD? (mk-KEYBOARD {})))
    (is (KEYBOARDWARRANTY? (mk-TWOYEARSNOREPAIRS {})))
    (is (KEYBOARDWARRANTY? (BestDeal {})))
    (is (fiveyearspartslabor? (BestDeal {})))
    (is (KEYBOARDVENDOR? :KeyboardVendor/Razer))
    (is (KEYBOARDVENDOR? :KeyboardVendor/Kinesis))))

(ns spark.sparkspec.diff-test
  (:use clojure.test
        spark.sparkspec
        spark.sparkspec.spec
        spark.sparkspec.diff
        spark.sparkspec.test-specs))

(defspec Human
  [name :is-a :string]
  [age :is-a :long]
  [pets :is-many :Animal])

(deftest test-diff
  (let [peter (human {:name "Peter" :age 17})
        paul  (human {:name "Paul"  :age 18})
        mary  (human {:name "Mary"  :age 18 :pets [(dog {:name "George"}) (cat {:name "Ringo"})]})]
    (is (= (diff peter paul)
           [{:name "Peter" :age 17} {:name "Paul" :age 18} {}]))
    (is (= (diff peter (human {:name "Peter" :age 25}))
           [{:age 17} {:age 25} {:name "Peter"}]))
    (is (= (diff peter (human {:age 25}))
           [{:name "Peter" :age 17} {:age 25} {}]))

    (is (= (diff mary (human {:name "Mary" :age 18 :pets [(cat {:name "Ringo"}) (dog {:name "George"})]}))
           [{} {} {:age 18,
                   :name "Mary",
                   :pets #{(cat {:name "Ringo"}) (dog {:name "George"})}}]))))

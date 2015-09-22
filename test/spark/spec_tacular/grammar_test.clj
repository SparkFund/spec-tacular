(ns spark.spec-tacular.grammar-test
  (:use clojure.test)
  (:require [spark.spec-tacular.grammar :refer :all]))

(deftest test-valid-syntax
  (testing "spec"
    (let [spec (parse-spec '(Link
                             (:link
                              [a :is-a :A]
                              [b :is-many :B])
                             [c :is-a :C :required]
                             [d :is-many :D]))]
      (is (= (:name spec) :Link))
      (is (= (:opts spec) nil))
      (is (= (:items spec)
             [#spark.spec_tacular.spec.Item{:name :a, :type [:one :A],:precondition nil, 
                                            :required? nil,:unique? nil,:optional? nil, 
                                            :identity? nil, :default-value nil, :link? true}
              #spark.spec_tacular.spec.Item{:name :b, :type [:many :B], :precondition nil, 
                                            :required? nil, :unique? nil, :optional? nil, 
                                            :identity? nil, :default-value nil, :link? true}
              #spark.spec_tacular.spec.Item{:name :c, :type [:one :C], :precondition nil, 
                                            :required? true, :unique? nil, :optional? nil, 
                                            :identity? nil, :default-value nil}
              #spark.spec_tacular.spec.Item{:name :d, :type [:many :D], :precondition nil, 
                                            :required? nil, :unique? nil, :optional? nil, 
                                            :identity? nil, :default-value nil}]))))

  (testing "union"
    (is (= (parse-union '(Foo :Bar :Baz))
           #spark.spec_tacular.spec.UnionSpec{:name :Foo, :elements #{:Baz :Bar}})))

  (testing "enum"
    (is (= (parse-enum '(Foo Bar Baz))
           #spark.spec_tacular.spec.EnumSpec{:name :Foo, :values #{:Foo/Bar, :Foo/Baz}}))

    (is (= (:items (parse-spec '(HasEnum [word :is-a :IsEnum])))
           [#spark.spec_tacular.spec.Item{:name :word, :type [:one :IsEnum]}]))))

(deftest test-invalid-syntax
  (testing "spec"
    (is (thrown? clojure.lang.ExceptionInfo
                 (parse-spec '(Foo [nonsense :nonsense]))))

    (is (thrown? clojure.lang.ExceptionInfo
                 (parse-spec '(Foo [nonsense :is-a :string :nonsense]))))

    (is (thrown? clojure.lang.ExceptionInfo
                 (parse-spec '(Foo :nonsense))))

    (is (thrown? clojure.lang.ExceptionInfo
                 (parse-spec '(Person [name :is-a string]))))

    #_(is (thrown? clojure.lang.ExceptionInfo ;; TODO
                   (parse-spec '(Foo [bar :is-a :Bar] [bar :is-a :Bar]))))

    (is (thrown? clojure.lang.ExceptionInfo
                 (parse-spec '(Container [one :is-a :Container :link :component])))))

  (testing "union"
    #_(is (thrown? clojure.lang.ExceptionInfo ;; TODO
                   (parse-union '(Foo 5)))))

  (testing "enum"
    (is (thrown? clojure.lang.ExceptionInfo (parse-enum '(Foo :bar))))
    (is (thrown? clojure.lang.ExceptionInfo (parse-enum '(:Foo :bar))))
    (is (thrown? clojure.lang.ExceptionInfo (parse-enum '(Foo))))
    (is (thrown? clojure.lang.ExceptionInfo (parse-enum '())))))

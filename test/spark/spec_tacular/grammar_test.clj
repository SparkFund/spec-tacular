(ns spark.spec-tacular.grammar-test
  (:use clojure.test)
  (:require [spark.spec-tacular.grammar :refer [parse-spec parse-union]]))

(deftest test-valid-syntax
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
                                          :identity? nil, :default-value nil}])))


  (is (= (parse-union '(Foo :Bar :Baz))
         #spark.spec_tacular.spec.UnionSpec{:name :Foo, :elements #{:Baz :Bar}})))

(deftest test-invalid-syntax
  (is (thrown? clojure.lang.ExceptionInfo
               (parse-spec '(Foo [nonsense :nonsense]))))

  (is (thrown? clojure.lang.ExceptionInfo
               (parse-spec '(Foo [nonsense :is-a :string :nonsense]))))

  (is (thrown? clojure.lang.ExceptionInfo
               (parse-spec '(Foo :nonsense))))

  (is (thrown? clojure.lang.ExceptionInfo
               (parse-spec '(Person [name :is-a string]))))

  ;; TODO
  #_(is (thrown? clojure.lang.ExceptionInfo
                 (parse-spec '(Foo [bar :is-a :Bar] [bar :is-a :Bar]))))

  #_(is (thrown? clojure.lang.ExceptionInfo
                 (parse-union '(Foo 5)))))

(ns spark.sparkspec.grammar-test
  (:use clojure.test)
  (:require [spark.sparkspec.grammar :refer [parse-spec parse-enum]]))

(deftest test-valid-syntax
  (is (= (parse-spec '(Link
                       (:link
                        [a :is-a :A]
                        [b :is-many :B])
                       [c :is-a :C :required]
                       [d :is-many :D]))
         #spark.sparkspec.spec.Spec
         {:name :Link, 
          :opts nil, 
          :items [#spark.sparkspec.spec.Item{:name :a, :type [:one :A],:precondition nil, 
                                             :required? nil,:unique? nil,:optional? nil, 
                                             :identity? nil, :default-value nil, :link? true}
                  #spark.sparkspec.spec.Item{:name :b, :type [:many :B], :precondition nil, 
                                             :required? nil, :unique? nil, :optional? nil, 
                                             :identity? nil, :default-value nil, :link? true}
                  #spark.sparkspec.spec.Item{:name :c, :type [:one :C], :precondition nil, 
                                             :required? true, :unique? nil, :optional? nil, 
                                             :identity? nil, :default-value nil}
                  #spark.sparkspec.spec.Item{:name :d, :type [:many :D], :precondition nil, 
                                             :required? nil, :unique? nil, :optional? nil, 
                                             :identity? nil, :default-value nil}]}))

  (is (= (parse-enum '(Foo :Bar :Baz))
         #spark.sparkspec.spec.EnumSpec{:name :Foo, :elements #{:Baz :Bar}})))

(deftest test-invalid-syntax
  (is (thrown? clojure.lang.ExceptionInfo
               (parse-spec '(Foo [nonsense :nonsense]))))

  (is (thrown? clojure.lang.ExceptionInfo
               (parse-spec '(Foo [nonsense :is-a :string :nonsense]))))

  (is (thrown? clojure.lang.ExceptionInfo
               (parse-spec '(Foo :nonsense))))

  ;; TODO
  #_(is (thrown? clojure.lang.ExceptionInfo
                 (parse-spec '(Foo [bar :is-a :Bar] [bar :is-a :Bar]))))

  #_(is (thrown? clojure.lang.ExceptionInfo
                 (parse-enum '(Foo 5)))))

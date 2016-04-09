(ns spark.spec-tacular.datomic.util
  {:doc "Utility functions for spark.spec-tacular.datomic"
   :core.typed {:collect-only true}}
  (:require [clojure.core.typed :as t]
            [clojure.string :refer [lower-case]]
            ;; for types
            [spark.spec-tacular :as spec]))

(t/ann ^:no-check db-keyword [SpecT t/Keyword -> t/Keyword])
(defn db-keyword
  [spec a]
  (let [dns (-> spec :name name lower-case)
        make-keyword #(keyword dns %)]
    (cond
      (instance? clojure.lang.Named a)
      ,(make-keyword (name a))
      (contains? a :name)
      ,(make-keyword (name (:name a)))
      :else (throw (ex-info "cannot make db-keyword" {:spec spec :attr a})))))

(t/ann ^:no-check datomic-ns [SpecT -> t/Str])
(defn datomic-ns
  "Returns a string representation of the db-normalized namespace for the given spec."
  [spec]
  (some-> spec :name name lower-case))

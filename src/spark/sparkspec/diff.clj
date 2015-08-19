(ns spark.sparkspec.diff
  (:use spark.sparkspec.spec
        spark.sparkspec)
  (:require [clojure.core.typed :as t]
            [clojure.data :as data]))

(defn diff
  "Takes two spec instances and returns a vector of three maps created
  by calling clojure.data/diff on each item of the spec.

  Only well defined when sp1 and sp2 share the same spec.

  For :is-many fields, expect to see sets of similarities or
  differences in the result, as order should not matter."
  [sp1 sp2]
  (when-not (= (get-spec sp1) (get-spec sp2))
    (throw (ex-info "Spec instances do not share spec" {:sp1 sp1 :sp2 sp2})))
  (let [diff-results
        ,(for [key (set (concat (keys sp1) (keys sp2)))
               :let [v1 (key sp1), v2 (key sp2)]]
           [key (if (map? v1)
                  (if (= v1 v2)
                    [[nil nil v1]]
                    [[v1 v2 nil]])
                  (data/diff (if (vector? v1) (set v1) v1)
                             (if (vector? v2) (set v2) v2)))])
        keep-column
        ,(fn [col]
           (->> diff-results
                (keep (fn [[kw res]] (let [v (col res)] (when (some? v) [kw v]))))
                (into {})))]
    [(keep-column #(nth % 0))
     (keep-column #(nth % 1))
     (keep-column #(nth % 2))]))

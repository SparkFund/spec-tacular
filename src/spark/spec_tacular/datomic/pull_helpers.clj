(ns spark.spec-tacular.datomic.pull-helpers
  (:require [clj-time.coerce :as timec]
            [datomic.api :as d]
            [spark.spec-tacular :refer [get-spec get-item primitive? recursive-ctor]]
            [spark.spec-tacular.datomic.util :refer [db-keyword]]
            [spark.spec-tacular.datomic.coerce :refer [coerce-datomic-entity]])
  (:import (spark.spec_tacular.spec Item
                                    EnumSpec
                                    UnionSpec)))

(defn datomify-spec-pattern [spec pattern]
  (cond
    (instance? UnionSpec spec)
    (let [rec (map #(datomify-spec-pattern (get-spec %) pattern) (:elements spec))]
      {:datomic-pattern (mapcat :datomic-pattern rec)
       :rebuild (fn [db m] (apply merge (map #(% db m) (map :rebuild rec))))})
    (keyword? pattern)
    (when-let [{[arity sub-spec-name] :type :as item} (get-item spec pattern)]
      (let [kw (db-keyword spec pattern)
            sub-spec (get-spec sub-spec-name)]
        (if (and (primitive? sub-spec-name)
                 (instance? EnumSpec sub-spec))
          {:datomic-pattern [{kw [:db/ident]}]
           :rebuild (fn [db m]
                      (when-let [v (get m kw)]
                        [pattern (:db/ident v)]))}
          {:datomic-pattern [kw]
           :rebuild (fn [db m]
                      (when-let [v (get m kw)]
                        (let [f #(recursive-ctor (get-spec sub-spec-name)
                                                 (if (:component? item)
                                                   (coerce-datomic-entity %)
                                                   (d/entity db (:db/id %))))]
                          [pattern
                           (if (primitive? sub-spec-name)
                             (if (= sub-spec-name :calendarday)
                               (timec/to-date-time v)
                               v)
                             (if (= :many arity)
                               (mapv f v)
                               (f v)))])))})))
    (instance? Item pattern)
    (let [spec (get-spec (:parent-name pattern))
          kw (db-keyword spec (keyword (str "_" (name (:name pattern)))))]
      {:datomic-pattern [kw]
       :rebuild (fn [db m]
                  (when-let [v (get m kw)]
                    (let [f #(recursive-ctor spec (d/entity db (:db/id %)))]
                      [pattern
                       (if (:component? pattern)
                         (f v)
                         (map f v))])))})
    (vector? pattern)
    (let [rec (keep (partial datomify-spec-pattern spec) pattern)
          datomic-pattern (mapcat :datomic-pattern rec)]
      {:datomic-pattern datomic-pattern
       :rebuild (fn [db m] (into {} (map #(% db m) (map :rebuild rec))))})
    (map? pattern)
    (let [rec (for [[kw-or-item sub-pattern] pattern
                    :let [{[arity sub-spec-name] :type component? :component? :as item}
                          (if (keyword? kw-or-item)
                            (get-item spec kw-or-item)
                            kw-or-item)
                          sub-spec-name
                          (if (keyword? kw-or-item)
                            sub-spec-name
                            (:parent-name kw-or-item))
                          {[db-kw] :datomic-pattern}
                          (datomify-spec-pattern spec kw-or-item)
                          {:keys [datomic-pattern rebuild]}
                          (datomify-spec-pattern (get-spec sub-spec-name) sub-pattern)]]
                {:datomic-pattern [db-kw datomic-pattern]
                 :rebuild (fn [db m]
                            (when-let [v (get m db-kw)]
                              [kw-or-item (cond (instance? Item kw-or-item)
                                                (if (:component? kw-or-item)
                                                  (rebuild db v)
                                                  (map #(rebuild db %) v))
                                                (= arity :many)
                                                (map #(rebuild db %) v)                                                
                                                :else (rebuild db v))]))})]
      {:datomic-pattern [(dissoc (into {} (map :datomic-pattern rec)) nil)]
       :rebuild (fn [db m] (into {} (map #(% db m) (map :rebuild rec))))})))


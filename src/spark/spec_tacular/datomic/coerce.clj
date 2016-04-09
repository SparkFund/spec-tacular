(ns spark.spec-tacular.datomic.coerce
  (:require [spark.spec-tacular :refer [get-spec]]
            [spark.spec-tacular.datomic.util :refer [db-keyword]])
  (:import (spark.spec_tacular.spec EnumSpec)))

(defn coerce-datomic-entity [em]
  (if-let [spec (get-spec em)] ;; Spec
    (do (when-not (every? (fn [kw] (some #(= (db-keyword spec (:name %)) kw) (:items spec)))
                          (filter #(case % (:spec-tacular/spec :db/id :db/txInstant) false true)
                                  (keys em)))
          (throw (ex-info "bad entity in database" {:entity em})))
        (->> (for [{iname :name :as item} (:items spec)]
               [iname (get em (db-keyword spec iname))])
             (cons [:db-ref {:eid (:db/id em)}])
             (cons [:spec-tacular/spec (:name spec)])
             (filter (comp some? second))
             (into {})))
    (if-let [kw (:db/ident em)] ;; EnumSpec
      (do (when-not (keyword? kw)
            (throw (ex-info "bad enum in database" {:entity em})))
          (if-let [spec (get-spec kw)]
            (if (instance? EnumSpec spec) kw
                (throw (ex-info "bad enum in database" {:entity em})))))
      (throw (ex-info "bad entity in database" {:entity em})))))

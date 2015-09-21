(ns spark.spec-tacular.restify
  "Totally unsupported and depricated, but this namespace would be a
  good start for web serialization."
  (:require [datomic.api :as d]
            [clojure.walk :as walk]
            [spark.spec-tacular.datomic :as sd]
            [spark.spec-tacular :as sp]
            [clojure-csv.core :as csv]
            [clojure.string :as str :refer [lower-case]])
  (:import java.lang.Throwable))

;; -----------------------------------------------------------------------------
;; inspect-spec

(defn ^:no-doc inspect-spec
  "Produces a json-friendly nested-map representation of a spec.
   Nesting depth is bounded by the mask."
  [spec-name mask & [resource-prefix-str schema-prefix-str]]
  (let [spec (sp/get-spec spec-name)
        spec-type (if (:elements spec)
                    :union
                    (if (sp/primitive? spec-name)
                      :primitive
                      :record))
        resource-kv (when (and resource-prefix-str (= :record spec-type))
                      {:resource-url (str resource-prefix-str "/"
                                      (lower-case (name spec-name)))})
        inspect-kv (when (and schema-prefix-str (#{:record :union} spec-type))
                     {:schema-url (str schema-prefix-str "/"
                                          (lower-case (name spec-name)))})]
    (when mask
      (merge
       {:spec-name spec-name
        :spec-type spec-type}
       inspect-kv
       resource-kv
       (if (map? mask)
         (if (= :union spec-type)
           {:expanded true
            :union-elements (->> (:elements spec)
                                (map #(inspect-spec % (get mask %) resource-prefix-str schema-prefix-str))
                                (filter some?))}
           (let [items
                 , (for [{iname :name [cardinality sub-sp-nm] :type :as item} (:items spec)
                         :when (iname mask)]
                     {iname {:many (= cardinality :many)
                             :required (if (:required? item) true false)
                             ;; :identity? (:identity? item) ; not meaningful for front-end?
                             ;; :unique? (:unique? item)
                             ;; :optional (:optional item)
                             :spec (inspect-spec sub-sp-nm (iname mask) resource-prefix-str schema-prefix-str)}})]
             {:expanded true
              :items (or (reduce merge items) [])}))
         (if (= :primitive spec-type)
           {:expanded true}
           {:expanded false}))))))

;; -----------------------------------------------------------------------------

(defn explicitly-tag
  "deep-walks a sp object adding explicit :spec-tacular/spec
   spec name tags to the object and all its child items."
  [sp]
  (let [spec (sp/get-spec sp)
        {recs :rec non-recs :non-rec} (group-by sp/recursiveness (:items spec))
        sub-kvs (->> recs
                     (map (fn [{[arity sub-spec-name] :type :as item}]
                            (let [sub-sp (get sp (:name item))]
                              (cond
                               (and (= arity :one) (some? sub-sp)) ; only build non-nil sub-sps
                               , [(:name item) (explicitly-tag sub-sp)]
                               (and (= arity :many) (some? sub-sp))
                               , [(:name item) (map #(explicitly-tag %) sub-sp)]
                               :else nil))))
                     (filter some?))]
    (into (merge (into {} sp) {:spec-tacular/spec (:name spec)}) sub-kvs)))

(defn to-json-friendly
  "converts sp object to json representation with explicit spec tags
  with the un-namespaced :spec-tacular-spec keyword"
  [sp]
  (->> (explicitly-tag sp)
       (walk/postwalk
        (fn [o] (if (and (map? o) (contains? o :spec-tacular/spec))
                  (-> o
                      (assoc :spec-tacular-spec (get o :spec-tacular/spec))
                      (dissoc :spec-tacular/spec))
                  o)))))

(defn from-json-friendly
  "converts from the json converted rep to the given spark record re-namespacing the 
  spec tag :spec-tacular-spec to :spec-tacular/spec.
  Also converts the spec tag back into a keyword, (json would have stringed it)"
  [spec-name jf]
  (->> jf
       (walk/postwalk
        (fn [o]
          (if (and (map? o) (get o :spec-tacular-spec))
               (-> o
                   (assoc :spec-tacular/spec (keyword (get o :spec-tacular-spec)))
                   (dissoc :spec-tacular-spec))
               o)))
       (sp/recursive-ctor spec-name)))

(defn csv-extract-fields
  "returns list of pairs of colName and colValues, where colName is a list of 'item' names representing the nesting structure.
  eg ([(:phones) 'List of 0 phones'] [(:first-name) nil] [(:some-nested-thing :first-name) nil])"
  [spec-name sp]
  (let [spec (sp/get-spec spec-name)]
    (if (:elements spec)
      [[(list spec-name) (:name (sp/get-spec sp))]]
      (let [{recs :rec non-recs :non-rec} (group-by sp/recursiveness (:items spec))
            rec-cols (->> recs
                          (map (fn [{[arity sub-spec-name] :type :as item}]
                                 (let [sub-sp (get sp (:name item))]
                                   (cond
                                     (= arity :one)
                                     ;TODO: extract this and share w/ :many-arity case?
                                     , (->> (csv-extract-fields sub-spec-name sub-sp) ; :grandparentAttribute -> [[[:parentAttribute :subAttribute] col-value],...]
                                            (map (fn [[colName colVal]]
                                                   [(cons (:name item) colName) colVal])))
                                     (= arity :many)
                                     , (let [key-name (name (:name item))
                                             ;for arrays, we want to expand columns for the FIRST element in the array
                                             ;TODO: extract this and share w/ :one-arity case?
                                             sub-fields (->> (csv-extract-fields sub-spec-name (first sub-sp))
                                                            (map (fn [[colName colVal]]
                                                                   [(cons (str key-name "[0]") colName) colVal])))]
                                         (concat [[(list (str key-name "[].count")) (count sub-sp)]] sub-fields))
                                     :else nil))))
                          (filter some?)
                          (apply concat))
            non-rec-cols (->> non-recs
                              (map (fn [item]
                                     [(list (:name item)) (get sp (:name item))])))]
        (concat non-rec-cols rec-cols)))))

(defn csv-build
  "returns a comma delimited string listing all instances of a given spec.
   usage like (build-csv :User (sd/get-all-of-type (db/get-db) (sp/get-spec :User)))"
  [sp-name sp-list]
  (let [col-names (->> (csv-extract-fields sp-name nil)
                       (map first)
                       (map #(clojure.string/join "." (map name %))))
        vals (map (fn [sp]
                    (->> (csv-extract-fields sp-name sp)
                         (map (comp str second))))
                  sp-list)]
    (csv/write-csv (cons col-names vals))))

(defn- mk-coll-get-response-json
  "Returns JSON describing the collection, in a ring-response. If ?simple=true is passed as a query param, then the items
  will go through a transformation function which returns {:value value :display \"display\"} representation"
  [req eids sp-list simple-repr-fn]
  (let [tagged (map to-json-friendly sp-list)
        simple-mode (and (fn? simple-repr-fn)
                         (= (-> req :query-params :simple) "true"))
        result (if simple-mode (map simple-repr-fn tagged) tagged)]
    result))

(defn- ent-of-type?
  "Helper; Given a datomic entity (from d/entity) and a spec,
  returns true if the entity is of that spec's type"
  [ent spec]
  (and (some? ent)
       (not (empty? ent))
       (= (:name spec) (:spec-tacular/spec ent))))

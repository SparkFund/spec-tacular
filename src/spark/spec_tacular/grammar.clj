(ns spark.spec-tacular.grammar
  (:use [spark.spec-tacular.spec])
  (:require [clojure.core.match :refer [match]])
  (:refer-clojure :exclude [name]))

(declare parse-spec parse-item parse-type parse-opts)

;; -----------------------------------------------------------------------------
;; spec

(defn parse-spec [stx & [loc]]
  (let [loc (or loc (merge {:namespace (str *ns*)} (meta stx)))]
    (letfn [(build [name docstring items]
              (let [name  (keyword name)
                    items (mapcat #(parse-item % loc) items)
                    attrs (cond-> {:name name
                                   :items items
                                   :syntax (cons 'defspec stx)}
                            docstring (assoc :doc docstring))]
                (map->Spec attrs)))]
      (match stx
        ([name (docstring :guard string?) & items] :seq)
        ,(build name docstring items)
        ([name & items] :seq)
        ,(build name nil items)
        :else (throw (ex-info "expecting name followed by sequence of items"
                              (merge loc {:syntax stx})))))))

(defn parse-item [stx & [loc]]
  (match stx
    ([:link & items*] :seq)
    ,(mapcat parse-item (map #(conj % :link) items*))

    ([:component & items*] :seq)
    ,(mapcat parse-item (map #(conj % :component) items*))

    (([name card t & opts] :seq) :guard vector?)
    ,(let [cardinality (case card (:is-a :is-an) :one (:is-many) :many)
           item-info (->> (parse-opts opts loc)
                          (#(if (contains? type-map t) (dissoc % :link?) %)))
           item-name (keyword name)]
       (when (and (:component? item-info) (:link? item-info))
         (throw (ex-info "can't have a component link"
                         (merge loc {:syntax stx}))))
       (when-not (keyword? t)
         (throw (ex-info (str "expecting keyword type, got " (type t))
                         (merge loc {:syntax stx}))))
       [(map->Item (merge {:name item-name :type [cardinality t]} item-info))])

    :else (throw (ex-info "expecting item [item-name cardinality type opts*] or (:link item*)"
                          (merge loc {:syntax stx})))))

(defn parse-opts [stx & [loc]]
  (let [k (fn [m rest] (merge m (parse-opts rest)))]
    (match stx
      (_ :guard empty?) {}
      ([:precondition func & rest] :seq) (k {:precondition func} rest)
      ([:required & rest] :seq)          (k {:required? true}    rest)
      ([:identity & rest] :seq)          (k {:identity? true}    rest)
      ([:unique & rest] :seq)            (k {:unique? true}      rest)
      ([:link & rest] :seq)              (k {:link? true}        rest)
      ([:default-value v & rest] :seq)   (k {:default-value v}   rest)
      ([:component & rest] :seq)         (k {:component? true}   rest)
      ([:doc doc-string & rest] :seq)    (k {}                   rest)
      :else (throw (ex-info "invalid options" (merge loc {:syntax stx}))))))

;; -----------------------------------------------------------------------------
;; union

(defn parse-union [stx & [loc]]
  (let [loc (or loc (merge {:namespace (str *ns*)} (meta stx)))]
    (letfn [(build [name docstring specs]
              (let [attrs (cond-> {:name (keyword name)
                                   :elements (into #{} specs)}
                            docstring
                            (assoc :doc docstring))]
                (map->UnionSpec attrs)))]
      (match stx
        ([name (docstring :guard string?) & specs] :seq)
        ,(build name docstring specs)
        ([name & specs] :seq)
        ,(build name nil specs)
        :else (throw (ex-info "expecting name followed by sequence of specs"
                              (merge loc {:syntax stx})))))))

;; -----------------------------------------------------------------------------
;; enum

(defn parse-enum [stx & [loc]]
  (let [loc (or loc (merge {:namespace (str *ns*)} (meta stx)))]
    (letfn [(build [name docstring values]
              (do (when-not (symbol? name)
                    (throw (ex-info (str "enumeration name must be a symbol, given " (type name))
                                    (merge loc {:syntax stx}))))
                  (when-not (every? symbol? values)
                    (throw (ex-info "some enumeration values are not symbols"
                                    (merge loc {:syntax stx :problems (filter (complement symbol?) values)}))))
                  (when (empty? values)
                    (throw (ex-info "enumeration can't be empty"
                                    (merge loc {:syntax stx}))))
                  (let [ename (keyword name)
                        vals  (map #(keyword (str name) (str %)) values)
                        attrs (cond-> {:name ename
                                       :values (into #{} vals)}
                                docstring
                                (assoc :doc docstring))]
                    (map->EnumSpec attrs))))]
      (match stx
        ([name (docstring :guard string?) & values] :seq)
        ,(build name docstring values)
        ([name & values] :seq)
        ,(build name nil values)
        :else (throw (ex-info "expecting name followed by arbitrary number of symbols"
                              (merge loc {:syntax stx})))))))

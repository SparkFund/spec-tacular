(ns spark.sparkspec
  {:core.typed {:collect-only true}}
  (:require [n01se.syntax :refer [defsyntax]]
            [n01se.seqex :refer [recap]]
            [clojure.core.typed :as t]
            [clojure.string :refer [lower-case]]
            [spark.sparkspec.grammar :refer [defspec-rule defenum-rule]]
            [schema.core :as s]
            [schema.macros :as m]
            [spark.sparkspec.spec :refer :all])
  (:import (clojure.lang ASeq)))

;;;; TODO:
;;     - Recursive explosions ??!?!?
;;     - CRUDding resources
;;     - append grammar to docstring ;)
;;     - coerce everything to :is-a

;;;; There is no existing Java class for a primitive byte array
(def Bytes (class (byte-array [1 2])))

(defrecord SpecType [name type type-symbol coercion])

(def type-map
  (reduce
   (fn [m [n t ts c]]
     (assoc m n (map->SpecType {:name n :type t :type-symbol ts :coercion c})))
          {}
          [[:keyword clojure.lang.Keyword `clojure.lang.Keyword keyword]
           [:string String `String nil] ; str Q: Do we lean on "str" coercion?
           [:boolean Boolean `Boolean boolean]
           [:long Long `Long long]
           [:bigint java.math.BigInteger `java.math.BigInteger bigint]
           [:float Float `Float float]
           [:double Double `Double double]
           [:bigdec java.math.BigDecimal `java.math.BigDecimal bigdec]
           [:instant java.util.Date `java.util.Date nil]
           [:uuid java.util.UUID `java.util.UUID #(if (string? %) (java.util.UUID/fromString %) %)]
           [:uri java.net.URI `java.net.URI nil]
           [:bytes Bytes `Bytes nil]
           ;; :ref could maybe be datomic.db.DbId ? But it seems Datomic accepts raw integers too?
           ;; TODO: Longs don't seem to be datomic.db.DbIds
           [:ref Object `Object nil]])) 

(defn get-item [spec kw] 
  "returns the item in spec with the name kw"
  (->> spec :items (filter #(= (name (:name %)) (name kw))) first))

(defn coerce [spec kw val] 
  (let [{[cardinality typ] :type :as item} (get-item spec kw)]
    (if (and item val) ;; this is defined in the spec, and the item is non-nil
      (case cardinality
        :one ((or (-> type-map typ :coercion) identity) val)
        :many (vec (map #((or (-> type-map typ :coercion) identity) %) val)))
      val)))

;;;; Exported methods dealing with specs

(defn- resolve-fn [o]
  (if (or (keyword? o)
          (= (class o) (class (class o))))
    o (class o)))

(defmulti get-spec
  "If the given item is a Spec/EnumSpec or was defined by a
  Spec/EnumSpec, returns the Spec/EnumSpec, otherwise nil."
  resolve-fn)

(defmethod get-spec :default [_] nil)

(defmulti get-type resolve-fn)
(defmethod get-type :default [x] (get type-map x))

(defmulti get-ctor identity)
(defmulti get-lazy-ctor identity)

(defmulti get-map-ctor
  "If the keyword or class names a Spec MySpec,
   returns a map->MySpec map factory fn-name.
   return wrapped map-ctors, not the actual map->Record, to avoid CLJ-1388
   while waiting for Clojure 1.7" 
  resolve-fn)

(defmulti get-spec-class
  "returns the class of the record type representing the given spec name, 
   or nil (if it is an enum spec, it won't have a corresponding class)"
  identity)

(defmethod get-spec-class :default [_] nil)

(doseq [[k v] type-map]
  (defmethod get-spec-class k [_] (:type v)))

(defn check-component!
  "checks the key and its value against the given spec"
  [spec k v]
  (assert (some #(= k (-> % :name keyword)) (:items spec))
          (format "%s is not in the spec of %s" k (:name spec)))
  (let [{iname :name
         [cardinality typ] :type
         required? :required?
         precondition :precondition}
        (get-item spec k)
        sname (:name spec)]
    (when required?
      (assert (some? v) (format "%s is required in %s" iname sname)))
    (when (and precondition (or required? v))
      (assert (precondition v)
              (format "precondition for %s failed for %s = %s" sname iname v)))
    (when (and typ (or required? v))
      (let [;; This is a work-around for being able to define
            ;; recursive types
            spec-class (get-spec-class typ)
            is-type? (fn [v] 
                       (if (class? spec-class)
                         (instance? spec-class v)
                         (contains? (set (:elements (get-spec typ)))
                                    (:name (get-spec v)))))]
        (assert
         (case cardinality
           :many (every? is-type? v)
           :one  (is-type? v))
         (format "invalid type (%s %s) for %s in %s. value %s has class %s."
                 cardinality typ iname sname v (class v))))))
  v)

(defn check-complete! 
  "checks all fields of a record-sp (not enum), 
   throws exception if required field is missing."
  [spec sp]
  (assert (nil? (:elements spec)) "spec cannot be an enum")
  (doseq [item (:items spec)]
    (check-component! spec (:name item) (get sp (:name item))))
  sp)

(defn assoc+check
  ([m k v]
   (check-component! (get-spec m) k (k m))
   (assoc m k v))
  ([m k v & kvs]
   (reduce (fn [[k v] m] (assoc+check m k v))
           (assoc+check m k v)
           (partition 2 kvs))))

(defn has-spec?
  "Returns whether or not the given object has a spec."
  [o]
  (some? (get-spec o)))

(defn get-basis
  [o]
  (if-let [spec (get-spec o)]
    (vec (map :name (:items spec)))))

(defn basis=
  "Checks whether the basis values of the given objects are equal recursively if
  they are specs, otherwise whether the two objects are equal."
  [a b]
  (if (has-spec? a)
    (and (= (get-spec a) (get-spec b))
         (every? identity (map #(basis= (% a) (% b)) (get-basis a))))
    (= a b)))

(def core-types (into #{} (keys type-map)))

(defn primitive? [spec-name] (contains? core-types spec-name))

(defn recursiveness [{[_ t] :type}] 
  (if (primitive? t) :non-rec :rec))

;;;; Spec->form Functions

(defn- make-name [spec append-fn]
  (symbol (append-fn (name (:name spec)))))

(defn checked-lazy-access [spec atmap k v]
  "user has asked to reference a field of a lazily constructed sp
   the value of that field has not been cached yet, so we check
   that the value v adheres to the spec if we can, and return it"
  (let [item     (first (filter #(= (:name %) k) (:items spec)))
        sub-spec (second (:type item))]
    (if (primitive? sub-spec)
      (check-component! spec k v)
      ((get-lazy-ctor sub-spec) v))))

(defn- mk-record [spec]
  "defines a record type for spec, and a lazy type for spec"
  `(do (defrecord ~(symbol (str "s-" (-> spec :name name))) [])
       (deftype ~(symbol (str "l-" (-> spec :name name))) [~'atmap ~'cache]
         clojure.lang.ILookup
         (valAt ~'[this k not-found]
           (or (~'k (deref ~'cache))
               (let [val# (.valAt ~'atmap ~'k ~'not-found)
                     lsp# (checked-lazy-access ~spec ~'atmap ~'k val#)]
                 (do (swap! ~'cache assoc ~'k lsp#) lsp#))))
         (valAt [~'this ~'k]
           (.valAt ~'this ~'k nil))
         clojure.lang.IPersistentCollection
         (equiv ~'[this o]
           (.equiv ~'atmap (.atmap ~'o))))))

(defn spec->enum-type [spec]
  `(t/U ~@(map #(symbol (str *ns*) (name %)) (:elements spec))))

(defn spec->record-type [spec]
  (let [opts (for [{iname :name [cardinality sub-sp-nm] :type :as item} (:items spec)]
               (let [item-type (if (primitive? sub-sp-nm)
                                 (get-in type-map [sub-sp-nm :type-symbol])
                                 (symbol (str *ns*) (name sub-sp-nm)))]
                 [iname (case cardinality
                          :one  item-type
                          :many (list 'clojure.lang.ASeq item-type))]))]
    `(t/HMap
      ;; FIXME: This should be true but waiting on 
      ;; http://dev.clojure.org/jira/browse/CTYP-198
      :complete? false 
      :optional ~(into {} opts)))) ;; TODO: aren't there required fields?

(defn spec->type [spec]
  (if (:elements spec) (spec->enum-type spec) (spec->record-type spec)))

(defn- mk-type-alias
  "defines an core.typed alias named after the given spec's name"
  [spec]
  (let [alias (symbol (str *ns*) (name (:name spec)))
        type  (spec->type spec)]
    `(do (t/defalias ~alias ~type)
         (defmethod get-type ~(:name spec) [_#] 
           {:name ~(:name spec) :type-symbol '~alias}))))

(m/defn non-recursive-ctor
  "builds a spark type from a record, checking fields, but children
   must all be primitive, non-recursive values or already spark types"
  [map-ctor :- (s/=> s/Any {s/Keyword s/Any}),
   spec :- spark.sparkspec.spec.Spec,
   sp :- {s/Keyword s/Any}]
  (let [items (:items spec)
        defaults
        (reduce
         (fn [m {name :name dv :default-value}]
           (if (some? dv)
             (assoc m (keyword name) (if (ifn? dv) (dv) dv))
             m))
         ;; We have to write over the provided so we don't lose the type. 
         ;; It may be the only way to choose the right constructor if the spec is an enum.
         sp items)
        sp-map (reduce (fn [m k] 
                         (let [coerced (coerce spec k (get sp k))]
                           (if (some? coerced) (assoc m k coerced) m)))
                       sp
                       (keys sp))
        sp-map (merge defaults sp-map)]
    (doall (map
            (fn [i]
              (check-component! spec (:name i) (get sp-map (:name i))))
            (filter
             #(contains? sp-map (:name %))
             items)))
    (dissoc (with-meta (map-ctor sp-map) {:spec spec})
            :spec-tacular/spec)))

(m/defn recursive-ctor 
  "deep-walks a nested map structure, constructing 
   proper spark types from nested values. Any sub-sp
   that is already a Spec of the correct type is
   acceptable as well."
  [spec-name :- s/Keyword, sp :- s/Any]
  (let [spec (get-spec spec-name)]
    (if (and (instance? clojure.lang.IRecord sp)
             (if (:elements spec)
               (not (contains? (:elements spec) (:name (get-spec (class sp)))))
               ;;sp is already an instance of some specific record; has to agree.
               (not= spec (get-spec (class sp))))) 
      (throw (ex-info (str "provided wrong spec type in ctor: " (class sp) " expecting " spec-name)
                      {:provided (class sp) :expecting spec-name}))
      (let [{recs :rec non-recs :non-rec} (group-by recursiveness (:items spec))
            sub-kvs (->> recs
                         (keep (fn [{[arity sub-spec-name] :type :as item}]
                                 (let [sub-sp (get sp (:name item))]
                                   (cond
                                     (and (= arity :one) sub-sp) ; only build non-nil sub-sps
                                     ,[(:name item) (recursive-ctor sub-spec-name sub-sp)]
                                     (and (= arity :many) sub-sp)
                                     ,[(:name item) (map #(recursive-ctor sub-spec-name %) sub-sp)]
                                     :else nil)))))]
        (non-recursive-ctor (get-map-ctor spec-name) spec (into sp sub-kvs))))))

(defn- mk-checking-ctor [spec]
  "For use in macros, creates a constructor that checks
  precondititions and types."
  (let [ctor-name (make-name spec #(lower-case %))]
    `(do
       (t/ann ~ctor-name ~(if (empty? (:items spec))
                            ['-> (symbol (str *ns*) (name (:name spec)))]
                            [(symbol (str *ns*) (name (:name spec)))
                             '-> (symbol (str *ns*) (name (:name spec)))]))
       (defn ~ctor-name ~(str "deep-walks a nested map structure to construct a "
                              (name (:name spec)))
         [& [sp#]] (recursive-ctor ~(:name spec) (if (some? sp#) sp# {})))
       (defmethod get-ctor ~(:name spec) [_#] ~ctor-name)
       (defmethod get-ctor ~spec [_#] ~ctor-name))))

(defn- mk-lazy-ctor [spec]
  `(let [l-ctor# (fn [atmap#]
                   (when-not (instance? clojure.lang.IPersistentCollection atmap#)
                     (throw (ex-info "cannot construct " (name (:name ~spec)) " from " atmap#)))
                   (~(symbol (str *ns*) (str "l-" (name (:name spec)) ".")) atmap# (atom {})))]
     (do (defmethod get-lazy-ctor ~(:name spec) [_#] l-ctor#)
         (defmethod get-lazy-ctor ~spec [_#] l-ctor#))))

(defn eval-default-values [spec]
  "like eval, but eval won't go into records types like Spec or Item.
   we need to eval because we want the Spec record at macro time, but want to defer
   evalling the fn forms for eg :default-value
  (we could eval all nested values, not just :default-value, esp
   if we want to use non-literals in other fields in the future.)"
  (update-in spec [:items]
             (fn [items] (doall (map (fn [item] (update-in item [:default-value] eval)) items)))))

(defn- mk-get-spec [spec]
  `(let [spec-sym# (eval-default-values ~spec)] ; want eval to happen after this is expanded, so the two methods share the resulting value.
     (defmethod get-spec ~(:name spec) [_#] spec-sym#)
     (defmethod get-spec ~(symbol (str "s-" (name (:name spec)))) [_#] spec-sym#)))

(defn- mk-get-spec-class [spec]
  (let [class-name (symbol (str "s-" (name (:name spec))))]
    `(defmethod get-spec-class ~(:name spec) [_#] ~class-name)))

(defn- mk-get-map-ctor [spec]
  (let [builtin-sym (symbol (str "map->s-" (name (:name spec))))
        fac-sym     (symbol (str builtin-sym "-fixed"))]
    `(do
       (defn ~fac-sym [o#] (~builtin-sym (into {} o#))) ; avoid CLJ-1388 until Clojure 1.7 comes out.
       (defmethod get-map-ctor ~(:name spec) [_#] ~fac-sym)
       (defmethod get-map-ctor ~(symbol (str "s-" (name (:name spec)))) [_#] ~fac-sym))))

(defn- mk-enum-get-spec [spec]
  `(defmethod get-spec ~(:name spec) [_#] ~spec))

(defn- mk-enum-get-map-ctor [spec]
  (let [fac-sym (symbol (str "map->s-" (name (:name spec))))]
    `(do
       ;; the "map ctor" for an enum means it's arg needs to 
       ;; be a tagged map or a record type of one of the enum's ctors.
       (defn ~fac-sym [o#] 
         (let [subspec-name# (if (:spec-tacular/spec o#)
                               (:spec-tacular/spec o#)
                               (:name (get-spec o#)))]
           (assert (contains? ~(:elements spec) subspec-name#) 
                   (str subspec-name#" is not an element of "~(:name spec)))
           ((get-map-ctor subspec-name#) o#)))
       (defmethod get-map-ctor ~(:name spec) [_#] ~fac-sym)
       (defmethod get-map-ctor ~(symbol (name (:name spec))) [_#] ~fac-sym))))

(defn- mk-huh [spec]
  (let [huh (make-name spec #(str (lower-case %) "?"))
        strict-class (symbol (str "s-" (name (:name spec))))
        lazy-class   (symbol (str "l-" (name (:name spec))))]
    `(defn ~huh [o#] 
       (or (instance? ~strict-class o#)
           (instance? ~lazy-class o#)))))

(defn- mk-enum-huh [spec]
  (let [huh (symbol (str (-> spec :name name lower-case) "?"))]
    `(defn ~huh [o#] (contains? ~(:elements spec) (:name (get-spec o#))))))

;;;; defspec Macro

(defsyntax defspec
  (recap defspec-rule
         (fn [s]
           `(do
              ~(mk-type-alias s)
              ~(mk-record s)
              ~(mk-get-map-ctor s)
              ~(mk-huh s)
              ~(mk-checking-ctor s)
              ~(mk-lazy-ctor s)
              ~(mk-get-spec s)
              ~(mk-get-spec-class s)))))

(defsyntax defenum
  (recap defenum-rule
         (fn [s]
           `(do
              (def ~(-> s :name name symbol) ~(:name s))
              ~(mk-type-alias s)
              ~(mk-enum-get-map-ctor s)
              ~(mk-enum-get-spec s)
              ~(mk-enum-huh s)))))

(defn inspect-spec
  "Produces a json-friendly nested-map representation of a spec.
   Nesting depth is bounded by the mask."
  [spec-name mask & [resource-prefix-str schema-prefix-str]]
  (let [spec (get-spec spec-name)
        spec-type (if (:elements spec)
                    :enum
                    (if (primitive? spec-name)
                      :primitive
                      :record))
        resource-kv (when (and resource-prefix-str (= :record spec-type))
                      {:resource-url (str resource-prefix-str "/"
                                      (lower-case (name spec-name)))})
        inspect-kv (when (and schema-prefix-str (#{:record :enum} spec-type))
                     {:schema-url (str schema-prefix-str "/"
                                          (lower-case (name spec-name)))})]
    (when mask
      (merge
       {:spec-name spec-name
        :spec-type spec-type}
       inspect-kv
       resource-kv
       (if (map? mask)
         (if (= :enum spec-type)
           {:expanded true
            :enum-elements (->> (:elements spec)
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






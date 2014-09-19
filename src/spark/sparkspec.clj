(ns spark.sparkspec
  (:require [n01se.syntax :refer [defsyntax]]
            [n01se.seqex :refer [recap]]
            [clojure.string :refer [lower-case]]
            [spark.sparkspec.grammar :refer [defspec-rule defenum-rule]]
            [schema.core :as s]
            [schema.macros :as m]
            [spark.sparkspec.spec :refer :all]))

;;;; TODO:
;;     - Recursive explosions ??!?!?
;;     - Datomic integration
;;         * Spec-checking
;;         * db->sp        
;;         * sp->db
;;     - CRUDding resources
;;     - append grammar to docstring ;)
;;     - coerce everything to :is-a
;;     - add get-type 

;;;; There is no existing Java class for a primitive byte array
(def ^:private Bytes (class (byte-array [1 2])))

(defrecord SpecType [name type coercion])

(def type-map
  (reduce
   (fn [m [n t c]]
     (assoc m n (map->SpecType {:name n :type t :coercion c})))
          {}
          [[:keyword clojure.lang.Keyword keyword]
           [:string String nil] ; str Q: Do we lean on "str" coercion?
           [:boolean Boolean boolean]
           [:long Long long]
           [:bigint java.math.BigInteger bigint]
           [:float Float float]
           [:double Double double]
           [:bigdec java.math.BigDecimal bigdec]
           [:instant java.util.Date nil]
           [:uuid java.util.UUID #(if (string? %) (java.util.UUID/fromString %) %)]
           [:uri java.net.URI nil]
           [:bytes Bytes nil]
           [:ref Object nil]])) ; :ref could maybe be datomic.db.DbId ? But it seems Datomic accepts raw integers too?

;; TODO: reconcile this notion with type-map, also with db-type->spec-type
(def core-types #{:long :double :instant :bigint :float :string :keyword :bigdec :bytes :uri :uuid :boolean :ref})

(defn get-item [spec kw] 
  (->> spec :items (filter #(= (keyword (:name %)) kw)) first))

(defn coerce [spec kw val] 
  (let [{[cardinality typ] :type :as item} (get-item spec kw)]
    (if (and item val) ;; this is defined in the spec, and the item is non-nil
      (case cardinality
        :one ((or (-> type-map typ :coercion) identity) val)
        :many (vec (map #((or (-> type-map typ :coercion) identity) %) val))
        (throw (ex-info (str "ERROR: Not a recognized cardinality type, expected :one or :many - got: " cardinality)
                        {:item item :cardinality cardinality :keyword kw :val val  :spec spec})))
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

(defmulti get-ctor identity)

(defmulti get-map-ctor
  "If the keyword or class names a Spec MySpec,
   returns the map->MySpec map factory fn"
  resolve-fn)

(defmulti get-spec-class 
  "returns the class of the record type representing the given spec name, 
   or nil (if it is an enum spec, it won't have a corresponding class)"
  identity)

(defmethod get-spec-class :default [_] nil)

(doall (map (fn [[k v]] 
              (defmethod get-spec-class k [_] (:type v)))
            type-map))

(defn check-component!
  "Checks the key and its value against the spec.
   ret true if check passes, o/w throws exception."
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
      (assert (-> v nil? not)
              (format "%s is required in %s" iname sname)))
    (when (and precondition (or required? v))
      (assert (precondition v)
              (format "precondition for %s failed for %s = %s"
                      sname iname v)))
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
           :many (every? (fn [i] (is-type? i)) v)
           :one (is-type? v) (throw
                              (ex-info "unrecognized cardinality" {:card cardinality})))
         (format "invalid type (%s %s) for %s in %s. value %s has class %s."
                 cardinality typ iname sname v (class v))))))
  true)

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

(defn primitive? [spec-name] (contains? core-types spec-name))

(defn recursiveness [{[_ t] :type}] (if (primitive? t)
                                      :non-rec
                                      :rec))

;;;; Spec->form Functions

(defn make-name ^:private [spec append-fn]
  (symbol (append-fn (name (:name spec)))))

(defn mk-record
  "For use in macros, translates a Spec into a record type."
  ^:private
  [spec]
  `(defrecord ~(symbol (-> spec :name name))
       ~(vec (map #(-> % :name name symbol) (:items spec)))))

(m/defn non-recursive-ctor
  "builds a spark type from a record, checking fields, but children
   must all be primitive, non-recursive values or already spark types"
  [recfn :- (s/=> s/Any {s/Keyword s/Any}) spec :- spark.sparkspec.spec.Spec sp :- {s/Keyword s/Any}]
  (let [items (:items spec)
        defaults
        (reduce
         (fn [m {name :name dv :default-value}]
           (if (some? dv)
             (assoc m (keyword name) (if (ifn? dv) (dv) dv))
             m))
         sp ; We have to write over the provided so we don't lose the type. It may be the only way to choose the right constructor if the spec is an enum.
         items)
        sp-map (reduce (fn [m k] 
                         (let [coerced (coerce spec k (get sp k))]
                           (if (some? coerced)
                             (assoc m k coerced)
                             m)))
                       sp
                       (keys sp))
        sp-map (merge defaults sp-map)]
    (doall (map
            (fn [i]
              (check-component! spec (:name i) (get sp-map (:name i))))
            items))
    (dissoc (with-meta (recfn sp-map) {:spec spec})
            :spec-tacular/spec)))

(m/defn recursive-ctor 
  "deep-walks a nested map structure, constructing 
   proper spark types from nested values. Any sub-sp
   that is already a Spec of the correct type is
   acceptable as well."
  [spec-name :- s/Keyword sp :- s/Any]
  (let [spec (get-spec spec-name)
        {recs :rec non-recs :non-rec} (group-by recursiveness (:items spec))
        sub-kvs (->> recs
                     (map (fn [{[arity sub-spec-name] :type :as item}]
                            (let [sub-sp (get sp (:name item))]
                              (cond
                               (and (= arity :one) (some? sub-sp)) ; only build non-nil sub-sps
                               , [(:name item) (recursive-ctor sub-spec-name sub-sp)]
                               (and (= arity :many) (some? sub-sp))
                               , [(:name item) (map #(recursive-ctor sub-spec-name %) sub-sp)]
                               :else nil))))
                     (filter some?))]
    (if (and (instance? clojure.lang.IRecord sp)
             (if (:elements spec)
               (not (contains? (:elements spec) (:name (get-spec (class sp)))))
               (not= spec (get-spec (class sp))))) ;sp is already an instance of some specific record; has to agree.
      (throw (ex-info (str "provided wrong spec type in ctor: " (class sp) " expecting " spec-name) 
                      {:provided (class sp) :expecting spec-name}))
      (non-recursive-ctor (get-map-ctor spec-name) spec (into sp sub-kvs)))))

(defn mk-checking-ctor
  "For use in macros, creates a constructor that checks
  precondititions and types."
  ^:private
  [spec]
  (let [ctor-name (make-name spec #(lower-case %))]
    `(do
       (defn ~ctor-name ~(str "deep-walks a nested map structure to construct a "
                              (name (:name spec)))
         [& [sp#]] (recursive-ctor ~(:name spec) (if (some? sp#) sp# {})))
       (defmethod get-ctor ~(:name spec) [_#] ~ctor-name)
       (defmethod get-ctor ~spec [_#] ~ctor-name))))

(defn eval-default-values
  "like eval, but eval won't go into records types like Spec or Item.
   we need to eval because we want the Spec record at macro time, but want to defer
   evalling the fn forms for eg :default-value
  (we could eval all nested values, not just :default-value, esp
   if we want to use non-literals in other fields in the future.)"
  [spec]
  (update-in spec [:items]
             (fn [items]
               (doall (map (fn [item] (update-in item [:default-value] eval))
                           items)))))

(defn mk-get-spec 
  ^:private
  [spec]
  `(let [spec-sym# (eval-default-values ~spec)] ; want eval to happen after this is expanded, so the two methods share the resulting value.
     (defmethod get-spec ~(:name spec) [_#] spec-sym#)
     (defmethod get-spec ~(symbol (name (:name spec))) [_#] spec-sym#)))

(defn mk-get-spec-class
  ^:private
  [spec]
  `(defmethod get-spec-class ~(:name spec) [_#] ~(symbol (name (:name spec)))))

(defn mk-get-map-ctor
  ^:private
  [spec]
  (let [fac-sym (symbol (str "map->" (name (:name spec))))]
    `(do
       (defmethod get-map-ctor ~(:name spec) [_#] ~fac-sym)
       (defmethod get-map-ctor ~(symbol (name (:name spec))) [_#] ~fac-sym))))

(defn mk-enum-get-spec
  ^:private
  [spec]
  `(defmethod get-spec ~(:name spec) [_#] ~spec))

(defn mk-enum-get-map-ctor
  ^:private
  [spec]
  (let [fac-sym (symbol (str "map->" (name (:name spec))))]
    `(do
       (defn ~fac-sym [o#] ; the "map ctor" for an enum means it's arg needs to be a tagged map or a record type of one of the enum's ctors.
         (let [subspec-name# (if (:spec-tacular/spec o#)
                               (:spec-tacular/spec o#)
                               (:name (get-spec o#)))]
           (assert (contains? ~(:elements spec) subspec-name#) (str subspec-name#" is not an element of "~(:name spec)))
           ((get-map-ctor subspec-name#) o#)))
       (defmethod get-map-ctor ~(:name spec) [_#] ~fac-sym)
       (defmethod get-map-ctor ~(symbol (name (:name spec))) [_#] ~fac-sym))))

(defn mk-huh
  ^:private
  [spec]
  (let [huh (make-name spec #(str (lower-case %) "?"))]
    `(defn ~huh [o#] (instance? ~(symbol (name (:name spec))) o#))))

(defn mk-enum-huh
  ^:private
  [spec]
  (let [huh (symbol (str (-> spec :name name lower-case) "?"))]
    `(defn ~huh [o#] (contains? ~(:elements spec) (:name (get-spec o#))))))

;;;; defspec Macro

(defsyntax defspec
  (recap defspec-rule
         (fn [s]
           `(do
              ~(mk-record s)
              ~(mk-get-map-ctor s)
              ~(mk-huh s)
              ~(mk-checking-ctor s)
              ~(mk-get-spec s)
              ~(mk-get-spec-class s)))))

(defsyntax defenum
  (recap defenum-rule
         (fn [s]
           `(do
              (def ~(-> s :name name symbol) ~(:name s))
              ~(mk-enum-get-map-ctor s)
              ~(mk-enum-get-spec s)
              ~(mk-enum-huh s)))))

(defn inspect-spec
  "Produces a json-friendly nested-map representation of a spec.
   Nesting depth is bounded by the mask."
  [spec-name mask]
  (if (map? mask)
    (let [spec (get-spec spec-name)]
      (if (:elements spec)
        {:spec-name spec-name
         :enum-elements (->> (:elements spec)
                             (map #(inspect-spec % (get mask %)))
                             (filter some?))}
        (let [items
              , (for [{iname :name [cardinality type] :type :as item} (:items spec)
                      :when (iname mask)]
                  {iname {:many (= cardinality :many)
                          :required (if (:required item) true false)
;                     :identity (:identity item) ; not meaningful for front-end?
;                     :unique (:unique item)
;                     :optional (:optional item)
                          :spec (inspect-spec type (iname mask))}})]
          {:spec-name spec-name
           :items (or (reduce merge items) [])})))
    (when mask
      (if (primitive? spec-name)
        {:spec-name spec-name
         :primitive-type true}
        {:spec-name :ref
         :ref spec-name}))))

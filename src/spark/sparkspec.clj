(ns spark.sparkspec
  {:core.typed {:collect-only true}}
  (:require [clojure.core.typed :as t]
            [clojure.string :refer [lower-case]]
            [spark.sparkspec.grammar :refer [parse-spec parse-enum]]
            [spark.sparkspec.spec :refer :all])
  (:import (clojure.lang ASeq)))

;;;; TODO:
;;     - Recursive explosions ??!?!?
;;     - CRUDding resources
;;     - append grammar to docstring ;)
;;     - coerce everything to :is-a

(t/defalias SpecInstance (t/Map t/Any t/Any))
(t/defalias SpecName t/Keyword)
(t/defalias Item (t/HMap :mandatory {:name t/Keyword
                                     :type (t/HVec [(t/U (t/Val :one) (t/Val :many))
                                                    SpecName])}))

(t/defalias SpecT (t/HMap :mandatory {:name SpecName
                                      :items (t/Vec Item)}
                          :optional {:elements (t/Map t/Keyword SpecT)}))

(t/ann ^:no-check spark.sparkspec/primitive? [t/Keyword -> t/Bool])
(t/ann ^:no-check spark.sparkspec/get-spec [(t/U SpecInstance t/Keyword) -> SpecT])
(t/ann ^:no-check spark.sparkspec/get-ctor [t/Keyword -> [(t/Map t/Any t/Any) -> SpecInstance]])
(t/ann ^:no-check spark.sparkspec/get-type [(t/U SpecInstance t/Keyword) -> (t/Map t/Keyword t/Any)])

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

(defmethod get-spec :default [_] nil) ;; TODO

(defmulti get-type resolve-fn)
(defmethod get-type :default [x] (get type-map x))

(defmulti get-ctor identity)

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

(defmulti get-database-coersion
  "returns the coersion function that maps an object of a known database
   type (like datomic.query.EntityMap) to a map that can be used by recursive-ctor"
  (fn [o] (type o)))
(defmethod get-database-coersion :default [_] nil)

(doseq [[k v] type-map]
  (defmethod get-spec-class k [_] (:type v)))

(defn check-component!
  "checks the key and its value against the given spec"
  [spec k v]
  (when-not (some #(= k (-> % :name keyword)) (:items spec))
    (throw (ex-info (format "%s is not in the spec of %s" k (:name spec))
                    {:keyword k :value v :spec spec})))
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
        (when-not (case cardinality
                    :many (every? is-type? v)
                    :one  (is-type? v))
          (throw (ex-info "invalid type"
                          {:expected-type typ
                           :actual-type (class v)
                           :sub-spec-name iname
                           :spec-name sname}))))))
  true)

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

;; SpecInstances represent entities:
;; -- when wrap is :local, wrapping a local entity (not on the db)
;; -- when wrap is :db, wrapping an entity on the database
(defn- mk-record [spec]
  (let [gs (gensym), class-name (symbol (str "i_" (-> spec :name name)))
        {rec :rec non-rec :non-rec} (group-by recursiveness (:items spec))
        non-rec-kws (map :name non-rec)]
    `(do (deftype ~class-name [~'atmap]
           clojure.lang.IObj 
           ;; user doesn't get the map out again so it's fine to put the meta on it
           ;; stipulation -- meta of the SpecInstance is initially inherited from meta of the map
           (meta [this#] (meta ~'atmap))
           (withMeta [this# ~gs] (new ~class-name (with-meta ~'atmap ~gs)))
           clojure.lang.ILookup
           (valAt [this# k# not-found#]
             (let [val# (.valAt ~'atmap k# not-found#)]
               (cond
                 (identical? val# not-found#) val#
                 (some #(= % k#) [~@non-rec-kws]) val#
                 :else (if-let [db-fn# (get-database-coersion val#)]
                         (let [{[arity type] :type} (get-item ~spec k#)]
                           (recursive-ctor ~type (db-fn# val#)))
                         val#))))
           (valAt [~'this ~'k]
             (.valAt ~'this ~'k nil))
           clojure.lang.IPersistentMap
           #_(equiv ~'[this o]
               (and (instance? ~class-name ~'o)
                    (.equiv ~'atmap (.atmap ~'o))))
           (seq [~gs]
             (seq [~@(for [{iname :name :as item} (:items spec)]
                       `(new clojure.lang.MapEntry ~iname (~iname ~gs)))]))
           (assoc [this# k# v#] ;; TODO
             (new ~class-name (assoc ~'atmap k# v#)))
           (containsKey [this# k#] 
             (not (identical? this# (.valAt this# k# this#))))
           #_(iterator [this#]
               (.iterator (apply hash-map (mapcat #(list (:name %) ((:name %) ~'atmap)) 
                                                  (:items ~spec)))))
           #_(cons [this# e#]
               (assoc this# (first e#) (second e#)))
           (without [this# k#]
             (new ~class-name (dissoc ~'atmap k#))))
         
         (defmethod print-method ~class-name [v# ^java.io.Writer w#]
           (write-spec-instance ~spec v# w#)))))

(defn write-spec-instance [spec le ^java.io.Writer w]
  (letfn [(write-link [v w]
            (do (.write w "<link ")
                (if-let [eid (get-in v [:db-ref :eid])]
                  (.write w (str eid)))
                (.write w ">")))
          (write-value [v link? w]
            (if link? (write-link v w) (print-method v w)))
          (write-item [{iname :name link? :link? [c t] :type :as item} w]
            (if-let [v (iname le)]
              (do (.write w (str ":" (name iname) " "))
                  (case c
                    :one (write-value v link? w)
                    :many (do (.write w "[")
                              (doseq [sub-v v :when v]
                                (do (write-value sub-v link? w) (.write w ",")))
                              (.write w "]")))
                  (.write w ","))))]
    (do (.write w (str "#<" (name (:name spec)) "{"))
        (if-let [eid (get-in le [:db-ref :eid])]
          (.write w (str ":eid " eid ",")))
        (doseq [item (:items spec)]
          (write-item item w))
        (.write w "}>"))))

(defn spec->enum-type [spec]
  `(t/U ~@(map #(symbol (str *ns*) (name %)) (:elements spec))))

(defn spec->record-type [spec]
  (let [opts (for [{iname :name required? :required? [cardinality sub-sp-nm] :type :as item} (:items spec)]
               (let [item-type (if (primitive? sub-sp-nm)
                                 (get-in type-map [sub-sp-nm :type-symbol])
                                 (symbol (str *ns*) (name sub-sp-nm)))]
                 [iname (let [t (case cardinality
                                  :one  item-type
                                  :many (list `t/SequentialSeqable item-type))]
                          (if required? t (list `t/Option t)))]))]
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

(defn non-recursive-ctor
  "builds an instance from another, checking fields, but children
   must all be primitive, non-recursive values or already spark types"
  [map-ctor spec sp]
  (prn map-ctor (:name spec) sp)
  (let [items (:items spec)
        defaults (reduce (fn [m {name :name dv :default-value}]
                           (if (some? dv) (assoc m (keyword name) (if (ifn? dv) (dv) dv)) m))
                         sp items)
        sp-map   (reduce (fn [m k]
                           (let [coerced (coerce spec k (get sp k))]
                             (if (some? coerced) (assoc m k coerced) m)))
                         defaults (keys sp))
        actual-kws   (keys sp)
        required-kws (keep #(if (:required? %) (:name %)) items)]
    (do (doseq [kw (concat required-kws actual-kws)]
          (check-component! spec kw (get sp-map kw)))
        (map-ctor sp-map))))

(defn recursive-ctor 
  "walks a nested map structure, constructing proper instances from nested values.
   Any sub-sp that is already a SpecInstance of the correct type is acceptable as well."
  [spec-name sp]
  (prn spec-name sp)
  (letfn [(build-rec-item [{iname :name [arity sub-spec-name] :type link? :link?} sub-sp]
            (letfn [(rec [sub] (->> (if link? sub (dissoc sub :db-ref))
                                    (recursive-ctor sub-spec-name)))]
              [iname (case arity :one (rec sub-sp) :many (map rec sub-sp))]))]
    (if (if-let [spec-class (get-spec-class spec-name)]
          (instance? spec-class sp))
      sp
      (let [spec (get-spec spec-name)
            spec (if (:elements spec)
                   (if-let [sub-spec (get-spec sp)]
                     (and (some #(= (:name sub-spec) %) (:elements spec)) sub-spec))
                   spec)
            _ (when-not spec (throw (ex-info (str "cannot create " (name spec-name)) {:sp sp})))
            {recs :rec non-recs :non-rec} (group-by recursiveness (:items spec))
            sub-kvs (->> recs (keep (fn [{iname :name :as item}]
                                      (if-let [sub-sp (get sp iname)]
                                        (build-rec-item item sub-sp)))))]
        (non-recursive-ctor (get-map-ctor spec-name) spec (into sp sub-kvs))))))

(defn- mk-checking-ctor [spec]
  "For use in macros, creates a constructor that checks
  precondititions and types."
  (let [ctor-name (make-name spec #(lower-case %))]
    `(do
       (t/ann ~(with-meta ctor-name (assoc (meta ctor-name) :no-check true))
              ~(if (empty? (:items spec))
                 ['-> (symbol (name (:name spec)))]
                 [(symbol (name (:name spec))) '-> (symbol (name (:name spec)))]))
       (defn ~(with-meta ctor-name (assoc (meta ctor-name) :spec-tacular/spec (:name spec)))
         ~(str "deep-walks a nested map structure to construct a "
               (name (:name spec)))
         [& [sp#]] (recursive-ctor ~(:name spec) (if (some? sp#) sp# {})))
       (defmethod get-ctor ~(:name spec) [_#] ~ctor-name)
       (defmethod get-ctor ~spec [_#] ~ctor-name))))

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
     (defmethod get-spec ~(symbol (str "i_" (name (:name spec)))) [_#] spec-sym#)))

(defn- mk-get-spec-class [spec]
  (let [class-name (symbol (str "i_" (name (:name spec))))]
    `(defmethod get-spec-class ~(:name spec) [_#] ~class-name)))

(defn- mk-get-map-ctor [spec]
  (let [class-ctor (str "i_" (name (:name spec)))
        fac-sym    (symbol (str class-ctor "-fixed"))]
    `(do
       (defn ~fac-sym [o#] (~(symbol (str class-ctor ".")) o#))
       (defmethod get-map-ctor ~(:name spec) [_#] ~fac-sym)
       (defmethod get-map-ctor ~(symbol (str "i_" (name (:name spec)))) [_#] ~fac-sym))))

(defn- mk-enum-get-spec [spec]
  `(defmethod get-spec ~(:name spec) [_#] ~spec))

(defn- mk-enum-get-map-ctor [spec]
  (let [fac-sym (symbol (str "map->i_" (name (:name spec)) "-fixed"))]
    `(do
       ;; the "map ctor" for an enum means it's arg needs to 
       ;; be a tagged map or a record type of one of the enum's ctors.
       (defn ~(with-meta fac-sym (assoc (meta fac-sym) :spec-tacular/spec (:name spec))) [o#]
         (let [subspec-name# (or (:spec-tacular/spec o#)
                                 (:name (get-spec o#)))]
           (assert subspec-name#
                   (str "could not find spec for "o#))
           (assert (contains? ~(:elements spec) subspec-name#) 
                   (str subspec-name#" is not an element of "~(:name spec)))
           ((get-map-ctor subspec-name#) o#)))
       (defmethod get-map-ctor ~(:name spec) [_#] ~fac-sym)
       (defmethod get-map-ctor ~(symbol (name (:name spec))) [_#] ~fac-sym))))

(defn- mk-huh [spec]
  (let [huh (make-name spec #(str (lower-case %) "?"))
        strict-class (symbol (str (namespace-munge *ns*) ".i_" (name (:name spec))))]
    `(defn ~huh [o#] 
       (instance? ~strict-class o#))))

(defn- mk-enum-huh [spec]
  (let [huh (symbol (str (-> spec :name name lower-case) "?"))]
    `(defn ~huh [o#] (contains? ~(:elements spec) (:name (get-spec o#))))))

;;;; defspec Macro

(defmacro defspec [& stx]
  (let [s (parse-spec stx)]
    `(do
       ~(mk-type-alias s)
       ~(mk-record s)
       ~(mk-get-map-ctor s)
       ~(mk-huh s)
       ~(mk-checking-ctor s)
       ~(mk-get-spec s)
       ~(mk-get-spec-class s))))

(defmacro defenum [& stx]
  (let [s (parse-enum stx)]
    `(do
       ~(let [spec-name (:name s)
              spec-sym  (-> s :name name symbol)]
          `(def ~(with-meta spec-sym 
                   (assoc (meta spec-sym) :spec-tacular/spec spec-name))
             ~spec-name))
       ~(mk-type-alias s)
       ~(mk-enum-get-map-ctor s)
       ~(mk-enum-get-spec s)
       ~(mk-enum-huh s))))

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

(t/ann ^:no-check namespace->specs [clojure.lang.Namespace -> (t/Seq SpecT)])
(t/defn namespace->specs 
  [namespace :- clojure.lang.Namespace] :- (t/Seq SpecT)
  (->> (ns-publics namespace)
       (map (fn [[sym v]] 
              (some-> (meta v) :spec-tacular/spec get-spec)))
       (filter identity)
       (into #{})))

(ns spark.spec-tacular
  {:doc "A database-agnostic DSL for data specifications"
   :core.typed {:collect-only true}}
  (:require [clojure.core.typed :as t]
            [clojure.string :refer [lower-case join]]
            [clojure.data :as data]
            [clojure.walk :as walk]
            [clojure.pprint :as pp]
            [clj-time.core :as time]
            [spark.spec-tacular.grammar :refer :all]
            [spark.spec-tacular.spec :refer :all])
  (:import spark.spec_tacular.spec.Spec
           spark.spec_tacular.spec.UnionSpec
           spark.spec_tacular.spec.EnumSpec
           spark.spec_tacular.spec.SpecType))

;; -----------------------------------------------------------------------------
;; types

(t/defalias SpecInstance
  "The broadest type for a spec instance, but it is
  preferable to use an alias defined via [[defspec]]."
  (t/Map t/Keyword t/Any))

(t/defalias SpecName
  "The type of the `:name` field of a spec"
  t/Keyword)

(t/defalias Item
  "The type of a field in a spec"
  '{:name t/Keyword
    :type '[(t/U ':one ':many) SpecName]})

(t/defalias SpecT
  "The type of a spec"
  (t/U '{:name SpecName :elements (t/Seqable SpecName)}
       '{:name SpecName :items (t/Seqable Item)}
       '{:name SpecName :values (t/Seqable t/Keyword)}))

;; -----------------------------------------------------------------------------
;; multimethods

(defn- resolve-fn [o & rest]
  (cond
    (:spec-tacular/spec o) (:spec-tacular/spec o)
    (or (instance? Spec o)
        (instance? EnumSpec o)
        (instance? UnionSpec o)) (:name o)
    (or (keyword? o) (= (class o) (class (class o)))) o
    :else (class o)))

(t/ann ^:no-check get-spec [t/Any -> (t/Option SpecT)])
(defmulti get-spec
  "Acts like the identity function if sent an actual spec object.

  Otherwise, if there is a spec (defined
  with [[defspec]], [[defunion]], or [[defenum]]) such that:

  * the spec has the `:name` denoted by the argument, when given a
  keyword;
  * the spec instance is an instance of that spec, when given an
  actual spec instance;

  then returns that spec, otherwise returns `nil`."

  resolve-fn)
(defmethod get-spec :default [& _] nil)

(t/ann ^:no-check get-type [(t/U SpecInstance t/Keyword) -> SpecType])
(defmulti get-type
  "Returns a [[SpecType]] record containing the `name` of the type,
  the class `type`, a symbol `type-symbol` that would eval to the
  `type` (useful for macros), and possibly a `coercion` function."
  resolve-fn)
(defmethod get-type :default [x] (get type-map x))

(t/ann ^:no-check get-ctor [t/Keyword -> [t/Any -> SpecInstance]])
(defmulti ^:no-doc get-ctor
  "Returns the ctor for the given spec name."
  identity)

(defmulti ^:no-doc get-map-ctor
  "Returns a map-><SpecName> map factory for the given spec name."
  identity)

(defmulti ^:no-doc get-spec-class
  "Returns the class of the record type representing the given spec name,
   or nil (if it is an union spec, it won't have a corresponding class)"
  identity)
(defmethod get-spec-class :default [_] nil)

(defmulti database-coercion
  "Returns the coercion function that maps an object of a known database
  type (currently `datomic.query.EntityMap`) to a map that can be used
  in a spec constructor.  Defaults to `nil` if the object does not come
  from a known database type."
  (fn [o] (type o)))
(defmethod database-coercion :default [_] nil)

(doseq [[k v] type-map]
  (defmethod get-spec-class k [_] (:type v)))

;; -----------------------------------------------------------------------------
;; helpers

(t/ann ^:no-check get-item [SpecT t/Keyword -> Item])
(defn ^:no-doc get-item
  "Returns the [[Item]] in `spec` with the name `kw`"
  [spec kw]
  (assert (keyword? kw) (str "not a keyword: " kw))
  (->> spec :items (some #(when (-> % :name name (= (name kw))) %))))

(t/ann ^:no-check primitive? [t/Keyword -> t/Bool])
(defn primitive?
  "Returns `true` if the given spec name is primitive (i.e. is part of
  the base type environment and not defined as a spec), `false`
  otherwise.  See [[defspec]] for a list of primitive type keywords.

  Note that all specs defined by `defenum` are considered primitive,
  since their values are all keywords."
  [spec-name]
  (boolean (or (contains? core-types spec-name)
               (instance? EnumSpec (get-spec spec-name)))))

;; Item -> (t/U (t/Val :non-rec) (t/Val :rec))
(t/ann ^:no-check recursiveness [Item -> (t/U ':non-rec ':rec)])
(defn recursiveness
  "Returns `:non-rec` if the given item is [[primitive?]], `:rec` otherwise."
  [{[_ t] :type}]
  (if (primitive? t) :non-rec :rec)) ;; TODO

(defn coerce
  "Coerces `val` into something that could be put in the `kw` field of
  `spec`.  Returns `nil` only if the value is coerced to `nil`; i.e.
  if `val` does not have a `coercion` the `val` is returned unharmed."
  [spec kw val]
  (let [{[cardinality typ] :type :as item} (get-item spec kw)]
    (if (and item (some? val)) ;; this is defined in the spec, and the item is non-nil
      (case cardinality
        :one ((or (-> type-map typ :coercion) identity) val)
        :many (let [l (map #((or (-> type-map typ :coercion) identity) %) val)]
                (cond (empty? l) nil, (primitive? typ) (set l), :else l)))
      val)))

(t/ann ^:no-check has-spec? [t/Any -> t/Bool])
(defn has-spec?
  "Returns whether or not the given object has a spec."
  [o]
  (some? (get-spec o)))

(t/ann ^:no-check namespace->specs [clojure.lang.Namespace -> (t/Seq SpecT)])
(defn namespace->specs
  "Returns a sequence containing every spec in the given namespace."
  [namespace]
  (->> (ns-publics namespace)
       (map (fn [[sym v]]
              (some-> (meta v)
                      (#(or (:spec-tacular/spec %)
                            (:spec-tacular/union %)
                            (:spec-tacular/enum %)))
                      get-spec)))
       (filter identity)
       (into #{})))

;; -----------------------------------------------------------------------------
;; verification

(defn check-component!
  "Checks the field name `k` and its value `v` against the given spec,
  errors if `v` is not of the correct type.  Returns `true` otherwise."
  [spec k v]
  (let [{iname :name [cardinality typ] :type required? :required? precondition :precondition :as item}
        ,(get-item spec k)
        sname (:name spec)]
    (when-not item
      (throw (ex-info (format "%s is not in the spec of %s" k (:name spec))
                      {:keyword k :value v :spec spec})))
    (when (and required? (not (some? v)))
      (throw (ex-info "missing required field" {:field iname :spec sname})))
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
                          {:spec spec
                           :field iname
                           :expected-type typ
                           :actual-type (class v)}))))))
  true)

;; -----------------------------------------------------------------------------
;; spec instances (have fields .atmap, .cache, and .db-hash)

(defn- mk-record
  "Returns syntax that defines a `deftype` for the given `spec` and
  then some print dispatch functions."
  [spec]
  (let [gs (gensym)
        class-name (spec->class spec)
        {:keys [non-rec]} (group-by recursiveness (:items spec))
        non-rec-kws (cons :db-ref (map :name non-rec))]
    `(do (deftype ~class-name [~'atmap ~'cache ~'db-hash]
           clojure.lang.IRecord
           clojure.lang.IObj
           ;; user doesn't get the map out again so it's fine to put the meta on it
           ;; stipulation -- meta of the SpecInstance is initially inherited from meta of the map
           (meta [this#]
             (meta ~'atmap))
           (withMeta [this# meta#]
             (new ~class-name (with-meta ~'atmap meta#) (atom {}) ~'db-hash))
           clojure.lang.ILookup
           (valAt [this# k# not-found#]
             (or (k# (deref ~'cache))
                 (let [val# (.valAt ~'atmap k# not-found#)]
                   (cond
                     (identical? val# not-found#) val#
                     (contains? #{~@non-rec-kws} k#) val#
                     (not val#) val#
                     :else (let [res# (let [{[arity# type#] :type :as item#} (get-item ~spec k#)]
                                        (case arity#
                                          :one (recursive-ctor type# val#)
                                          :many (when-not (empty? val#)
                                                  (set (mapv #(recursive-ctor type# %) val#)))))]
                             (do (swap! ~'cache assoc k# res#) res#))))))
           (valAt [this# k#]
             (.valAt this# k# nil))
           clojure.lang.IPersistentMap
           (equiv ~'[this o]
             (and (instance? ~class-name ~'o)
                  (= (.valAt ~'this :db-ref nil)
                     (.valAt ~'o    :db-ref nil))
                  ~@(for [{iname :name [arity sub-sp-nm] :type link? :link? :as item}
                          (:items spec)]
                      `(= (.valAt ~'this ~iname nil) (.valAt ~'o ~iname nil)))))
           (entryAt [this# k#]
             (let [v# (.valAt this# k# this#)]
               (when-not (identical? this# v#)
                 (clojure.lang.MapEntry. k# v#))))
           (seq [~gs]
             (->> [(if-let [ref# (.valAt ~gs :db-ref nil)] ;; :db-ref can't be false
                     (new clojure.lang.MapEntry :db-ref ref#))
                   ~@(for [{iname :name :as item} (:items spec)]
                       `(when (contains? ~gs ~iname)
                          (.entryAt ~gs ~iname)))]
                  (filter identity) ;; entries can't be false/nil
                  (seq)))
           (empty [this#]
             (throw (UnsupportedOperationException. (str "can't create empty " ~(:name spec)))))
           (assoc [this# k# v#] ;; TODO
             (let [{[arity# type#] :item required?# :required? :as item#} (get-item ~spec k#)]
               (when (and required?# (not (some? v#)))
                 (throw (ex-info "attempt to delete a required field" {:instance this# :field k#})))
               (when (and (= arity# :many) (not (every? some? v#)))
                 (throw (ex-info "invalid value for arity :many" {:entity this# :field k# :value v#})))
               (when (not (contains? #{~@(map :name (:items spec)) :db-ref} k#))
                 (throw (ex-info "attempt to associate field not in the spec" {:instance this# :field k#})))
               (new ~class-name
                    (reduce
                     (fn [m# [k1# v1#]] (assoc m# k1# (coerce ~spec k1# v1#)))
                     ~'atmap (conj (deref ~'cache) [k# v#])) ;; order important - k#/v# win
                    (atom {})
                    nil)))
           (containsKey [this# k#]
             (not (identical? this# (.valAt ~'atmap k# this#))))
           (iterator [this#]
             (.iterator
              (reduce
               (fn [m# [k1# v1#]]
                 (if-some [v2# (.valAt this# k1# nil)]
                   (assoc m# k1# v2#) m#))
               (deref ~'cache) ~'atmap)))
           (cons [this# e#]
             (if (map? e#)
               (reduce (fn [m# [k# v#]] (assoc m# k# v#)) this# e#)
               (if (= (count e#) 2)
                 (assoc this# (first e#) (second e#))
                 (throw (ex-info (str "don't know how to cons " e#) {:this this# :e e#})))))
           (without [this# k#]
             (new ~class-name (dissoc ~'atmap k#) (atom {}) nil))
           clojure.lang.IHashEq
           (hasheq [this#]
             (let [sub-hash# (or ~'db-hash
                                 (->> (reduce (fn [m# [k# v#]]
                                                (if-some [v2# (.valAt this# k# nil)]
                                                  (assoc m# k# v2#) m#))
                                              ~'atmap ~'atmap)
                                      (clojure.lang.APersistentMap/mapHasheq)))]
               (bit-xor ~(hash class-name) sub-hash#)))
           #_(hashCode [this#] ;; I'm not sure what hashCode does that hasheq doesn't
               (clojure.lang.APersistentMap/mapHash this#))
           (equals [this# ~gs]
             (= this# ~gs)))

         (defmethod print-method ~class-name [v# ^java.io.Writer w#]
           (.write w# (spec-instance->str ~spec v# '~(ns-name *ns*))))
         (defmethod pp/simple-dispatch ~class-name [v#]
           (let [pp-fn# pp/*print-pprint-dispatch*]
             (pp/with-pprint-dispatch
               (fn [obj#]
                 (if (instance? org.joda.time.DateTime obj#)
                   (pp/simple-dispatch (date-time-dispatch obj#))
                   (pp-fn# obj#)))
               (pp/pprint-logical-block
                :prefix (str "(" (resolved-ctor-name ~spec '~(ns-name *ns*)) " ") :suffix ")"
                (pp/simple-dispatch (merge (.atmap v#) (deref (.cache v#)))))))))))

(defn ^:no-doc resolved-ctor-name [spec ns]
  (let [ctor-name (make-name spec lower-case)]
    (cond
      (= (ns-name *ns*) ns) ctor-name
      (contains? (ns-refers *ns*) ctor-name) ctor-name
      :else
      (if-let [alias (some->> (ns-aliases *ns*)
                              (some (fn [[short long]] (and (= (ns-name long) ns) short))))]
        (str alias "/" ctor-name)
        (str ns    "/" ctor-name)))))

(defn ^:no-doc date-time-dispatch [dt]
  (let [time-ns (find-ns 'clj-time.core)
        maybe-refer (some #(when (= (second %) time-ns) (first %)) (ns-aliases *ns*))]
    (list (symbol (str (or maybe-refer 'clj-time.core))
                  "date-time")
          (time/year dt)
          (time/month dt)
          (time/day dt))))

(defn ^:no-doc spec-instance->str [spec si ns]
  "returns a string representation of spec instance si suitable for printing"
  (let [ctor-name (resolved-ctor-name spec ns)]
    (letfn [(write-value [v link?]
              (if-let [ref (and link? (get v :db-ref))]
                (str "(" (resolved-ctor-name (get-spec v) ns) " " {:db-ref ref} ")")
                (if (instance? org.joda.time.DateTime v)
                  (with-out-str (print-method (date-time-dispatch v) *out*))
                  (with-out-str (print-method v *out*)))))
            (write-item [{iname :name link? :link? [c t] :type :as item}]
              (when (contains? si iname)
                (->> (if-some [v (iname si)]
                       (case c
                         :one (write-value v link?)
                         :many (str "[" (->> v (map #(write-value % link?)) (join " ")) "]"))
                       "nil")
                     (str iname " "))))]
      (str "(" ctor-name " {"
           (->> (:items spec)
                (map #(write-item %))
                (cons (if-let [ref (get si :db-ref)] (str ":db-ref " ref)))
                (filter identity)
                (join " "))
           "})"))))

;; -----------------------------------------------------------------------------
;; type alias

(defn- spec->record-type [spec]
  (let [opts (for [{iname :name required? :required? [arity sub-sp-nm] :type :as item}
                   (:items spec)]
               (let [item-type (or (get-in type-map [sub-sp-nm :type-symbol])
                                   (symbol (str *ns*) (name sub-sp-nm)))]
                 [iname (let [t (case arity
                                  :one  item-type
                                  :many (list `t/Set item-type))]
                          (if required? t (list `t/Option t)))]))
        opts (cons [:db-ref `(t/Option t/Any)] opts)
        opts (cons [:spec-tacular/spec `(t/Option t/Keyword)] opts)]
    ;; TODO: aren't there required fields?
    `(t/HMap :complete? true :optional ~(into {} opts))))

(defn- spec->type [spec]
  (condp instance? spec
    Spec (spec->record-type spec)
    UnionSpec `(t/U ~@(map #(symbol (str *ns*) (name %)) (:elements spec)))
    EnumSpec `(t/U ~@(map (fn [val] `'~val) (:values spec)))))

(defn- mk-type-alias
  "Defines a core.typed alias named after the given spec's name"
  [spec alias-name]
  (let [alias (symbol (str alias-name))
        nsd-alias (symbol (str *ns*) (str alias-name))
        type  (spec->type spec)]
    `(do (t/defalias ~alias ~type)
         (defmethod get-type ~(:name spec) [_#]
           ~(condp instance? spec
              Spec      (map->SpecType {:name (:name spec) :type-symbol nsd-alias})
              UnionSpec (map->SpecType {:name (:name spec) :type-symbol nsd-alias})
              EnumSpec  (map->SpecType {:name (:name spec)
                                        :type clojure.lang.Keyword
                                        :type-symbol nsd-alias}))))))

;; -----------------------------------------------------------------------------
;; constructors

(defn ^:no-doc non-recursive-ctor
  "builds an instance from another, checking fields, but children
   must all be primitive, non-recursive values or already spark types"
  [map-ctor spec sp h]
  (let [items (:items spec)
        defaults (reduce (fn [m {name :name dv :default-value}]
                           (if (some? dv) (assoc m (keyword name) (if (ifn? dv) (dv) dv)) m))
                         sp items)
        sp-map   (reduce (fn [m k]
                           (let [coerced (coerce spec k (get sp k))]
                             (assoc m k coerced)))
                         defaults (keys sp))
        sp-map   (dissoc sp-map :spec-tacular/spec)
        actual-kws   (keys (dissoc sp :spec-tacular/spec :db-ref))
        required-kws (keep #(if (:required? %) (:name %)) items)
        rec-kws      (set (keep (fn [{iname :name [arity sub-spec-name] :type}]
                                  (when-not (primitive? sub-spec-name) iname))
                                items))]
    (do (doseq [kw (concat required-kws actual-kws)
                :when (not (contains? rec-kws kw))]
          (check-component! spec kw (get sp-map kw)))
        (map-ctor sp-map h))))

(t/ann ^:no-check recursive-ctor (t/All [a] [t/Keyword a -> a]))
(defn ^:no-doc recursive-ctor [spec-name orig-sp]
  "walks a nested map structure, constructing proper instances from nested values.
   Any sub-sp that is already a SpecInstance of the correct type is acceptable as well."
  (let [spec  (get-spec spec-name orig-sp)
        db-sp (database-coercion orig-sp)
        sp    (or (database-coercion orig-sp) orig-sp)
        spec-class (get-spec-class (:name spec))]
    (when-not (and spec sp)
      (throw (ex-info (str "cannot create " (name spec-name)) {:sp orig-sp})))
    (if (instance? spec-class sp)
      sp
      (if (instance? spec-class db-sp)
        db-sp
        (do (when-not (instance? clojure.lang.IPersistentMap sp)
              (throw (ex-info "sp is not a map" {:spec (:name spec) :sp sp})))
            (let [sub-kvs
                  ,(for [{iname :name [arity sub-spec-name] :type :as item} (:items spec)
                         :when (not (primitive? sub-spec-name))
                         :let [sub-sp (get sp iname)]
                         :when (some? sub-sp)]
                     (let [f (if db-sp identity #(recursive-ctor sub-spec-name %))]
                       [iname (case arity :one (f sub-sp) :many (map f sub-sp))]))]
              (non-recursive-ctor (get-map-ctor (:name spec))
                                  spec
                                  (into sp sub-kvs)
                                  (when db-sp (hash orig-sp)))))))))

(defn- mk-checking-ctor [spec ctor-name alias-name]
  "For use in macros, creates a constructor that checks
  precondititions and types."
  (let [ctor-name (symbol ctor-name)
        ctor-name-ann (with-meta ctor-name (assoc (meta ctor-name) :no-check true))
        core-type (symbol alias-name)]
    `(do
       (t/ann ~ctor-name-ann
              ~(if (empty? (:items spec))
                 ['-> core-type]
                 [core-type '-> core-type]))
       (defn ~ctor-name
         ~(str "Constructs a " (-> spec :name name) " from the given map")
         [& [sp#]] (recursive-ctor ~(:name spec) (if (some? sp#) sp# {})))
       (defmethod get-ctor ~(:name spec) [_#] ~ctor-name)
       (defmethod get-ctor ~spec [_#] ~ctor-name))))

(defn- mk-get-map-ctor [spec]
  (let [class-ctor (str "i_" (name (:name spec)))
        fac-sym    (symbol (str class-ctor "-fixed"))]
    `(do
       (defn ~fac-sym [o# h#] (~(symbol (str class-ctor ".")) o# (atom {}) h#))
       (defmethod get-map-ctor ~(:name spec) [_#] ~fac-sym))))

(defn- mk-union-get-map-ctor [spec]
  (let [fac-sym (symbol (str "map->i_" (name (:name spec)) "-fixed"))]
    `(do
       ;; the "map ctor" for an union means it's arg needs to
       ;; be a tagged map or a record type of one of the union's ctors.
       (defn ~fac-sym [o# h#]
         (let [subspec-name# (:name (get-spec o#))]
           (assert subspec-name#
                   (str "could not find spec for "o#))
           (assert (contains? ~(:elements spec) subspec-name#)
                   (str subspec-name#" is not an element of "~(:name spec)))
           ((get-map-ctor subspec-name#) o# h#)))
       (defmethod get-map-ctor ~(:name spec) [_#] ~fac-sym))))

;; -----------------------------------------------------------------------------
;; get-spec

(defn ^:no-doc eval-default-values [spec]
  "like eval, but eval won't go into records types like Spec or Item.
   we need to eval because we want the Spec record at macro time, but want to defer
   evalling the fn forms for eg :default-value
  (we could eval all nested values, not just :default-value, esp
   if we want to use non-literals in other fields in the future.)"
  (update-in spec [:items]
             (fn [items] (doall (map (fn [item] (update-in item [:default-value] eval)) items)))))

(defn- mk-get-spec [spec]
  (let [gs (gensym)]
    `(let [~gs (eval-default-values ~spec)]
       (defmethod get-spec ~(:name spec) [& _#] ~gs)
       (defmethod get-spec ~(spec->class spec) [& _#] ~gs))))

(defn- mk-union-get-spec [spec]
  ;; when calling get-spec on an union, try using the extra args to
  ;; narrow down the spec i.e. it's not always `spec` being returned if
  ;; we can do better
  (let [elements (:elements spec)]
    `(letfn [;; is spec2 somewhere in spec1's union
             (member-of-union# [spec1# spec2#]
               (when (or (contains? (:elements spec1#) (:name spec2#))
                         (some #(member-of-union# (get-spec %) spec2#) (:elements spec1#)))
                 spec2#))]
       (defmethod get-spec ~(:name spec) [_# & [rest#]]
         (if (some? rest#)
           (or (member-of-union# ~spec (get-spec rest#))
               (throw (ex-info "arguments to get-spec do not have the same spec"
                               {:obj1 _# :obj2 rest#})))
           ~spec)))))

(defn- mk-enum-get-spec [enum]
  (let [values (:values enum)]
    `(do ~@(map (fn [val] `(defmethod get-spec ~val [& _#] ~enum)) values)
         (defmethod get-spec ~(:name enum) [& _#] ~enum))))

;; -----------------------------------------------------------------------------
;; get-spec-class

(defn- mk-get-spec-class [spec]
  (cond
    (instance? Spec spec)
    ,(let [class-name (symbol (str "i_" (name (:name spec))))]
       `(defmethod get-spec-class ~(:name spec) [_#] ~class-name))
    (instance? EnumSpec spec)
    ,`(defmethod get-spec-class ~(:name spec) [_#] clojure.lang.Keyword)))

;; -----------------------------------------------------------------------------
;; huh

(defn- mk-huh [spec huh-name]
  (let [huh (symbol huh-name)
        huh-name-ann (with-meta huh (assoc (meta huh) :no-check true))
        gs (gensym)]
    `(do (t/ann ~huh [t/Any ~'-> t/Bool])
         (defn ~huh-name-ann [~gs]
           ~(condp instance? spec
              Spec      `(instance? ~(spec->class spec) ~gs)
              UnionSpec `(contains? ~(:elements spec) (:name (get-spec ~gs)))
              EnumSpec  `(contains? ~(:values spec) ~gs))))))

;; -----------------------------------------------------------------------------
;; meta

(defn spec-meta
  "Parses the meta-data that can be placed on a namespace containing
  spec definitions, or on spec definitions themselves.  Not intended
  to be called directly.

  Currently, it is possible to override the constructor, predicate,
  and type aliases name for a single spec or all specs in a namespace.

  ```
  (ns my-ns
   {....
    :spec-tacular
    {:ctor-name-fn  (fn [s] ....)
     :huh-name-fn   (fn [s] ....)
     :alias-name-fn (fn [s] ....)}
    ....}
    (:require ....))
  ```

  Each function should expect a string and return a string.  The
  function is currently `eval`uated in the [[spark.spec-tacular]]
  namespace, so plan accordingly.

  These names can be overriden on a per-spec basis in a similar
  fashion.

  ```
  (defspec
    ^{:ctor-name  \"mk-my-spec\"
      :huh-name   false ;; fall back to default
      :alias-name \"my-spec-type\"}
    my-spec
    ....)
  ```

  The spec-level meta-data has higher priority than the
  namespace-level meta-data.
  "
  {:added "0.6.0"}
  [ns spec spec-meta]
  (let [{:keys [ctor-name-fn huh-name-fn alias-name-fn]}
        ,(:spec-tacular (meta *ns*))
        {:keys [ctor-name huh-name alias-name]} spec-meta
        spec-name-str (-> spec :name name)]
    {:ctor-name  (cond
                   (and (not (some? ctor-name)) ctor-name-fn)
                   ,(symbol ((eval ctor-name-fn) spec-name-str))
                   ctor-name (symbol ctor-name)
                   :else (spec->ctor spec))
     :huh-name   (cond
                   (and (not (some? huh-name)) huh-name-fn)
                   ,(symbol ((eval huh-name-fn) spec-name-str))
                   huh-name (symbol huh-name)
                   :else (spec->huh spec))
     :alias-name (cond
                   (and (not (some? alias-name)) alias-name-fn)
                   (symbol ((eval alias-name-fn) spec-name-str))
                   alias-name (symbol alias-name)
                   :else (spec->alias spec))}))

;; -----------------------------------------------------------------------------
;; defspec, defunion, defenum

(defmacro defspec
  "Defines a spec-tacular spec type.

  ```
  (defspec Name
    [field-name docstring? arity type option ...]
    ...)
  ```

  creates the spec `:Name`; where arity is either `:is-a` or
  `:is-many` and type is either another spec name or a primitive type
  keyword. The docstring is optional; if given, it will become the
  :doc metadata on the spec var.

  spec-tacular supports base types `:keyword`, `:string`, `:boolean`,
  `:long`, `:bigint`, `:float`, `:double`, `:bigdec`, `:instant`,
  `:calendarday`, `:uuid`, `:uri`, and `:bytes`.

  Fields are allowed to have the following options:

  * `:unique` and `:identity`, meaning only one entity can have a
  given value for this attribute in the database
  * `:link`, meaning the instance is always passed by-reference
  * `:component`, mutually exclusive with `:link`, means the instance
  only exists when tied to it's parent
  * `:required`, a required field

  A core.typed alias `Name` is created, as well as a constructor
  `name` and a predicate `name?`.

  Spec instances that are created from a database may also contain the
  `:db-ref` field which, in the case of Datomic, contains a map `{:eid
  database-id}` containing the entity's `:db/id`."
  [& stx]
  (let [s (parse-spec stx)
        {:keys [ctor-name huh-name alias-name]} (spec-meta *ns* s (meta (first stx)))
        spec-name (make-name s identity)
        {:keys [doc]} s]
    `(do ~(mk-type-alias s alias-name)
         (def ~(with-meta spec-name {:spec-tacular/spec (:name s)
                                     :doc doc}) ~s)
         ~(mk-record s)
         ~(mk-get-map-ctor s)
         ~(mk-huh s huh-name)
         ~(mk-checking-ctor s ctor-name alias-name)
         ~(mk-get-spec s)
         ~(mk-get-spec-class s))))

(defmacro defunion
  "Defines a spec-tacular union.

  ```
  (defunion Name :SpecName ...)
  ```

  where each SpecName is another spec to be added to the union."
  [& stx]
  (let [u (parse-union stx)
        {:keys [huh-name alias-name]} (spec-meta *ns* u (meta (first stx)))
        spec-name (make-name u identity)]
    `(do ~(mk-type-alias u alias-name)
         (def ~(with-meta spec-name {:spec-tacular/union (:name u)}) ~u)
         ~(mk-union-get-map-ctor u)
         ~(mk-union-get-spec u)
         ~(mk-huh u huh-name))))

(defmacro defenum
  "Defines an enumeration of values under a parent name.

  ```
  (defenum Name symbol ...)
  ```

  The resulting enumeration recognizes `:Name/<symbol>` keywords,
  which answer `true` to `name?`.  Enumerations do not define
  constructors as keywords are already Clojure values."
  {:added "0.6.0"}
  [& stx]
  (let [e (parse-enum stx)
        {:keys [huh-name alias-name]} (spec-meta *ns* e (meta (first stx)))
        spec-name (make-name e identity)]
    `(do ~(mk-type-alias e alias-name)
         (def ~(with-meta spec-name {:spec-tacular/enum (:name e)}) ~e)
         ~(mk-enum-get-spec e)
         ~(mk-huh e huh-name)
         ~(mk-get-spec-class e))))

;; -----------------------------------------------------------------------------
;; diffing

(defn diff
  "Takes two spec instances and returns a vector of three maps created
  by calling `clojure.data/diff` on each item of the spec.

  Only well defined when `sp1` and `sp2` share the same spec.

  For `:is-many` fields, expect to see sets of similarities or
  differences in the result, as order should not matter."
  [sp1 sp2]
  (when-not (= (get-spec sp1) (get-spec sp2))
    (throw (ex-info "Spec instances do not share spec" {:sp1 sp1 :sp2 sp2})))
  (let [diff-results
        ,(for [key (disj (set (concat (keys sp1) (keys sp2))) :db-ref)
               :let [v1 (key sp1), v2 (key sp2)]]
           [key (if (map? v1)
                  (if (= v1 v2) [nil nil v1] [v1 v2 nil])
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

(defn identical-keys
  "Takes any number of spec instances and returns the keys for fields
  with values they all share in common.  Useful for error messages."
  [si]
  (->> (for [key (doall (distinct (mapcat keys si)))
             :when (apply = (map key si))]
         [key (key (first si))])
       (into {})))

;; -----------------------------------------------------------------------------
;; refless

(defn refless
  "Returns a version of the given spec instance with no `:db-ref`s on
  any sub-instance."
  {:added "0.5.0"}
  [si]
  (walk/prewalk (fn [si] (if (get-spec si) (dissoc si :db-ref) si)) si))

(declare refless=)

(defn- spec-refless= [x y]
  (if-let [x-spec (get-spec x)]
    (and (= x-spec (get-spec y))
         (if-let [items (:items x-spec)] ;; Spec
           (every? (fn [{iname :name [arity _] :type}]
                     (let [v1 (iname x), v2 (iname y)]
                       (case arity
                         :one (spec-refless= v1 v2)
                         :many (refless= v1 v2))))
                   items)
           (= x y))) ;; EnumSpec
    (= x y))) ;; no spec

(defn refless=
  "Given any walkable collection, returns `true` if the two
  collections would be `=` if no spec instances had `:db-ref`s.

  Contains a fast path if both `x` and `y` have specs, otherwise
  expect bad asymptotics as each collection must be rebuilt without
  `:db-ref`s."
  {:added "0.5.0"}
  [x y]
  (cond
    (and (get-spec x) (get-spec y))
    ,(spec-refless= x y)
    (and (sequential? x) (sequential? y))
    ,(and (= (count x) (count y))
          (every? #(apply refless= %) (map vector x y)))
    (and (map? x) (map? y))
    ,(every? #(refless= (get x %) (get y %))
             (set (concat (keys x) (keys y))))
    (and (set? x) (set? y))
    ,(and (= (contains? x nil) (contains? y nil))
          (loop [l (seq (clojure.set/difference x #{nil})),
                 s (clojure.set/difference y #{nil})]
            (if-some [v1 (when-first [v1 l] v1)]
              (if-some [v2 (some #(when (= (refless= % v1)) [%]) s)] ;; wrap false-ish values
                (recur (rest l) (clojure.set/difference s #{(first v2)}))
                false)
              (empty? s))))
    :else (= x y)))

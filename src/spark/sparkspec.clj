(ns spark.sparkspec
  {:core.typed {:collect-only true}}
  (:require [clojure.core.typed :as t]
            [clojure.string :refer [lower-case]]
            [clojure.data :as data]
            [spark.sparkspec.grammar :refer [parse-spec parse-enum]]
            [spark.sparkspec.spec :refer :all]
            [clojure.string :refer [join]] ;; TODO
            [clj-time.core :as time]
            [clojure.pprint :as pp])
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
(t/ann ^:no-check spark.sparkspec/get-spec [t/Any -> (t/Option SpecT)])
(t/ann ^:no-check spark.sparkspec/get-ctor [t/Keyword -> [(t/Map t/Any t/Any) -> SpecInstance]])
(t/ann ^:no-check spark.sparkspec/get-type [(t/U SpecInstance t/Keyword) -> (t/Map t/Keyword t/Any)])

(defn get-item [spec kw] 
  "returns the item in spec with the name kw"
  (assert (keyword? kw) (str "not a keyword: " kw))
  (->> spec :items (filter #(= (name (:name %)) (name kw))) first))

(defn coerce [spec kw val] 
  (let [{[cardinality typ] :type :as item} (get-item spec kw)]
    (if (and item val) ;; this is defined in the spec, and the item is non-nil
      (case cardinality
        :one ((or (-> type-map typ :coercion) identity) val)
        :many (vec (map #((or (-> type-map typ :coercion) identity) %) val)))
      val)))

;;;; Exported methods dealing with specs

(defn- resolve-fn [o & rest]
  (cond
    (:spec-tacular/spec o) (:spec-tacular/spec o) 
    (or (keyword? o) (= (class o) (class (class o)))) o
    :else (class o)))

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

(defmulti database-coercion
  "returns the coercion function that maps an object of a known database
   type (like datomic.query.EntityMap) to a map that can be used by recursive-ctor"
  (fn [o] (type o)))
(defmethod database-coercion :default [_] nil)

(doseq [[k v] type-map]
  (defmethod get-spec-class k [_] (:type v)))

(defn check-component!
  "checks the key and its value against the given spec"
  [spec k v]
  (let [{iname :name
         [cardinality typ] :type
         required? :required?
         precondition :precondition
         :as item}
        (get-item spec k)
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
                          {:expected-type typ
                           :actual-type (class v)
                           :sub-spec-name iname
                           :spec-name sname}))))))
  true)

(defn check-complete! 
  "checks all fields of a record-sp (not enum), 
   throws exception if required field is missing."
  [spec sp]
  (do (assert (nil? (:elements spec)) "spec cannot be an enum")
      (doseq [{iname :name :as item} (:items spec)]
        (check-component! spec iname (get sp iname)))
      sp))

;; t/Any -> t/Bool
(defn has-spec?
  "Returns whether or not the given object has a spec."
  [o]
  (some? (get-spec o)))

(def core-types (into #{} (keys type-map)))

;; t/Keyword -> t/Bool
(defn primitive?
  "Returns true if the given spec name is primitive (i.e. is part of
   the base type environment and not defined as a spec), false otherwise."
  [spec-name]
  (contains? core-types spec-name))

;; Item -> (t/U (t/Val :non-rec) (t/Val :rec))
(defn recursiveness
  "Returns :non-rec if the given item is primitive, :rec otherwise."
  [{[_ t] :type}]
  (if (primitive? t) :non-rec :rec)) ;; TODO

;;;; Spec->form Functions

(defn- make-name [spec append-fn]
  (symbol (append-fn (-> spec :name name))))
(defn- spec->class [spec]
  (symbol (str "i_" (-> spec :name name))))
(defn- spec->huh [])

(defn- mk-record [spec] ;; CODEWALK
  (let [gs (gensym), class-name (spec->class spec)
        {:keys [non-rec]} (group-by recursiveness (:items spec))
        non-rec-kws (cons :db-ref (map :name non-rec))]
    `(do (deftype ~class-name [~'atmap ~'cache]
           clojure.lang.IRecord
           clojure.lang.IObj 
           ;; user doesn't get the map out again so it's fine to put the meta on it
           ;; stipulation -- meta of the SpecInstance is initially inherited from meta of the map
           (meta [this#] (meta ~'atmap))
           (withMeta [this# ~gs] (new ~class-name (with-meta ~'atmap ~gs) (atom {})))
           clojure.lang.ILookup
           (valAt [this# k# not-found#]
             (or (k# (deref ~'cache))
                 (let [val# (.valAt ~'atmap k# not-found#)]
                   (cond
                     (identical? val# not-found#) val#
                     (some #(= % k#) [~@non-rec-kws]) val#
                     (not val#) val#
                     :else (let [res# (let [{[arity# type#] :type :as item#} (get-item ~spec k#)]
                                        (case arity#
                                          :one (recursive-ctor type# val#)
                                          :many (vec (map #(recursive-ctor type# %) val#))))]
                             (do (swap! ~'cache assoc k# res#) res#))))))
           (valAt [~'this ~'k]
             (.valAt ~'this ~'k nil))
           clojure.lang.IPersistentMap
           (equiv ~'[this o]
             (and (instance? ~class-name ~'o)
                  (let [ref1# (get ~'this :db-ref), ref2# (get ~'o :db-ref)]
                    (or (not ref1#) (not ref2#)
                        (= ref1# ref2#)))
                  ~@(for [{iname :name [arity sub-sp-nm] :type link? :link? :as item}
                          (:items spec)]
                      (let [v1 (gensym), v2 (gensym), l (gensym), m (gensym)
                            remove-ref #(if (or link? (primitive? sub-sp-nm))
                                          % `(dissoc ~% :db-ref))]
                        `(let [~v1 (~iname ~'this), ~v2 (~iname ~'o)]
                           ~(case arity
                              :one `(= ~(remove-ref v1) ~(remove-ref v2))
                              :many `(and (every?
                                           (fn [~l]
                                             (some (fn [~m] (= ~(remove-ref l) ~(remove-ref m)))
                                                   ~v1))
                                           ~v2)
                                          (= (count ~v1) (count ~v2)))))))))
           (entryAt [this# k#]
             (let [v# (.valAt this# k# this#)]
               (when-not (identical? this# v#)
                 (clojure.lang.MapEntry. k# v#))))
           (seq [~gs]
             (->> [(if-let [ref# (.valAt ~gs :db-ref)]
                     (new clojure.lang.MapEntry :db-ref ref#))
                   ~@(for [{iname :name :as item} (:items spec)]
                       `(when (contains? ~gs ~iname)
                          (new clojure.lang.MapEntry ~iname (~iname ~gs))))]
                  (filter identity)
                  (seq)))
           (empty [this#]
             (throw (UnsupportedOperationException. (str "can't create empty " ~(:name spec)))))
           (assoc [this# k# v#] ;; TODO
             (let [{[arity# type#] :item required?# :required? :as item#} (get-item ~spec k#)]
               (when (and required?# (not v#))
                 (throw (ex-info "attempt to delete a required field" {:instance this# :field k#})))
               (when (and (= arity# :many) (not (every? identity v#)))
                 (throw (ex-info "invalid value for arity :many"
                                 {:entity this# :field k# :value v#})))
               (new ~class-name
                    (reduce
                     (fn [m# [k1# v1#]] (assoc m# k1# v1#))
                     ~'atmap (conj (deref ~'cache) [k# v#])) ;; order important - k#/v# win
                    (atom {}))))
           (containsKey [this# k#]
             (not (identical? this# (.valAt ~'atmap k# this#))))
           (iterator [this#]
             (.iterator ~'atmap))
           (cons [this# e#]
             (if (map? e#)
               (reduce (fn [m# [k# v#]] (assoc m# k# v#)) this# e#)
               (if (= (count e#) 2)
                 (assoc this# (first e#) (second e#))
                 (throw (ex-info (str "don't know how to cons " e#) {:this this# :e e#})))))
           (without [this# k#]
             (new ~class-name (dissoc ~'atmap k#) (atom {})))
           clojure.lang.IHashEq
           (hasheq [this#]
             (bit-xor ~(hash class-name)
                      (clojure.lang.APersistentMap/mapHasheq (.without this# :db-ref))))
           (hashCode [this#]
             (clojure.lang.APersistentMap/mapHash this#))
           (equals [this# ~gs]
             (clojure.lang.APersistentMap/mapEquals this# ~gs)))
         
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

(defn resolved-ctor-name [spec ns]
  (let [ctor-name (make-name spec lower-case)]
    (cond
      (= (ns-name *ns*) ns) ctor-name
      (contains? (ns-refers *ns*) ctor-name) ctor-name
      :else
      (if-let [alias (some->> (ns-aliases *ns*)
                              (some (fn [[short long]] (and (= (ns-name long) ns) short))))]
        (str alias "/" ctor-name)
        (str ns    "/" ctor-name)))))

(defn date-time-dispatch [dt]
  (let [time-ns (find-ns 'clj-time.core)
        maybe-refer (some #(when (= (second %) time-ns) (first %)) (ns-aliases *ns*))]
    (list (symbol (str (or maybe-refer 'clj-time.core))
                  "date-time")
          (time/year dt)
          (time/month dt)
          (time/day dt))))

(defn spec-instance->str [spec si ns]
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
                (->> (if-let [v (iname si)]
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

(defn spec->enum-type [spec]
  `(t/U ~@(map #(symbol (str *ns*) (name %)) (:elements spec))))

(defn spec->record-type [spec]
  (let [opts (for [{iname :name required? :required? [arity sub-sp-nm] :type :as item}
                   (:items spec)]
               (let [item-type (if (primitive? sub-sp-nm)
                                 (get-in type-map [sub-sp-nm :type-symbol])
                                 (symbol (str *ns*) (name sub-sp-nm)))]
                 [iname (let [t (case arity
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
  (let [items (:items spec)
        defaults (reduce (fn [m {name :name dv :default-value}]
                           (if (some? dv) (assoc m (keyword name) (if (ifn? dv) (dv) dv)) m))
                         sp items)
        sp-map   (reduce (fn [m k]
                           (let [coerced (coerce spec k (get sp k))]
                             (if (some? coerced) (assoc m k coerced) m)))
                         defaults (keys sp))
        sp-map   (dissoc sp-map :spec-tacular/spec)
        actual-kws   (keys (dissoc sp :spec-tacular/spec :db-ref))
        required-kws (keep #(if (:required? %) (:name %)) items)]
    (do (doseq [kw (concat required-kws actual-kws)]
          (check-component! spec kw (get sp-map kw)))
        (map-ctor sp-map))))

(t/ann ^:no-check recursive-ctor (t/All [a] [t/Keyword a -> a]))
(defn recursive-ctor [spec-name orig-sp]
  "walks a nested map structure, constructing proper instances from nested values.
   Any sub-sp that is already a SpecInstance of the correct type is acceptable as well."
  (let [spec (get-spec spec-name orig-sp)
        sp   (or (database-coercion orig-sp) orig-sp)]
    (when-not (and spec sp)
      (throw (ex-info (str "cannot create " (name spec-name)) {:sp orig-sp})))
    (when-not (instance? clojure.lang.IPersistentMap sp)
      (throw (ex-info "sp is not a map" {:spec (:name spec) :sp sp})))
    (if (if-let [spec-class (get-spec-class (:name spec))]
          (instance? spec-class sp))
      sp
      (let [sub-kvs
            ,(keep (fn [{iname :name [arity sub-spec-name] :type :as item}]
                     (when-not (primitive? sub-spec-name)
                       (when-let [sub-sp (get sp iname)]
                         [iname (case arity
                                  :one (recursive-ctor sub-spec-name sub-sp)
                                  :many (map #(recursive-ctor sub-spec-name %) sub-sp))])))
                   (:items spec))]
        (non-recursive-ctor (get-map-ctor (:name spec)) spec (into sp sub-kvs))))))

(defn- mk-checking-ctor [spec]
  "For use in macros, creates a constructor that checks
  precondititions and types."
  (let [ctor-name (make-name spec lower-case)
        ctor-name-ann (with-meta ctor-name (assoc (meta ctor-name) :no-check true))
        ctor-name-def (with-meta ctor-name (assoc (meta ctor-name) :spec-tacular/spec (:name spec)))
        core-type (-> spec :name name symbol)]
    `(do
       (t/ann ~ctor-name-ann
              ~(if (empty? (:items spec))
                 ['-> core-type]
                 [core-type '-> core-type]))
       (defn ~ctor-name-def
         ~(str "deep-walks a nested map structure to construct a "
              (-> spec :name name))
         [& [sp#]] (recursive-ctor ~(:name spec) (if (some? sp#) sp# {})))
       (defmethod get-ctor ~(:name spec) [_#] ~ctor-name-def)
       (defmethod get-ctor ~spec [_#] ~ctor-name-def))))

(defn eval-default-values [spec]
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
       (defmethod get-spec ~(symbol (str "i_" (name (:name spec)))) [& _#] ~gs))))

(defn- mk-get-spec-class [spec]
  (let [class-name (symbol (str "i_" (name (:name spec))))]
    `(defmethod get-spec-class ~(:name spec) [_#] ~class-name)))

(defn- mk-get-map-ctor [spec]
  (let [class-ctor (str "i_" (name (:name spec)))
        fac-sym    (symbol (str class-ctor "-fixed"))]
    `(do
       (defn ~fac-sym [o#] (~(symbol (str class-ctor ".")) o# (atom {})))
       (defmethod get-map-ctor ~(:name spec) [_#] ~fac-sym)
       (defmethod get-map-ctor ~(symbol (str "i_" (name (:name spec)))) [_#] ~fac-sym))))

;; when calling get-spec on an enum, try using the extra args to narrow down the spec
;; i.e. it's not always `spec` being returned if we can do better
(defn- mk-enum-get-spec [spec]
  (let [elements (:elements spec)]
    `(defmethod get-spec ~(:name spec) [o# & [rest#]]
       (if-let [rest-spec# (and rest# (get-spec rest#))]
         (if (some #(= (:name rest-spec#) %) [~@elements]) rest-spec#
             (throw (ex-info "trying to construct an enum out of spec instance(s) not in enum spec"
                             {:objects (cons o# rest#) :spec ~spec})))
         ~spec))))

(defn- mk-enum-get-map-ctor [spec]
  (let [fac-sym (symbol (str "map->i_" (name (:name spec)) "-fixed"))]
    `(do
       ;; the "map ctor" for an enum means it's arg needs to 
       ;; be a tagged map or a record type of one of the enum's ctors.
       (defn ~(with-meta fac-sym (assoc (meta fac-sym) :spec-tacular/spec (:name spec))) [o#]
         (let [subspec-name# (:name (get-spec o#))]
           (assert subspec-name#
                   (str "could not find spec for "o#))
           (assert (contains? ~(:elements spec) subspec-name#) 
                   (str subspec-name#" is not an element of "~(:name spec)))
           ((get-map-ctor subspec-name#) o#)))
       (defmethod get-map-ctor ~(:name spec) [_#] ~fac-sym)
       (defmethod get-map-ctor ~(symbol (name (:name spec))) [_#] ~fac-sym))))

(defn- mk-huh [spec]
  (let [huh (make-name spec #(str (lower-case %) "?"))
        class-name (make-name spec #(str "i_" %))]
    `(do (t/ann ~huh [t/Any ~'-> t/Bool])
         (defn ~huh [o#] (instance? ~class-name o#)))))

(defn- mk-enum-huh [spec]
  (let [huh (make-name spec #(str (lower-case %) "?"))]
    `(defn ~huh [o#] (contains? ~(:elements spec) (:name (get-spec o#))))))

;;;; defspec Macro

(defmacro defspec [& stx]
  (let [s (parse-spec stx)]
    `(do ~(mk-type-alias s)
         ~(mk-record s)
         ~(mk-get-map-ctor s)
         ~(mk-huh s)
         ~(mk-checking-ctor s)
         ~(mk-get-spec s)
         ~(mk-get-spec-class s))))

(defmacro defenum [& stx]
  (let [s (parse-enum stx)]
    `(do ~(let [spec-name (:name s)
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

(defn identical-keys [si]
  "Takes any number of spec instances and returns the keys that they
  all share in common.  Good for error messages."
  (->> (for [key (doall (distinct (mapcat keys si)))
             :when (apply = (map key si))]
         [key (key (first si))])
       (into {})))


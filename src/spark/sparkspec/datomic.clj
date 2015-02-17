(ns spark.sparkspec.datomic
  (:refer-clojure :exclude [for])
  (:use spark.sparkspec.spec
        spark.sparkspec
        clojure.data
        [clojure.string :only [lower-case]]
        [clojure.set :only [rename-keys difference]])
  (:import clojure.lang.MapEntry)
  (:require [clojure.core.typed :as t :refer [for letfn>]]
            [clojure.data :only diff]
            [clojure.tools.macro :as m]
            [clojure.walk :as walk]))

(require '[datomic.api :as db])

(t/defalias SpecInstance (t/Map t/Any t/Any))
(t/defalias Pattern (t/Map t/Any t/Any))
(t/defalias SpecName t/Keyword)
(t/defalias Mask (t/Rec [mask] (t/Map t/Keyword (t/U mask t/Bool))))
(t/defalias Item (t/HMap :mandatory {:name t/Keyword
                                     :type (t/HVec [(t/U (t/Val :one)
                                                         (t/Val :many))
                                                    SpecName])}))

(t/defalias SpecT (t/HMap :mandatory {:name SpecName
                                      :items (t/Vec Item)}))
(t/defalias ConnCtx (t/HMap :mandatory {:conn datomic.peer.LocalConnection}))

(t/ann clojure.core/some? [t/Any -> t/Bool :filters {:then (! nil 0) :else (is nil 0)}])
(t/ann clojure.core/not-empty (t/All [x] [(t/Option (t/Vec x)) -> (t/Option (t/NonEmptyVec x)) :filters {:then (is (t/NonEmptyVec x) 0)}])) ; Can't make Seq work- the polymorphic specialization fails to match.
(t/ann datomic.api/q [t/Any * -> (t/Vec (t/Vec t/Any))])
(t/ann datomic.api/tempid [t/Keyword -> datomic.db.DbId])
(t/ann spark.sparkspec/primitive? [t/Keyword -> t/Bool])
(t/ann spark.sparkspec/get-ctor [t/Keyword -> [(t/Map t/Any t/Any) -> SpecInstance]])
(t/ann spark.sparkspec/get-spec [(t/U SpecInstance t/Keyword) -> SpecT])
(t/ann spark.sparkspec.test-utils/make-db [(t/Vec t/Any) -> datomic.peer.LocalConnection])
(t/ann spark.sparkspec.test-utils/db [-> datomic.db.Db])
(t/ann spark.sparkspec.test-utils/*conn* datomic.peer.LocalConnection)
(t/ann spark.sparkspec.datomic-test/simple-schema (t/Vec t/Any))

(def spark-type-attr 
  "The datomic attribute holding onto a keyword-valued spec type."
  :spec-tacular/spec)

(def db-type->spec-type
  ^:private
  (reduce (t/ann-form #(assoc %1 %2 (keyword (name %2)))
                      [(t/Map t/Keyword t/Keyword) t/Keyword -> (t/Map t/Keyword t/Keyword)]) {}
          [:db.type/keyword :db.type/string :db.type/boolean :db.type/long
           :db.type/bigint :db.type/float :db.type/double :db.type/bigdec
           :db.type/instant :db.type/uuid :db.type/uri :db.type/bytes]))

(t/ann datomic-ns [SpecT -> t/Str])
(defn- datomic-ns
  "Returns a string representation of the db-normalized namespace for
  the given spec."
  [spec]
  (some-> spec :name name lower-case))

(t/ann ^:no-check datomic-schema [SpecT -> (t/ASeq (t/Map t/Keyword t/Any))])
(defn datomic-schema 
  "generate a list of entries for a datomic schema to represent this spec."
  [spec]
  (for [{iname :name [cardinality type] :type :as item} :- Item (:items spec)]  ;; FIXME not sure why this doesn't typecheck?
    (merge
     {:db/id (db/tempid :db.part/db)
      :db/ident (keyword (datomic-ns spec) (name iname))
      :db/valueType (if (primitive? type)
                      (keyword "db.type" (name type))
                      :db.type/ref)
      :db/cardinality (if (= cardinality :many)
                        :db.cardinality/many
                        :db.cardinality/one)
      :db/doc ""
      :db.install/_attribute :db.part/db}
     (if (:unique? item)
       (if (:identity? item)
         {:db/unique :db.unique/identity}
         {:db/unique :db.unique/value})
       {}))))

; This one is pretty tricky to annotate
(t/ann ^:no-check check-schema [(t/ASeq SpecInstance) SpecT -> (t/ASeq t/Str)])
(defn check-schema
  "Returns a list of errors representing discrepencies between the
  given spec and schema."
  [schema spec]
  (letfn [;reduce-component :- [(t/Map t/Keyword t/Keyword) (t/Map t/Keyword t/Keyword) -> (t/Map t/Keyword t/Keyword)]
          (reduce-component [m v] (assoc m (-> v :db/ident name keyword) v))
;reduce-items :- [(t/Map t/Keyword Item) Item -> (t/Map t/Keyword Item)]
          (reduce-items [m v] (assoc m (:name v) v))
;check :- [t/Any t/Any -> t/Any]
          (check [v m] (if v nil m))
;all-errors :- [t/Any * -> t/Any]
          (all-errors [& rest] (filter some? (flatten rest)))
;diff-uniques :- [(t/HVec [(t/Map t/Keyword t/Keyword)
;                          Item])
;                 -> (t/U nil t/Str)]
          (diff-uniques [[{schema-uniq :db/unique}
                          {iname :name item-uniq :unique? item-ident :identity?}]]
            (check (case schema-uniq
                     nil (not (or item-uniq item-ident))
                     :db.unique/value (and item-uniq (not item-ident))
                     :db.unique/identity (and item-uniq item-ident))
                   (format "uniqueness for field %s in %s is inconsistant"
                           iname (:name spec))))]
    (let [{sname :name
           opts :opts
           items :items} spec
           spec-name (-> sname name lower-case)
           relevant-schema (filter
                            #(= spec-name (namespace (:db/ident %)))
                            schema)
           component-by-name (reduce reduce-component {} relevant-schema)
           item-by-name (reduce reduce-items {} (:items spec))
           schema-keys (set (keys component-by-name))
           name-keys (set (keys item-by-name))
           component->item (map
                            #(vector (% component-by-name) (% item-by-name))
                            schema-keys)]
      (all-errors
       (check (= schema-keys name-keys)
              (format "inconsistent keys between schema and spec. Diff: %s"
                      (diff schema-keys name-keys)))
       (map diff-uniques component->item)
       ;; TODO: Add more checks! Be strict!
       ))))


(t/ann get-all-eids [datomic.db.DbId SpecT -> (t/ASeq Long)])
(defn get-all-eids
  "Retrives all of the eids described by the given spec from the
  database."
  [db spec]
  (let [names (map (t/ann-form #(keyword (datomic-ns spec) (-> % :name name))
                               [Item -> t/Keyword]) (:items spec))
        query '[:find ?eid :in $ [?attr ...] :where [?eid ?attr ?val]]]
    (map (t/ann-form (fn [x] 
                       (let [r (first x)]
                         (assert (instance? Long r))
                         r))
                     [(t/Vec t/Any) -> Long])
         (db/q query db names))))




(t/ann get-eid (t/IFn 
                [datomic.db.DbId SpecInstance -> (t/Option Long)]
                [datomic.db.DbId SpecInstance SpecT -> (t/Option Long)]))
(defn get-eid
  "Returns an EID associated with the data in the given spark type if
  it exists in the database. Looks up according to identity
  items. Returns nil if not found."
  ([db sp] (when (map? sp) (get-eid db sp (get-spec sp))))
  ([db sp spec]
     (let [eid (or (:eid (meta sp)) (get-in sp [:db-ref :eid]))]
       (assert (or (nil? eid) (instance? Long eid)))
       (t/ann-form ; Seems like 'if' isn't typechecked correctly w/o annotation?
        (if eid
          eid
          (let [sname (datomic-ns spec)
                idents (filter #(or (:identity? %) (:unique? %)) (:items spec))
                query '[:find ?eid :in $ ?attr ?val :where [?eid ?attr ?val]]
                eids
                , (for [{iname :name [_ type] :type} :- Item idents]
                    :- (t/Option (t/Vec (t/Vec t/Any)))
                    (let [sub-sp (get sp iname)
                          sub-val (if (primitive? type)
                                    sub-sp
                                    (when (map? sub-sp) (get-eid db sub-sp)))
                          eid (when (some? sub-val)
                                (db/q query db
                                      (keyword (name sname) (name iname))
                                      sub-val))]
                      eid))]
            (->> eids
                 (filter (t/ann-form 
                          not-empty
                          [(t/Option (t/Vec (t/Vec t/Any))) -> (t/Option (t/NonEmptyVec (t/Vec t/Any))) :filters {:then (is (t/NonEmptyVec (t/Vec t/Any)) 0)}]))
                 (map (t/ann-form
                       #(let [r (ffirst %)]
                          (assert (instance? Long r))
                          r)
                       [(t/Option (t/NonEmptyVec (t/Vec t/Any))) -> Long]))
                 (first))))
        (t/Option Long)))))

(t/ann ^:no-check db->sp [datomic.db.DbId (t/Map t/Any t/Any) -> SpecInstance])
(defn db->sp
  [db ent & [sp-type]]
  (if-not ent
    nil
    (let [eid (:db/id ent)
          ent (into {} ent)
          spec (get-spec (spark-type-attr ent))
          ctor (get-ctor (:name spec))
          reduce-attr->kw #(assoc %1 (keyword (datomic-ns spec) (name %2)) (-> %2 name keyword))
          val (rename-keys ent (reduce reduce-attr->kw {} (map :name (:items spec))))
          val (reduce (fn [m {iname :name [cardinality typ] :type :as item}]
                        (let [v (get val iname)]
                          (if (nil? v)
                            (assoc m iname ; explicitly list nils for missing values
                                   (case cardinality
                                     :one nil
                                     :many (list)))
                            (case (recursiveness item)
                              :rec (assoc m iname
                                          (case cardinality
                                            :one (db->sp db v typ)
                                            :many (map #(db->sp db % typ) v)))
                              :non-rec m))))
                      val (:items spec))]
      (assert ctor (str "No ctor found for " (:name spec)))
      (-> (ctor val)
          (assoc :db-ref {:eid eid})
          (dissoc spark-type-attr)))))

(t/ann ^:no-check get-by-eid (t/IFn [datomic.db.DbId Long -> SpecInstance]
                                    [datomic.db.DbId Long SpecT -> SpecInstance]))
(defn get-by-eid
  "throws IllegalArgumentException when eid isn't found."
  [db eid & [sp-type]]
  (db->sp db (db/entity db eid) sp-type))

(t/ann ^:no-check get-all-of-type [datomic.db.DbId Long SpecT -> (t/ASeq SpecInstance)])
(defn get-all-of-type
  "Helper function that returns all items of a single spec"
  [db spec]
  (let [eids (get-all-eids db spec)]
    (map (fn [eid] (db->sp db (db/entity db eid) (:name spec))) eids)))


(t/ann ^:no-check expand-pattern [Pattern SpecName t/Symbol (t/Atom1 (t/Map t/Symbol t/Any)) (t/Atom1 (t/Map t/Symbol SpecName)) -> Pattern])
(defn- expand-pattern
  [pattern spec-name sym in-env ret-type-env]
  (let [spec (get-spec spec-name)
        {recs :rec non-recs :non-rec} (group-by recursiveness (:items spec))]
    (concat
     (->> recs 
          (mapcat (fn [{[arity sub-spec-name] :type :as item}]
                    (cond
                     (map? (get pattern (:name item)))
                     , (let [gs (gensym "?v")]
                         (cons [sym (keyword (lower-case (name spec-name))
                                             (name (:name item))) gs]
                               (expand-pattern (get pattern (:name item)) sub-spec-name gs in-env ret-type-env)))
                     (contains? pattern (:name item))
                     , (throw "only maps can be bound to refs right now")
                     :else []))))
     (->> non-recs
          (mapcat (fn [item]
                    (when (contains? pattern (:name item))
                      (let [expr (get pattern (:name item))]
                        (if (and (symbol? expr)
                                 (::ret-var (meta expr)))
                          (do (swap! ret-type-env assoc expr (second (:type item)))
                              [[sym (keyword (lower-case (name spec-name))
                                             (name (:name item)))
                                expr]])
                          (let [insym (gensym "?i")]
                            (swap! in-env assoc insym expr)
                            [[sym (keyword (lower-case (name spec-name))
                                           (name (:name item)))
                              insym]]))))))))))

(t/ann ^:no-check analyze-query [(t/ASeq t/Symbol) Pattern
                                 -> (t/HMap :mandatory
                                            {:ret-env (t/ASeq (t/HVec [t/Symbol t/Symbol]))
                                             :pattern Pattern
                                             :ret-type-env (t/Map t/Symbol SpecName)
                                             :inputs (t/Map t/Symbol t/Any)})])
(defn- analyze-query [rets pattern]
  (let [ret-env (map (fn [r] [r (with-meta (gensym (str "?" r)) (assoc (meta r) ::ret-var true))]) rets)
        [_ marked-pattern] ; We mark the symbols bound by the ret binder. We use symbol-macrolet as a lexical-scope-respecting symbol traversal. It wraps output in a (do ..) so we pick out the 2nd pattern.
        , (m/mexpand `(m/symbol-macrolet
                       ~(vec (apply concat ret-env))
                       ~pattern))
        spec-name (:spark.sparkspec/spec marked-pattern)
        in-env (atom {})
        ret-type-env (atom {})
        pattern (expand-pattern marked-pattern spec-name (gensym "?v") in-env ret-type-env)]
    {:ret-env ret-env
     :pattern pattern
     :ret-type-env @ret-type-env
     :inputs @in-env}))

(defmacro query [rets db pattern]
  (let [{:keys [ret-env ret-type-env pattern inputs]} (analyze-query rets pattern)
        args-types (map (fn [[k v]] [(gensym (name k))
                                     (:type-symbol (get type-map v))])
                        ret-type-env)]
    `(let [check# (t/ann-form
                   (fn [~(vec (map first args-types))]
                     ~@(map (fn [[k v]] ; assert each argument is of correct type.
                              `(assert (instance? ~v ~k)
                                       (str "Query returned " ~k
                                            " with type " (type ~k)
                                            " instead of something of type " ~v
                                            " Possible spec mismatch?"))) 
                            args-types)
                     ~(vec (map first args-types)))
                   [(t/Vec t/Any) ~'-> (t/HVec ~(vec (map second args-types)))])
           dbret# (db/q '[:find ~@(map second ret-env)
                          :in ~'$ ~@(map first inputs)
                          :where ~@pattern] ~db ~@(map second inputs))]
       (map check# dbret#))))

(t/ann ^:no-check build-transactions
       (t/IFn [datomic.db.DbId SpecInstance Mask (t/Atom1 (t/ASeq (t/Vec t/Any))) -> (t/Map t/Keyword t/Any)]
              [datomic.db.DbId SpecInstance Mask (t/Atom1 (t/ASeq (t/Vec t/Any))) SpecT -> (t/Map t/Keyword t/Any)]))
(defn build-transactions
  "Builds a nested datomic-data datastructure for the sp data, only
  for what's specified in the mask. Adds Datomicy deletion commands to
  the given atomic list of deletions when appropriate."
  [db sp mask deletions & [spec]]
  (let [spec (or spec (get-spec sp))
        [spec mask] (if (:elements spec) ; Need to pick which enum branch to pick in the mask.
                      (let [sub-name (:name (get-spec sp))]
                        [(get-spec (get-in spec [:elements sub-name]))
                         (get mask sub-name)])
                      [spec mask])
        extra-keys (clojure.set/difference (into #{} (keys sp)) ; we could check for just non-nil extra keys if we had to for some reason. starting with pickier for now.
                                           (into #{:db-ref} ; this extra key is fine.
                                                 (map :name (:items spec))))
        _ (assert (empty? extra-keys)
                  (str (:name spec) " has extra keys not in the spec: " extra-keys))
        eid (get-eid db sp)
        db-value (if eid (get-by-eid db eid (:name spec)) nil)
        eid (or eid (db/tempid :db.part/user))]
    (->> (for [{iname :name [cardinality type] :type :as item} (:items spec)
               :when (iname mask)
               :let [is-nested (= (recursiveness item) :rec)
                     is-many (= cardinality :many)
                     ival (iname sp)
                     sub-spec (get-spec type) ; Not necessarily ival's spec: could be an enum.
                     mask (iname mask)
                     ival-db (iname db-value)
                     datomic-key (keyword (datomic-ns spec) (name iname))
                     retract (fn [r] [:db/retract eid datomic-key
                                      (if-let [eid (get-in r [:db-ref :eid])]
                                        eid
                                        r)])]]
           (do
;             (prn "ival-db" ival-db "ival" ival)
             [datomic-key
              (if is-nested
                (if (map? mask)
                  (if is-many
                    (let [old-eids (set (map (partial get-eid db) ival-db))
                          new-eids (set (map (partial get-eid db) ival))
                          [_ deletes _] (diff new-eids old-eids)]
                      (swap! deletions concat (map retract deletes))
                      (set (map #(build-transactions db % mask deletions sub-spec)
                                ival)))
                    (if (some? ival)
                      (build-transactions db ival mask deletions sub-spec)
                      (if (some? ival-db)
                        (do (swap! deletions conj (retract ival-db)) nil)
                        nil)))
                  (if is-many
                    (let [old-eids (set (map (partial get-eid db) ival-db))
                          new-eids (set (map (partial get-eid db) ival))
                          [adds deletes _] (diff new-eids old-eids)]
                      (swap! deletions concat (map retract deletes))
                      adds)
                    (if (some? ival)
                      (get-eid db ival)
                      (if (some? ival-db)
                        (do (swap! deletions conj (retract ival-db)) nil)
                        nil))))
                (if is-many
                  (let [[adds deletes] (diff ival ival-db)]
                    (swap! deletions concat (map retract deletes))
                    adds)
                  (if (some? ival)
                    ival
                    (if (some? ival-db)
                      (do (swap! deletions conj (retract ival-db)) nil)
                      nil))))]))
         (filter (fn [[_ v]] (some? v)))
         (into {})
         (#(assoc % :db/id eid))
         (#(assoc % spark-type-attr (:name spec)))
         (#(with-meta % {:eid eid})))))

(t/ann ^:no-check union-masks (t/IFn [-> (t/Val nil)]
                                     [(t/Option Mask) -> (t/Option Mask)]
                                     [(t/Option Mask) (t/Option Mask) -> (t/Option Mask)]))
(defn union-masks
  "union (join) taken w.r.t. a lattice of 'specificity' eg -- nil < true < {:item ...} 
   (recall 'true' means the mask consisting only of the db-ref)
   keys are combined and their values are recursively summed.
   Enums and Records can be summed the same, as we represent both with maps."
  ([] nil)
  ([m] m)
  ([ma mb]
     (cond
      (and (map? ma) (map? mb))
      (merge-with union-masks ma mb)
      (map? ma) ma
      (map? mb) mb
      :else (or ma mb))))

(t/ann ^:no-check item-mask [SpecT SpecInstance -> Mask])
(defn item-mask
  "Builds a mask-map representing a specific sp value,
   where missing keys represent masked out fields and sp objects
   consisting only of a db-ref are considered 'true' valued leaves
   for identity updates only."
  [spec-name sp]
  (let [spec (get-spec spec-name)]
    (if (and (some? spec) (and (some? sp)
                               (if (and (coll? sp) (not (map? sp))) ; union-masks over empty lists would result in the nil mask, but we want an empty list to mean 'true' -- i.e. explicitly empty.
                                 (not-empty sp)
                                 true)))
      (if (:elements spec)
        (let [enum-mask (fn [sp-item]
                          (let [sub-spec-name (:name (get-spec sp-item))]
                            {sub-spec-name (item-mask sub-spec-name sp-item)}))]
          (if (and (coll? sp) (not (map? sp)))
            (reduce union-masks (map enum-mask sp)) ; only recover mask from the particular enum types used by the instance
            (enum-mask sp)))
        (let [field-mask
              , (fn [sp-item]
                  (if (= [:db-ref] (keys sp-item))
                    true
                    (into {} (map (fn [{iname :name [_ sub-spec-name] :type}]
                                    (when (contains? sp-item iname) ; the "parent" 
                                      [iname (item-mask sub-spec-name 
                                                        (get sp-item iname))]))
                                  (:items spec)))))]
          (if (and (coll? sp) (not (map? sp)))
            (reduce union-masks (map field-mask sp)) ; only recover mask from the particular enum types used by the instance
            (field-mask sp))))
      true)))


(t/ann ^:no-check shallow-mask [SpecT -> Mask])
(defn shallow-mask
  "Builds a mask-map of the given spec for consumption by
  build-transactions. Only lets top-level and is-component fields
  through."
  [spec]
  (if (:elements spec)
    (into {} (map #(vector % true) (:elements spec)))
    (->> (for [{iname :name
                [_ typ] :type
                is-component :is-component?
                :as item} (:items spec)]
           [iname
            (if is-component
              (shallow-mask (get-spec typ))
              true)])
         (into {}))))

(t/ann ^:no-check shallow-plus-enums-mask [SpecT -> Mask])
(defn shallow-plus-enums-mask
  "Builds a mask-map of the given spec for consumption by
  build-transactions. Only lets top-level and is-component fields
  through As well, expands toplevel enums and any enum members which
  have only primitive fields(intended to catch common cases like
  'status' enums where the options have no interesting fields)."
  [spec]
  (let [is-leaf? (fn [sp-name]
                   (let [spec (get-spec sp-name)]
                     (and (:items spec)
                          (every? primitive?
                                  (map #(second (:type %)) (:items spec))))))]
    (if (:elements spec)
      (into {} (map #(if (is-leaf? %)
                       [% (shallow-mask (get-spec %))] ; one more step is enough to expand leafy records
                       [% true])
                    (:elements spec)))
      (->> (for [{iname :name
                  [_ typ] :type
                  is-component :is-component?
                  :as item} (:items spec)]
             (let [sub-sp (get-spec typ)]
               [iname
                (if (:elements sub-sp)
                  (shallow-plus-enums-mask sub-sp) ;toplevel enums can be leaf-expanded
                  true)]))
           (into {})))))

(t/ann ^:no-check new-components-mask [SpecInstance SpecT -> Mask])
(defn new-components-mask
  "Builds a mask that specifies only adding entities that don't already
  have eids. Any value with an eid will be treaded as an association and 
  will not result in any updates to the properties of that object in the
  transaction.
  Spec is generated from a particular sp value, not just the value's spec."
  [sp spec]
  (if (:elements spec)
    (let [sub-sp (get-spec sp)]
      {(:name sub-sp) (new-components-mask sp sub-sp)}) ; only need to specify the actual type for the enum branch.
    (if (get-in sp [:db-ref :eid])
      true ;treat as a ref, already in db
      (if-let [spec (get-spec sp)]
        (->> (for [{iname :name [_ type] :type :as item} (:items spec)]
               [iname (new-components-mask (get sp iname) type)])
             (into {}))
        true)))) ;primitive

(t/ann ^:no-check depth-n-mask [SpecT t/AnyInteger -> Mask])
(defn depth-n-mask
  [spec n]
  (if (= n 0)
    (shallow-mask spec)
    (if (:elements spec)
      (into {} (map #(vector % (depth-n-mask (get-spec %) (dec n))) (:elements spec)))
      (->> (for [{iname :name
                  [_ typ] :type
                  is-component :is-component?
                  :as item} (:items spec)]
             [iname
              (if (= (recursiveness item) :rec)
                (depth-n-mask (get-spec typ)
                              (if is-component n (dec n))) ; components don't count as depth-increasing
                true)]) 
           (into {})))))


(declare complete-mask)

(t/ann-datatype CompleteMask [spec :- SpecT])
(t/tc-ignore
(deftype CompleteMask [spec]
  clojure.lang.IPersistentMap
  (assoc [_ k v]
    (throw (ex-info "Mask function not implemented." {:name "assoc"})))
  (assocEx [_ k v]
    (throw (ex-info "Mask function not implemented." {:name "assocEx"})))
  (without [_ k]
    (throw (ex-info "Mask function not implemented." {:name "without"})))
  clojure.lang.Associative
  (containsKey [_ k]
    (if (:elements spec)
      (contains? (:elements spec) k)
      (not-empty (filter #(= (:name %) k) (:items spec)))))
  (entryAt [_ k]
    (if (:elements spec)
      (if (contains? (:elements spec) k)
        (MapEntry. k (complete-mask (get-spec k))))
      (let [{iname :name
             [_ typ] :type
             :as item} (first (filter #(= (:name %) k) (:items spec)))
             is-nested (= :rec (recursiveness item))]
        (MapEntry. k (when (some? item)
                       (if is-nested (complete-mask (get-spec typ)) true))))))
  clojure.lang.ILookup
  (valAt [t k] (when-let [e (.entryAt t k)] (.val e)))
  (valAt [t k default] (if-let [e (.entryAt t k)] (.val e) default)))
)

(t/ann ^:no-check complete-mask [SpecT -> Mask])
(defn complete-mask [spec]
  "Builds a mask-map of the given spec for consumption by
  build-transactions. Recurs down specs to add everything in
  time."
  (CompleteMask. spec))

(t/ann ^:no-check sp->transactions 
       (t/IFn [datomic.db.DbId SpecInstance -> (t/ASeq t/Any)]
              [datomic.db.DbId SpecInstance t/Bool -> (t/ASeq t/Any)]))
(defn sp->transactions
  "Returns a vectors for datomic.api/transact that persist the given
  specced value sp to the database, according to the given db. If
  called with the optional shallow? argument, will persist according
  to the shallow-mask function, otherwise will persist the entire
  datastructure."
  [db sp & [shallow?]]
  (let [deletions (atom '())
        mask ((if shallow? shallow-mask complete-mask) (get-spec sp))
        datomic-data (build-transactions db sp mask deletions)]
    (with-meta
      (cons datomic-data @deletions)
      (meta datomic-data))))

(t/ann ^:no-check commit-sp-transactions 
       [ConnCtx(t/ASeq t/Any) -> Long])
(defn commit-sp-transactions
  "if :transaction-log is specified in conn-ctx (a regular sp object),
   we attach its attributes to the transaction."
  [conn-ctx transaction]
  (let [txn-id (db/tempid :db.part/tx)
        txn-log (when-let [tl (:transaction-log conn-ctx)]
                  (->> (sp->transactions (db/db (:conn conn-ctx)) tl) ; hijack db/id to point to txn.
                       (map #(assoc % :db/id txn-id))))
        tx @(db/transact (:conn conn-ctx) (concat transaction txn-log))
        eid (->> transaction meta :eid)
        entid (db/resolve-tempid (db/db (:conn conn-ctx)) (:tempids tx) eid)]
    (or entid eid)))

(t/ann ^:no-check create-sp!
       [ConnCtx SpecInstance -> Long])
(defn create-sp!
  "aborts if sp is already in db.
   if successful, returns the eid of the newly-added entity."
  [conn-ctx new-sp]
  (let [spec (get-spec new-sp)
        db (db/db (:conn conn-ctx))]
    (assert (not (get-eid db new-sp))
            "object must not already be in the db")
    (check-complete! spec new-sp)
    (commit-sp-transactions conn-ctx (sp->transactions db new-sp))))

(t/ann ^:no-check masked-create-sp!
       [ConnCtx SpecInstance Mask -> Long])
(defn masked-create-sp!
  "Ensures sp is not in the db prior to creating. aborts if so."
  [conn-ctx sp mask]
  (let [spec (get-spec sp)
        _ (check-complete! spec sp)
        db (db/db (:conn conn-ctx))
        _ (assert (not (get-eid db sp))
                  "object must not already be in the db")
        deletions (atom '())
        datomic-data (build-transactions db sp mask deletions)
        txns (with-meta
               (cons datomic-data @deletions)
               (meta datomic-data))]
    (commit-sp-transactions conn-ctx txns)))

(t/ann ^:no-check sp-filter-with-mask
       [Mask SpecT SpecInstance -> (t/Option SpecInstance)])
(defn sp-filter-with-mask
  "applies a mask to a sp instance, keeping only the keys mentioned, 
  (including any relevant :db-id keys)"
  [mask spec-name sp]
  (let [spec (get-spec spec-name)]
    (if (some? spec)
      (let [filter-one
            , (fn [sp]
                (if (= true mask)
                  (reduce (fn [a k] (if (= :db-ref k) a (dissoc a k)))
                          sp (keys sp))
                  (if (map? mask)
                    (if (:elements spec)
                      (let [sub-spec-name (:name (get-spec sp))]
                        (sp-filter-with-mask (get mask sub-spec-name)
                                             sub-spec-name sp))
                      (let [kept-keys (into #{} (keys mask))]
                        (reduce
                         (fn [a k] 
                           (if (kept-keys k)
                             (assoc a k (sp-filter-with-mask 
                                         (get mask k)
                                         (->> (:items spec)
                                              (filter #(= k (:name %)) )
                                              (first)
                                              (:type)
                                              (second))
                                         (get sp k)))
                             (if (= :db-ref k)
                               a
                               (dissoc a k))))
                         sp (keys sp))))
                    nil)))]
        (if (and (coll? sp) (not (map? sp)))
          (map filter-one sp)
          (filter-one sp)))
      (if (= true mask)
        sp
        nil))))

(t/ann ^:no-check update-sp!
       [ConnCtx SpecInstance SpecInstance -> Long])
(defn update-sp!
  "old-sp and new-sp need {:db-ref {:eid eid}} defined.
  Uses the semantics of item-mask, so keys that are not present in
  new-sp won't be updated or checked for comparing old vs new.  

  We may want to consider more conservative transaction semantics,
  i.e.  take the maximum/union of old-sp and new-sp masks, (i.e. if
  some property of new-sp is calculated as a function of other
  proeprties, but those source properties aren't mentioned in the
  sp-new they won't be locked for the update."
  [conn-ctx old-sp new-sp]
  (assert old-sp "must be updating something")
  (let [spec (get-spec old-sp)
        _ (when (not= spec (get-spec new-sp)) (throw (ex-info "old-sp and new-sp have mismatched specs" {:old-spec spec :new-spec (get-spec new-sp)})))
        mask (item-mask (:name spec) new-sp) ;(union-masks (item-mask (:name spec) old-sp) (item-mask (:name spec) new-sp)) ; Summed because we could be adding more "precise" keys etc than were present in old.
        db (db/db (:conn conn-ctx))
        old-eid (get-in old-sp [:db-ref :eid])
        _ (when (not= old-eid (get-in new-sp [:db-ref :eid])) (throw (ex-info "old-sp and new-sp need to have matching eids to update."  {:old-eid old-eid :new-eid (get-in new-sp [:db-ref :eid])})))
        current (sp-filter-with-mask mask (:name spec) (db->sp db (db/entity db old-eid) (:name spec)))]
    (when (not= (sp-filter-with-mask mask (:name spec) old-sp) current) (throw (ex-info "Aborting transaction: old-sp has changed." {:old old-sp :current current})))
    (let [deletions (atom '())
          datomic-data (build-transactions db new-sp mask deletions)
          txns  (with-meta
                  (cons datomic-data @deletions)
                  (meta datomic-data))]
      (commit-sp-transactions conn-ctx txns))))

; TODO consider replocing the old "update-sp" etc with something more explicitly masked like this?
(t/ann ^:no-check masked-update-sp!
       [ConnCtx SpecInstance Mask -> Long])
(defn masked-update-sp!
  "Ensures sp is in the db prior to updating. aborts if not."
  [conn-ctx sp mask]
  (let [db (db/db (:conn conn-ctx))
        _ (assert (db/entity db (get-in sp [:db-ref :eid])) "Entity must exist in DB before updating.")
        deletions (atom '())
        datomic-data (build-transactions db sp mask deletions)
        txns (with-meta
               (cons datomic-data @deletions)
               (meta datomic-data))]
    (commit-sp-transactions conn-ctx txns)))

(t/ann ^:no-check remove-eids [SpecInstance -> SpecInstance])
(defn remove-eids
  "recursively strip all entries of :db-ref {:eid ...} from sp.
   can be used for checking equality with a non-db value."
  [sp]
  (walk/postwalk (fn [m] (if (get-in m [:db-ref :eid])
                           (dissoc m :db-ref)
                           m))
                 sp))

(t/ann ^:no-check remove-identity-items [SpecT SpecInstance -> SpecInstance])
(defn remove-identity-items
  "recursively walks a sp and removes any :unique? or :identity?
  items (as those are harder to test.) We do want to come up with some
  sensible :identity? tests at some point though."
  [spec-name sp]
  (let [spec (get-spec spec-name)]
    (if (and (some? spec) (and (some? sp)
                               (if (and (coll? sp) (not (map? sp)))
                                 (not-empty sp)
                                 true)))
      (if (:elements spec)
        (let [enum-remove (fn [sp-item]
                            (remove-identity-items (:name (get-spec sp-item))
                                                   sp-item))]
          (if (and (coll? sp) (not (map? sp)))
            (into (empty sp) (map enum-remove sp))
            (enum-remove sp)))
        (let [item-remove
              , (fn [sp-item]
                  (reduce 
                   (fn [it {:keys [unique? identity?]
                            iname :name
                            [_ sub-spec-name] :type}]
                     (if (or unique? identity?)
                       (dissoc it iname) ; drop unique fields
                       (assoc it iname
                              (remove-identity-items sub-spec-name
                                                     (get sp-item iname)))))
                   sp-item (:items spec)))]
          (if (and (coll? sp) (not (map? sp)))
            (into (empty sp) (map item-remove sp))
            (item-remove sp))))
      sp)))

; TODO we could make it actually remove the keys instead of nil them, need a helper with some sentinal probably easiest?
; also we probably want to only strip these on nested things, not the toplevel thing. (i.e. THIS is the helper already.)
(t/ann ^:no-check remove-items-with-required [SpecInstance -> SpecInstance])
(defn remove-items-with-required
  "recursively walks a sp and removes any sub things that have
  required fields in the spec. Another tricky-to-test updates helper. (including top-level)"
  [sp]
  (if (and (coll? sp) (not (map? sp)))
    (into (empty sp) (remove #(= % ::remove-me) (map remove-items-with-required sp)))
    (let [spec (get-spec sp)]
      (if (and (some? spec) (and (some? sp)
                                 (if (and (coll? sp) (not (map? sp)))
                                   (not-empty sp)
                                   true)))
        (if (some identity (map :required? (:items spec)))
          ::remove-me           
          (reduce 
           (fn [it [sub-name sub-val]]
             (let [rec-val (remove-items-with-required sub-val)]
               (if (= ::remove-me rec-val)
                 (dissoc it sub-name)
                 (assoc it sub-name
                        rec-val))))
           sp sp))
        sp))))

(t/ann ^:no-check remove-sub-items-with-required [SpecInstance -> SpecInstance])
(defn remove-sub-items-with-required
  "recursively walks a single (map) sp and removes any sub-sps that
  have required fields in the spec (but not the toplevel one, only
  sub-items). Another tricky-to-test updates helper.  Also, remove any
  top-level attributes that are required but have nil values."
  [sp]
  (let [spec (get-spec sp)]
    (assert (nil? (:elements sp)))
    (if (and (some? spec) (and (some? sp)
                               (if (and (coll? sp) (not (map? sp)))
                                 (not-empty sp)
                                 true)))
      (reduce 
       (fn [it {:keys [required?] sub-name :name}] ; [sub-name sub-val]
         (if (contains? sp sub-name)
           (let [rec-val (remove-items-with-required (get sp sub-name))]
             (if (or (and required? (nil? rec-val))
                     (= ::remove-me rec-val))
               (dissoc it sub-name)
               (assoc it sub-name
                      rec-val)))
           it))
       sp (:items spec))
      sp)))

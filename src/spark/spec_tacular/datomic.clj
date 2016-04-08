(ns spark.spec-tacular.datomic
  {:doc "Datomic interface for the spec-tacular DSL"
   :core.typed {:collect-only true}}
  (:refer-clojure :exclude [for remove assoc!])
  (:use spark.spec-tacular.spec
        spark.spec-tacular)
  (:import clojure.lang.MapEntry
           spark.spec_tacular.spec.Spec
           spark.spec_tacular.spec.EnumSpec
           (spark.spec_tacular.spec UnionSpec))
  (:require [clj-time.coerce :as timec]
            [clojure.core.typed :as t :refer [for]]
            [clojure.string :refer [lower-case]]
            [clojure.set :refer [rename-keys difference]]
            [clojure.core.typed.unsafe :refer [ignore-with-unchecked-cast]]
            [clojure.data :as data]
            [clojure.tools.macro :as m]
            [clojure.walk :as walk]
            [clojure.core.match :refer [match]]))

(t/typed-deps spark.spec-tacular)

(require '[datomic.api :as db])

;; -----------------------------------------------------------------------------

(t/defalias ^:no-doc Mask (t/Rec [mask] (t/Map t/Keyword (t/U mask t/Bool))))

(t/defalias Database datomic.db.Db)
(t/defalias DatabaseId datomic.db.DbId)

(t/defalias Connection datomic.peer.LocalConnection)

(t/defalias ConnCtx
  "A connection context.  The only mandatory field is the `:conn`,
  which provides the actual connection to the database.

  Other option fields include:

  * `:transaction-log`, any object which can be converted to Datomic
    transaction data"
  (t/HMap :mandatory {:conn Connection}
          :optional  {:transaction-log t/Any}))

(t/ann ^:no-check datomic.api/entity 
       [datomic.db.Db (t/U Long (t/HVec [t/Keyword t/Any])) -> datomic.query.EntityMap])

(t/ann spark.spec-tacular.test-utils/make-db [(t/Vec t/Any) -> datomic.peer.LocalConnection])
(t/ann spark.spec-tacular.test-utils/db [-> datomic.db.Db])
(t/ann spark.spec-tacular.test-utils/*conn* datomic.peer.LocalConnection)

;; -----------------------------------------------------------------------------

(def ^:no-doc spark-type-attr 
  "The datomic attribute holding onto a keyword-valued SpecName"
  :spec-tacular/spec)

(def ^{:no-doc true :private true} db-type->spec-type
  (reduce (t/ann-form #(assoc %1 %2 (keyword (name %2)))
                      [(t/Map t/Keyword t/Keyword) t/Keyword -> (t/Map t/Keyword t/Keyword)]) {}
                      [:db.type/keyword :db.type/string :db.type/boolean :db.type/long
                       :db.type/bigint :db.type/float :db.type/double :db.type/bigdec
                       :db.type/instant :db.type/uuid :db.type/uri :db.type/bytes]))

(t/ann datomic-ns [SpecT -> t/Str])
(defn ^:no-doc datomic-ns
  "Returns a string representation of the db-normalized namespace for the given spec."
  [spec]
  (some-> spec :name name lower-case))

(t/ann db [ConnCtx -> Database])
(defn db
  "Returns the database in a given connection context."
  [conn-ctx]
  (db/db (:conn conn-ctx)))

(t/ann db-keyword [SpecT (t/U clojure.lang.Named Item) -> t/Keyword])
(defn ^:no-doc db-keyword
  [spec a]
  (t/let [dns (-> spec :name name lower-case)
          make-keyword :- [t/Str -> t/Keyword] #(keyword dns %)]
    (cond
      (instance? clojure.lang.Named a)
      ,(make-keyword (name a))
      (contains? a :name)
      ,(make-keyword (name (:name a)))
      :else (throw (ex-info "cannot make db-keyword" {:spec spec :attr a})))))

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

(t/ann ^:no-check database-coercion [datomic.query.EntityMap -> (t/Map t/Keyword t/Any)])
(t/tc-ignore
 ;; the returned map may be missing valid keys, but it will definitely
 ;; be of the correct spec and won't have completely invalid kws
 (defmethod database-coercion datomic.query.EntityMap [em]
   (coerce-datomic-entity em)))

(t/ann ^:no-check get-all-eids [datomic.db.Db SpecT -> (t/ASeq Long)])
(defn ^:no-doc get-all-eids
  "Retrives all of the eids described by the given spec from the database."
  [db spec]
  (t/let [mk-db-kw :- [Item -> t/Keyword] #(db-keyword spec %)
          names (map mk-db-kw (:items spec))
          query '[:find ?eid :in $ [?attr ...] :where [?eid ?attr ?val]]
          ref->eid :- [(t/Vec t/Any) -> Long] 
          ,#(let [r (first %)] (do (assert (instance? Long r)) r))]
    (map ref->eid (db/q query db names))))

(t/ann get-eid (t/IFn 
                [Database SpecInstance -> (t/Option Long)]
                [Database SpecInstance SpecT -> (t/Option Long)]))
(defn ^:no-doc get-eid
  "Returns an EID associated with the data in the given spark type if
  it exists in the database. Looks up according to identity
  items. Returns nil if not found."
  ([db sp] 
   (when (map? sp)
     (when-let [spec (get-spec sp)]
       (get-eid db sp spec))))
  ([db sp spec]
   (let [eid (or (get-in sp [:db-ref :eid]) (:eid (meta sp)))]
     (assert (or (nil? eid) (instance? Long eid)))
     (t/ann-form ; Seems like 'if' isn't typechecked correctly w/o annotation?
      (or eid
          (if-let [id (some (t/ann-form
                             #(if-let [id (and (or (:identity? %) (:unique? %))
                                               (get sp (:name %)))]
                                [(db-keyword spec (:name %)) id])
                             [Item -> (t/Option (t/HVec [t/Keyword t/Any]))])
                            (:items spec))]
            (if-let [em (db/entity db id)]
              (:db/id em))))
      (t/Option Long)))))

(t/ann ^:no-check db->sp [Database (t/Map t/Any t/Any) -> SpecInstance])
(defn ^:no-doc db->sp ;; TODO -- this function is pointless now
  [db ent & [sp-type]]
  (if-not ent
    nil
    (let [eid (:db/id ent)
          ent (into {} ent)
          spec (get-spec (spark-type-attr ent))
          ctor (get-ctor (:name spec))
          reduce-attr->kw #(assoc %1 (db-keyword spec %2) (-> %2 name keyword))
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

(t/ann ^:no-check get-by-eid (t/IFn [datomic.db.Db Long -> SpecInstance]
                                    [datomic.db.Db Long SpecT -> SpecInstance]))
(defn ^:no-doc get-by-eid ;; TODO -- clean up uses of this function in user code
  "fetches the entire SpecInstance from the db for the given eid
   throws IllegalArgumentException when eid isn't found."
  [db eid & [sp-type]]
  (do (assert (instance? java.lang.Long eid) 
              (str eid " is not an eid"))
      (db->sp db (db/entity db eid) sp-type)))

(defmacro get-all-by-spec
  "Returns all the entities in the database with the given spec.

  If the spec is a keyword at compile-time, the resulting entity is
  cast to the correct type.  Otherwise, the resulting entity is a
  generic [[SpecInstance]]"
  [db spec]
  `(let [spec# (get-spec ~spec)
         db# ~db
         _# (when-not spec#
              (throw (ex-info (str "Could not find spec for " ~spec) {:syntax '~spec})))
         _# (when-not (instance? datomic.db.Db db#)
              (throw (ex-info (str "Expecting database") {:given db#})))
         eids# (get-all-eids ~db spec#)
         eid->si# (clojure.core.typed.unsafe/ignore-with-unchecked-cast
                   (fn [eid#] (recursive-ctor (:name (get-spec ~spec)) (db/entity ~db eid#)))
                   [Long ~'-> ~(if (keyword? spec) (:type-symbol (get-type spec))
                                   `SpecInstance)])]
     (map eid->si# eids#)))

(defn count-all-by-spec
  "Returns the number of entities with the given spec in the database."
  [db spec]
  (assert (keyword? spec) "expecting spec name")
  (assert (instance? datomic.db.Db db) "expecting database")
  (or (ffirst (db/q {:find ['(count ?eid)] :in '[$] :where [['?eid :spec-tacular/spec spec]]} db)) 0))

(t/ann ^:no-check build-transactions
       (t/IFn [datomic.db.Db SpecInstance Mask (t/Atom1 (t/ASeq (t/Vec t/Any))) -> (t/Map t/Keyword t/Any)]
              [datomic.db.Db SpecInstance Mask (t/Atom1 (t/ASeq (t/Vec t/Any))) SpecT -> (t/Map t/Keyword t/Any)]))
(defn ^:no-doc build-transactions
  "Builds a nested datomic-data datastructure for the sp data, only
  for what's specified in the mask. Adds Datomicy deletion commands to
  the given atomic list of deletions when appropriate."
  [db sp mask deletions & [spec]]
  (let [spec (or spec (get-spec sp))
        [spec mask] (if (:elements spec) ; Need to pick which union branch to pick in the mask.
                      (let [sub-name (:name (get-spec sp))]
                        [(get-spec (get-in spec [:elements sub-name]))
                         (get mask sub-name)])
                      [spec mask])
        eid (get-eid db sp)
        db-value (and eid (get-by-eid db eid (:name spec)))
        eid (or eid (db/tempid :db.part/user))]
    (->> (for [{iname :name 
                [cardinality type] :type 
                required? :required?
                link? :link?
                :as item}
               (:items spec)
               :when (iname mask)
               :let [is-nested (= (recursiveness item) :rec)
                     is-many (= cardinality :many)
                     ival (iname sp)
                     ival (if (or link? (not (:db-ref ival))) ival (dissoc ival :db-ref))
                     sub-spec (get-spec type) ; Not necessarily ival's spec: could be an union.
                     mask (iname mask)
                     ival-db (iname db-value)
                     datomic-key (keyword (datomic-ns spec) (name iname))
                     retract (fn [r] 
                               (if required?
                                 (throw (ex-info "attempt to delete a required field"
                                                 {:item item :field iname :spec spec}))
                                 [:db/retract eid datomic-key
                                  (or (get-in r [:db-ref :eid]) r)]))]]
           (do
             [datomic-key
              (if is-nested
                (if (map? mask)
                  (if is-many
                    (let [old-eids (set (map (partial get-eid db) ival-db))
                          new-eids (set (map (partial get-eid db) ival))
                          [_ deletes _] (data/diff new-eids old-eids)]
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
                          [adds deletes _] (data/diff new-eids old-eids)]
                      (swap! deletions concat (map retract deletes))
                      adds)
                    (if (some? ival)
                      (get-eid db ival)
                      (if (some? ival-db)
                        (do (swap! deletions conj (retract ival-db)) nil)
                        nil))))
                (if is-many
                  (let [[adds deletes] (data/diff ival ival-db)]
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
(defn ^:no-doc union-masks
  "union (join) taken w.r.t. a lattice of 'specificity' eg -- nil < true < {:item ...} 
   (recall 'true' means the mask consisting only of the db-ref)
   keys are combined and their values are recursively summed.
   Unions and Records can be summed the same, as we represent both with maps."
  ([] nil)
  ([m] m)
  ([ma mb]
     (cond
      (and (map? ma) (map? mb))
      (merge-with union-masks ma mb)
      (map? ma) ma
      (map? mb) mb
      :else (or ma mb))))

(t/ann ^:no-check shallow-mask [SpecT -> Mask])
(defn ^:no-doc shallow-mask
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

(t/ann ^:no-check shallow-plus-unions-mask [SpecT -> Mask])
(defn ^:no-doc shallow-plus-unions-mask
  "Builds a mask-map of the given spec for consumption by
  build-transactions. Only lets top-level and is-component fields
  through As well, expands toplevel unions and any union members which
  have only primitive fields(intended to catch common cases like
  'status' unions where the options have no interesting fields)."
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
                  (shallow-plus-unions-mask sub-sp) ;toplevel unions can be leaf-expanded
                  true)]))
           (into {})))))

(t/ann ^:no-check new-components-mask [SpecInstance SpecT -> Mask])
(defn ^:no-doc new-components-mask
  "Builds a mask that specifies only adding entities that don't already
  have eids. Any value with an eid will be treaded as an association and 
  will not result in any updates to the properties of that object in the
  transaction.
  Spec is generated from a particular sp value, not just the value's spec."
  [sp spec]
  (if (:elements spec)
    (let [sub-sp (get-spec sp)]
      {(:name sub-sp) (new-components-mask sp sub-sp)}) ; only need to specify the actual type for the union branch.
    (if (get-in sp [:db-ref :eid])
      true ;treat as a ref, already in db
      (if-let [spec (get-spec sp)]
        (->> (for [{iname :name [_ type] :type :as item} (:items spec)]
               [iname (new-components-mask (get sp iname) type)])
             (into {}))
        true)))) ;primitive

(t/ann ^:no-check depth-n-mask [SpecT t/AnyInteger -> Mask])
(defn ^:no-doc depth-n-mask
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
(deftype ^:no-doc CompleteMask [spec]
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
  (valAt [t k default] (if-let [e (.entryAt t k)] (.val e) default))))

(alter-meta! #'->CompleteMask assoc :no-doc true)

(t/ann ^:no-check complete-mask [SpecT -> Mask])
(defn ^:no-doc complete-mask [spec]
  "Builds a mask-map of the given spec for consumption by
  build-transactions. Recurs down specs to add everything in
  time."
  (CompleteMask. spec))

(t/ann ^:no-check sp->transactions 
       (t/IFn [datomic.db.DbId SpecInstance -> (t/ASeq t/Any)]
              [datomic.db.DbId SpecInstance t/Bool -> (t/ASeq t/Any)]))
(defn ^:no-doc sp->transactions
  "Returns a vector for datomic.api/transact that persist the given
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

(t/ann ^:no-check commit-sp-transactions!
       [ConnCtx (t/ASeq t/Any) -> Long])
(defn ^:no-doc commit-sp-transactions!
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
    (or entid eid (:tempids tx))))

(t/ann ^:no-check create-sp! [ConnCtx SpecInstance -> Long])
(defn ^:no-doc create-sp!
  "aborts if sp is already in db.
   if successful, returns the eid of the newly-added entity."
  [conn-ctx new-sp]
  (let [spec (get-spec new-sp)
        db (db/db (:conn conn-ctx))]
    (assert (not (get-eid db new-sp))
            "object must not already be in the db")
    (commit-sp-transactions! conn-ctx (sp->transactions db new-sp))))

(t/ann ^:no-check masked-create-sp!
       [ConnCtx SpecInstance Mask -> Long])
(defn ^:no-doc masked-create-sp!
  "Ensures sp is not in the db prior to creating. aborts if so."
  [conn-ctx sp mask]
  (let [spec (get-spec sp)
        db (db/db (:conn conn-ctx))
        _ (assert (not (get-eid db sp))
                  "object must not already be in the db")
        deletions (atom '())
        datomic-data (build-transactions db sp mask deletions)
        txns (with-meta
               (cons datomic-data @deletions)
               (meta datomic-data))]
    (commit-sp-transactions! conn-ctx txns)))

(t/ann ^:no-check sp-filter-with-mask
       [Mask SpecT SpecInstance -> (t/Option SpecInstance)])
(defn ^:no-doc sp-filter-with-mask
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
          (filter identity (map filter-one sp)) ;; don't let nils thru
          (filter-one sp)))
      (if (= true mask)
        sp
        nil))))

; TODO consider replocing the old "update-sp" etc with something more explicitly masked like this?
(t/ann ^:no-check masked-update-sp!
       [ConnCtx SpecInstance Mask -> Long])
(defn ^:no-doc masked-update-sp!
  "Ensures sp is in the db prior to updating. aborts if not."
  [conn-ctx sp mask]
  (let [db (db/db (:conn conn-ctx))
        _ (assert (db/entity db (get-in sp [:db-ref :eid])) "Entity must exist in DB before updating.")
        deletions (atom '())
        datomic-data (build-transactions db sp mask deletions)
        txns (with-meta
               (cons datomic-data @deletions)
               (meta datomic-data))]
    (commit-sp-transactions! conn-ctx txns)))

(t/ann ^:no-check remove-eids [SpecInstance -> SpecInstance])
(defn ^:no-doc remove-eids
  "recursively strip all entries of :db-ref {:eid ...} from sp.
   can be used for checking equality with a non-db value."
  [sp]
  (walk/postwalk (fn [m] (if (get m :db-ref) (dissoc m :db-ref) m)) sp))

(t/ann ^:no-check remove-identity-items [SpecT SpecInstance -> SpecInstance])
(defn ^:no-doc remove-identity-items
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
        (let [union-remove (fn [sp-item]
                            (remove-identity-items (:name (get-spec sp-item))
                                                   sp-item))]
          (if (and (coll? sp) (not (map? sp)))
            (into (empty sp) (map union-remove sp))
            (union-remove sp)))
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
(defn ^:no-doc remove-items-with-required
  "recursively walks a sp and removes any sub things that have
  required fields in the spec. Another tricky-to-test updates helper. (including top-level)"
  [sp]
  (if (and (coll? sp) (not (map? sp)))
    (into (empty sp) (clojure.core/remove #(= % ::remove-me) (map remove-items-with-required sp)))
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

;; TODO probably shouldnt live here
(t/ann ^:no-check remove-sub-items-with-required [SpecInstance -> SpecInstance])
(defn ^:no-doc remove-sub-items-with-required
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



;; =============================================================================
;; query

(t/defalias ^:no-doc QueryIdent  (t/U t/Keyword t/Sym '[t/Sym t/Keyword]))
(t/defalias ^:no-doc QueryClause (t/U '[QueryIdent QueryMap] '[(t/List t/Any)]))
(t/defalias ^:no-doc QueryMapVal (t/U QueryIdent QueryClause QueryMap))
(t/defalias ^:no-doc QueryMap    (t/Map t/Keyword QueryMapVal))
(t/defalias ^:no-doc QueryMapVec '[t/Keyword QueryMapVal])

(t/defalias ^:no-doc QueryUEnv   (t/Atom1 (t/Map t/Sym t/Sym)))
(t/defalias ^:no-doc QueryTEnv   (t/Atom1 (t/Map t/Sym t/Keyword)))

(t/defalias ^:no-doc DatomicWhereClause '[t/Any t/Keyword t/Any])

(t/ann set-type! [QueryTEnv t/Sym t/Keyword -> nil])
(defn- set-type! [tenv x t]
  (if-let [t- (get @tenv x)]
    (when-not (or (= t t-)
                  (if-let [elems (:elements (get-spec t))]
                    (contains? elems t-)))
      (throw (ex-info "retvars has two return types" {:type1 t :type2 t-})))
    (do (swap! tenv assoc x t) nil)))

(t/ann expand-ident [QueryIdent QueryUEnv QueryTEnv ->
                     (t/HMap :mandatory {:var t/Sym :spec SpecT})])
(defn- expand-ident [ident uenv tenv]
  "takes an ident and returns a map with a unique variable and its intended spec"
  (cond
    (keyword? ident)
    ,(let [uid (gensym (str "?" (lower-case (name ident))))]
       (expand-ident [uid ident] uenv tenv))
    (symbol? ident)
    ,(let [spec-name (get @tenv ident)]
       (when-not spec-name
         (throw (ex-info (str "could not infer type for " ident)
                         {:syntax ident :tenv @tenv})))
       (expand-ident [ident spec-name] uenv tenv))
    (vector? ident)
    ,(let [[var spec-name] ident
           spec (get-spec spec-name)]
       (when-not spec
         (throw (ex-info (str "could not find spec for " spec-name)
                         {:syntax ident})))
       {:var var :spec spec})
    :else (throw (ex-info (str (type ident) " unsupported ident") {:syntax ident}))))

(declare expand-clause expand-map)
(t/ann ^:no-check expand-item [Item t/Keyword QueryMapVal t/Sym QueryUEnv QueryTEnv -> t/Any])
(defn- expand-item [item db-kw rhs x uenv tenv]
  (let [{[arity sub-spec-name] :type} item
        mk-where-clause (fn [rhs] [`'~x db-kw rhs])]
    (cond
      (::patvar (meta rhs))
      ,(do (set-type! tenv rhs sub-spec-name)
           (vec (concat (if-let [spec (get-spec (get @tenv rhs))]
                          (when (instance? Spec spec)
                            [[`'~rhs :spec-tacular/spec (get @tenv rhs)]])
                          [])
                        [(mk-where-clause `'~rhs)])))
      (and (keyword? rhs)
           (get-spec rhs))
      ,(t/let [y (gensym "?tmp")]
         [(mk-where-clause `'~y)
          [`'~y :spec-tacular/spec rhs]])
      (map? rhs)
      ,(t/let [y (gensym "?tmp")
               sub-atmap :- QueryMap rhs]
         `(conj ~(expand-map sub-atmap y (get-spec sub-spec-name) uenv tenv)
                ~(mk-where-clause `'~y)))
      (nil? rhs)
      ,[[(list 'list ''missing? ''$ `'~x db-kw)]]
      (vector? rhs)
      ,(t/let [y (gensym "?tmp"), k (gensym "?kw")
               [l r] rhs
               spec (get-spec sub-spec-name)
               spec (if (:elements spec)
                      (get-spec sub-spec-name (get @tenv l))
                      spec)]
         (cond
           (and (keyword? l)
                (get-spec l))
           `(conj ~(expand-clause [[y l] r] uenv tenv)
                  ~(mk-where-clause `'~y))
           (and (keyword? l)
                (primitive? l))
           ,(do (set-type! tenv r l)
                (expand-item item db-kw r x uenv tenv))
           (and (::patvar (meta l)) (:items spec))
           ,(do (set-type! tenv l sub-spec-name)
                `(conj ~(expand-clause [[l (:name spec)] r] uenv tenv)
                       ~(mk-where-clause `'~l)))
           (and (::patvar (meta l)) (::patvar (meta r)))
           ,(do (set-type! tenv l :keyword)
                (set-type! tenv r (:name spec))
                [[`'~x db-kw `'~r]
                 [`'~r :spec-tacular/spec `'~l]])
           :else ;; fall back to dynamic resolution
           (throw (ex-info "invalid item" {:syntax rhs}))))
      :else
      ;; We have a "thing" that eventually resolves into a value.
      ,(let [sub-spec (get-spec sub-spec-name)
             y (gensym "?tmp")
             z (gensym)]
         `(let [~z ~(if (= sub-spec-name :calendarday) `(timec/to-date ~rhs) rhs)
                rhs-spec# (get-spec ~z)]
            (when (nil? ~z)
              (throw (ex-info "Maps cannot have nil values at runtime"
                              {:field ~sub-spec-name :syntax '~rhs})))
            (if (and rhs-spec# (instance? Spec rhs-spec#)) ;; Spec
              (if-let [eid# (get-in ~z [:db-ref :eid])]
                [['~x ~db-kw eid#]]
                (let [item# (some #(if (:unique? %) %) (:items rhs-spec#))
                      val#  (and item# ((:name item#) ~z))]
                  (if (and item# (some? val#))
                    [['~x ~db-kw '~y]
                     ['~y (db-keyword rhs-spec# (:name item#)) val#]]
                    (throw (ex-info "Cannot uniquely describe the given value"
                                    {:syntax ~rhs :value ~z})))))
              ~[(mk-where-clause z)])))))) ;; EnumSpec; everything else

;; map = {:kw (ident | clause | map | value),+}
(t/ann ^:no-check expand-map [QueryMap t/Sym SpecT QueryUEnv QueryTEnv -> t/Any])
(defn- expand-map [atmap x spec uenv tenv]
  (when-not (map? atmap)
    (throw (ex-info "invalid map" {:syntax atmap})))
  (let [{:keys [elements items]} spec]
    (if items
      (-> (fn [[kw rhs]]
            (let [item (get-item spec kw)
                  db-keyword (db-keyword spec kw)]
              (cond
                item
                `(concat [['~x :spec-tacular/spec ~(:name spec)]]
                         ~(expand-item item db-keyword rhs x uenv tenv))
                (= kw :spec-tacular/spec)
                (do (set-type! tenv rhs :keyword)
                    [[`'~x :spec-tacular/spec `'~rhs]])
                :else (throw (ex-info "could not find item" {:syntax atmap :field kw})))))
          (keep atmap) (doall) (conj `concat))
      (let [maybe-spec (:spec-tacular/spec atmap)]
        (if (and (keyword? maybe-spec) (contains? elements maybe-spec))
          `(conj ~(expand-map (dissoc atmap :spec-tacular/spec) x (get-spec maybe-spec) uenv tenv)
                 ['~x :spec-tacular/spec ~maybe-spec])
          (let [try-all (-> #(try {% (expand-map (dissoc atmap :spec-tacular/spec) x
                                                 (get-spec %) uenv tenv)}
                                  (catch clojure.lang.ExceptionInfo e {% e}))
                            (keep elements))
                try-map (into {} try-all)
                grouped (group-by
                         #(if (or (= (type (second %)) clojure.lang.PersistentList)
                                  (= (type (second %)) clojure.lang.Cons))
                            :syntax :error)
                         try-map)]
            (when (empty? (get grouped :syntax))
              (throw (ex-info "does not conform to any possible unioned spec"
                              {:syntax atmap :possible-specs elements
                               :errors (get grouped :error)})))
            (cond
              (::patvar (meta maybe-spec))
              ,(let [opts (into {} (get grouped :syntax))]
                 (set-type! tenv maybe-spec :keyword)
                 `(concat
                   [['~x :spec-tacular/spec '~maybe-spec]]
                   (let [opts# ~opts]
                     (if (every? empty? (vals opts#)) []
                         [(list ~''or ~@(map (fn [[kw stx]] `(~'cons ~''and (~'concat [[(~'list ~''ground ~kw) '~maybe-spec]] ~stx)))
                                             opts))]))))
              (symbol? maybe-spec)
              `(let [opts# ~(into {} (get grouped :syntax))
                     spec# ~maybe-spec]
                 (concat [['~x :spec-tacular/spec spec#]]
                         (or (get opts# spec#)
                             (throw (ex-info "does not conform to any possible unioned spec"
                                             {:computed-spec spec#
                                              :available-specs (keys opts#)})))))
              (and (nil? maybe-spec) (nil? (get grouped :error)))
              `(let [opts# ~(into {} (get grouped :syntax))]
                 [(cons ~''or (map (fn [[kw# stx#]] (cons ~''and stx#)) opts#))])
              :else (throw (ex-info "Query syntax not supported"
                                    {:syntax atmap :errors (get grouped :error)})))))))))

(t/ann ^:no-check expand-clause [QueryClause QueryUEnv QueryTEnv -> t/Any]) ;; meta
(defn- expand-clause [clause uenv tenv]
  (cond
    (vector? clause)
    ,(cond
       (= (count clause) 1)
       ,[[(->> (rest (first clause))
               (map (fn [x] (if (::patvar (meta x))
                              `(with-meta '~x ~(merge (meta x) {:tag `'~(:type-symbol (get-type (get @tenv x)))}))
                              x)))
               (cons `'~(ffirst clause))
               (cons 'list))]]
       (= (count clause) 2)
       ,(let [[ident atmap] clause
              {:keys [var spec]} (expand-ident ident uenv tenv)]
          (cond
            (map? atmap) (expand-map atmap var spec uenv tenv)
            :else (throw (ex-info "Invalid clause rhs" {:syntax clause})))))
    (seq? clause)
    ,(let [[head & clauses] clause]
       (case head
         'not-join
         ,(let [[vars & clauses] clauses
                clauses (map #(expand-clause % uenv tenv) clauses)]
            `[(cons ~''not-join (cons '~vars (concat ~@clauses)))])
         :else (throw (ex-info "unsupported sequence head"
                               {:syntax clause}))))
    :else (throw (ex-info "invalid clause type, expecting vector or sequence"
                          {:type (type clause) :syntax clause}))))

(t/ann annotate-retvars! 
       [(t/List (t/U t/Sym t/Keyword)) QueryUEnv QueryTEnv -> t/Any])
(defn- annotate-retvars! [rets uenv tenv]
  (t/let [n :- (t/Atom1 t/Num) 
          ,(atom 1) 
          mk-new :- [(t/U t/Str t/Sym) -> t/Sym] 
          ,#(with-meta (gensym (str "?" %)) {::patvar true})
          swap-keyword! :- [t/Keyword -> t/Sym]
          ,(fn [spec]
             (let [%   (symbol (str "%" @n))
                   new (mk-new (lower-case (name spec)))]
               (set-type! tenv new spec)
               (swap! n inc)
               (swap! uenv assoc % new)
               new))
          swap-symbol! :- [t/Sym -> t/Sym]
          ,(fn [old] 
             (let [new (mk-new old)]
               (swap! uenv assoc old new) new))
          swap-retvar! :- [(t/U t/Keyword t/Sym) -> t/Sym]
          ,(fn [r]
             (cond 
               (keyword? r) (swap-keyword! r)
               (symbol? r)  (swap-symbol! r)))]
    (doall (map swap-retvar! rets))))

(t/ann annotate-patvars! [(t/List QueryClause) QueryUEnv QueryTEnv -> t/Any])
(defn- annotate-patvars! [clauses uenv tenv]
  (t/let [mk-new :- [(t/U t/Str t/Sym) -> t/Sym]
          ,#(with-meta (gensym (str "?" %)) {::patvar true})
          annotate-clause! :- [QueryClause -> t/Any]
          ,(fn [[id atmap]]
             (cond
               (symbol? id)
               ,(let [gs (mk-new id)]
                  (swap! uenv assoc id gs))
               (vector? id)
               ,(let [gs (mk-new (first id))]
                  (do (set-type! tenv gs (second id))
                      (swap! uenv assoc (first id) gs)))))]
    (doall (map annotate-clause! clauses))))

(t/ann ^:no-check desugar-query ; apply & macroexpand dont typecheck
       [(t/List t/Sym) (t/List QueryClause) QueryUEnv QueryTEnv 
        -> (t/HMap :mandatory {:rets (t/List t/Sym) :clauses (t/List QueryClause)})])
(defn- desugar-query [rets clauses uenv tenv]
  (let [rets     (annotate-retvars! rets uenv tenv)
        bindings (apply concat '[% %1] (vec @uenv))
        do-expr  (m/mexpand `(m/symbol-macrolet ~bindings ~clauses))
        _        (annotate-patvars! clauses uenv tenv)]
    {:rets rets :clauses (second do-expr)}))

(t/ann ^:no-check expand-query 
       [(t/List t/Sym) (t/List QueryClause) ->
        (t/HMap :mandatory {:args (t/List t/Sym) :env QueryTEnv :clauses t/Any})])
(defn- expand-query [f wc]
  (t/let [tenv :- QueryTEnv (atom {}) 
          uenv :- QueryUEnv (atom {})]
    (let [{:keys [rets clauses]} (desugar-query f wc uenv tenv)
          clauses (doall (map (t/ann-form
                               #(expand-clause % uenv tenv) ;; side effects
                               [QueryClause -> t/Any])
                              clauses))
          clauses `(concat ~@clauses)]
      (assert (= (count rets) (count f)) "internal error")
      {:args rets :env @tenv :clauses clauses})))

;; (q :find find-expr+ :in clojure-expr :where clause+)
;; find-expr = ident
;; ident     = spec-name
;;           | datomic-variable
;;             | [datomic-variable spec-name]
;; clause    = [ident map]
;; map       = % | %n | spec-name
;;           | {:kw (clause | map | ident | value),+}
(t/tc-ignore ;; only called from inside a macro; TODO type
 (defn- parse-query [stx]
   (let [keywords [:find :in :where]
         partitions (partition-by (fn [stx] (some #(= stx %) keywords)) stx)]
     (match partitions ;; ((:find) (1 2 ....) (:in) (3) (:where) (4 5 ....))
       ([([:find] :seq) f ([:in] :seq) db ([:where] :seq) wc] :seq)
       (do (when-not (or (every? #(or (symbol? %) (keyword? %)) f)
                         (match (first f) ([v '...] :seq) true :else false))
             (throw (ex-info "expecting sequence of symbols and keywords, or collection syntax"
                             {:syntax f})))
           (when-not (= (count db) 1)
             (throw (ex-info "expecting exactly one database expression" {:syntax db})))
           (when-not (every? #(or (vector? %) (list? %)) wc)
             (throw (ex-info "expecting sequence of vectors or lists" {:syntax wc})))
           ;; TODO -- can do more syntax checking here
           {:f (match (first f) ([v '...] :seq) [v] :else f)
            :coll? (match (first f) ([v '...] :seq) true :else false)
            :db (first db)
            :wc wc})
       :else
       (throw (ex-info "expecting keywords :find, :in, and :where followed by arguments"
                       {:syntax partitions}))))))

(defmacro q
  "Returns a set of results from the Datomic query.  See [the
  README](https://github.com/SparkFund/spec-tacular/tree/v0.5.0#querying-databases)
  for examples.

  ```
  (q :find FIND-EXPR+ :in expr :where CLAUSE+)

  FIND-EXPR = IDENT 
            | [IDENT ...]
  IDENT     = SpecName
            | ?variable
            | [SpecName ?variable]
  CLAUSE    = [IDENT MAP]
            | [(symbol (SpecName | ?variable)+)]
  MAP       = % | %n | SpecName
            | {keyword (CLAUSE | MAP | IDENT | expr),+}
  ```

  The `FIND-EXPR` are used as the `:find` arguments to the Datomic
  query.

  The value of `:in` is used as the database; no other arguments to
  `:in` are allowed at the moment.

  Each `:where` clause is expanded to one or more Datomic clauses.
  The `[IDENT MAP]` form finds `IDENT`s with the fields given in
  `MAP`.  The other clause syntax provides very experimental support
  for a subset of Datomic [\"Function
  Expressions\"](http://docs.datomic.com/query.html) -- more coming
  soon.

  Using `%` and `%n` is analogous to `#` and `%` in Clojure: `%`
  inserts the first SpecName in the `FIND-EXPR` if there is only one,
  or `%1` references the first, and `%2` the second, etc.

  Every `SpecName` must be a keyword at compile-time; to dynamically
  get the spec of an entity use the special keyword
  `:spec-tacular/spec` as an entry in any `MAP`."
  [& stx]
  (let [{:keys [f db wc coll?]} (parse-query stx)
        {:keys [args env clauses]} (expand-query f wc)
        type-kws  (map #(get env %) args)
        type-maps (map get-type type-kws)
        type-syms (map :type-symbol type-maps)
        err (fn [result t-s]
              `(throw (ex-info "possible spec mismatch"
                               {:actual-type   (type '~result)
                                :expected-type '~t-s
                                :query-result  ~result})))
        wrap (fn [result t-kw t-m t-s]
               (when-not (and t-kw t-m t-s)
                 (throw (ex-info (str "missing information about " result)
                                 {:type t-kw :type-sym t-s :syntax stx})))
               (if (and (primitive? t-kw)
                        (not (instance? EnumSpec (get-spec t-kw))))
                 (if (= t-kw :calendarday)
                   `(if (instance? java.util.Date ~result) (timec/to-date-time ~result)
                        ~(err result t-s))
                   `(if (instance? ~t-s ~result) ~result
                        ~(err result t-s)))
                 `(if (instance? java.lang.Long ~result)
                    (let [e# (clojure.core.typed.unsafe/ignore-with-unchecked-cast
                              (db/entity ~db ~result) ~t-s)]
                      (recursive-ctor ~t-kw e#))
                    ~(err result t-s))))]
    `(t/let [check# :- [~(if coll? `t/Any `(t/Vec t/Any)) ~'->
                        ~(if coll? (first type-syms) `(t/HVec ~(vec type-syms)))]
             ~(if coll?
                `(fn [~(first args)]
                   ~(wrap (first args) (first type-kws) (first type-maps) (first type-syms)))
                `(fn [~(vec args)]
                   [~@(map wrap args type-kws type-maps type-syms)]))]
       (->> (clojure.core.typed.unsafe/ignore-with-unchecked-cast
             (db/q {:find ~(if coll? `'([~(first args) ...]) `'~args)
                    :in '~['$]
                    :where (distinct ~clauses)}
                   ~db)
             (t/Seqable ~(if coll? `t/Any `(t/Vec t/Any))))
            (map check#) (set)))))

;; =============================================================================
;; database interfaces

(declare transaction-data)
(t/defalias ^:no-doc TransactionData (t/List (t/HVec [t/Keyword Long t/Keyword t/Any])))

(t/ann ^:no-check transaction-data-item
       [SpecT Long t/Any Item t/Any t/Any -> TransactionData])
(defn ^:no-doc transaction-data-item
  [parent-spec parent-eid txn-fns
   {iname :name required? :required? link? :link? [cardinality type] :type :as item}
   old new & [tmps]]
  (let [datomic-key (keyword (datomic-ns parent-spec) (name iname))]
    (letfn [(add [i] ;; adds i to field datomic-key in entity eid
              (when-not (some? i)
                (throw (ex-info "cannot add nil" {:spec (:name parent-spec) :old old :new new})))
              (if-let [sub-eid (and (or link? (:component? item)) ; not by value
                                    (or (get-in i [:db-ref :eid])
                                        (and tmps (some (fn [[k v]]
                                                          (and (identical? k i) v))
                                                        @tmps))))]
                ;; adding by reference
                [[:db/add parent-eid datomic-key sub-eid]]
                ;; adding by value
                (if (= (recursiveness item) :non-rec)
                  (let [sub-spec (get-spec type)
                        i (if (= type :calendarday) (timec/to-date i) i)]
                    (if-let [fn (get-in txn-fns [parent-spec iname])]
                      [[fn parent-eid datomic-key i]]
                      [[:db/add parent-eid datomic-key i]]))
                  (let [sub-eid  (db/tempid :db.part/user)
                        _ (when (and tmps link?)
                            (swap! tmps conj [i sub-eid]))
                        sub-spec (get-spec type)
                        sub-spec (if (:elements sub-spec)
                                   (get-spec i) sub-spec)]
                    (concat (when (and (:component? item) (= cardinality :one) (some? old))
                              [[:db.fn/retractEntity (get-in old [:db-ref :eid])]])
                            [[:db/add parent-eid datomic-key sub-eid]
                             [:db/add sub-eid :spec-tacular/spec (:name sub-spec)]]
                            (transaction-data txn-fns sub-spec {:db-ref {:eid sub-eid}} i tmps))))))
            (retract [i] ;; removes i from field datomic-key in entity eid
              (when (not (some? i))
                (throw (ex-info "cannot retract nil"
                                {:spec (:name parent-spec) :old old :new new})))
              (when required?
                (throw (ex-info "attempt to delete a required field"
                                {:item item :field iname :spec parent-spec})))
              (if-let [eid (get-in i [:db-ref :eid])]
                (if (:component? item)
                  [[:db.fn/retractEntity eid]]
                  [[:db/retract parent-eid datomic-key eid]])
                (do (let [sub-spec (get-spec type)]
                      (when (and link? (not (instance? EnumSpec sub-spec)))
                        (throw (ex-info "retracted link missing eid" {:entity i}))))
                    (let [i (if (= type :calendarday) (timec/to-date i) i)]
                      [[:db/retract parent-eid datomic-key i]]))))]
      (cond
        (= cardinality :one)
        ,(cond
           (some? new) (add new)
           (some? old) (retract old)
           :else [])
        (= (recursiveness item) :non-rec)
        ,(do (when-not (apply distinct? nil new)
               (throw (ex-info "adding identical" {:new new})))
             (let [[adds deletes both] (data/diff (set new) (set old))]
               (concat (mapcat retract deletes)
                       (mapcat add adds))))
        :else
        ;; this is a bit tricky:
        ;; -- group things from old and new by eid, resulting in
        ;;      {123 [<entity1>], 456 [<entity1> <entity2>], ....
        ;;       (gensym) [<new-entity>], ....}
        ;; -- if there are two things in the list, do nothing
        ;; -- if there is one thing in the list, either remove or add depending
        ;;    on which group (old or new) the entity came from
        ;; -- new entities won't have eids, so just give them something unique
        ;;    to key on and add them
        (let [by-eids (group-by #(get-in % [:db-ref :eid] (gensym)) (concat old new))]
          (->> (for [[_ [e1 & [e2]]] by-eids]
                 (if e2 []
                     (if (some #(identical? e1 %) old)
                       (retract e1) (add e1))))
               (apply concat)))))))

(t/ann ^:no-check transaction-data
       [SpecT t/Any (t/Map t/Keyword t/Any) -> TransactionData])
(defn ^:no-doc transaction-data [txn-fns spec old-si updates & [tmps]]
  "Given a possibly nil, possibly out of date old entity.
   Returns the transaction data to do the desired updates to something of type spec."
  (if-let [eid (when (and (nil? old-si) tmps)
                 (some (fn [[k v]] (and (identical? k updates) v)) @tmps))]
    (with-meta [] {:eid eid})
    (let [eid (or (get-in old-si [:db-ref :eid])
                  (db/tempid :db.part/user))]
      (when-not spec
        (throw (ex-info "spec missing" {:old old-si :updates updates})))
      (let [diff (clojure.set/difference (disj (set (keys updates)) :db-ref)
                                         (set (map :name (:items spec))))]
        (when-not (empty? diff)
          (throw (ex-info "Cannot add keys not in the spec." {:keys diff}))))
      (->> (for [{iname :name :as item} (:items spec)
                 :when (contains? updates iname)]
             (transaction-data-item spec eid txn-fns item (iname old-si) (iname updates) tmps))
           (apply concat)
           (#(if (get-in old-si [:db-ref :eid]) %
                 (cons [:db/add eid :spec-tacular/spec (:name spec)] %)))
           (#(do (when (and tmps (get-spec updates))
                   (swap! tmps conj [updates eid])) %))
           (#(with-meta % (assoc (meta %) :eid eid)))))))

(t/ann ^:no-check graph-transaction-data [ConnCtx t/Coll -> TransactionData])
(defn graph-transaction-data
  "Returns Datomic transaction data that would create the given graph
  on the database given in `conn-ctx`.  Also contains meta-data
  `:tmpids` and `:specs` for the expected temp-ids and specs, respectively."
  [conn-ctx new-si-coll]
  (let [tmps  (atom [])
        specs (map get-spec new-si-coll)
        data  (let [db (db/db (:conn conn-ctx))]
                (map (fn [si spec]
                       (when-not spec
                         (throw (ex-info "could not find spec" {:entity si})))
                       (when (or (get si :db-ref) (get-eid db si))
                         (throw (ex-info "entity already in database" {:entity si})))
                       (transaction-data (:txn-fns conn-ctx) spec nil si tmps))
                     new-si-coll specs))
        tmpids (map (comp :eid meta) data)
        data   (apply concat data)
        txn-id (db/tempid :db.part/tx)
        data (if-let [tl (:transaction-log conn-ctx)]
               (->> (sp->transactions (db/db (:conn conn-ctx)) tl) ; hijack db/id to point to txn.
                    (map #(assoc % :db/id txn-id))
                    (concat data))
               data)]
    (with-meta data {:tmpids tmpids :specs specs})))

(t/ann ^:no-check instance-transaction-data [ConnCtx t/Any -> TransactionData])
(defn instance-transaction-data
  "Returns the Datomic transaction data that would create the given
  spec instance on the database given in `conn-ctx`."
  [conn-ctx new-si]
  (let [data (graph-transaction-data conn-ctx [new-si])
        {:keys [tmpids specs]} (meta data)]
    (with-meta data {:tmpid (first tmpids) :spec (first specs)})))

(t/ann ^:no-check create-graph! (t/All [a] [ConnCtx a -> a]))
(defn create-graph!
  "Creates every new instance contained in the given collection, and
  returns the new instances in a collection of the same type, and in
  the same order where applicable.  For every object that is
  `identical?` in the given graph, only one instance is created on the
  database.

  create-graph! does *not* support arbitrarly nested collections.

  Currently supports sets, lists, vector, and sequences.

  Aborts if any entities already exist on the database."
  [conn-ctx new-si-coll]
  (let [data (graph-transaction-data conn-ctx new-si-coll)        
        {:keys [tmpids specs]} (meta data)
        txn-result (try @(db/transact (:conn conn-ctx) data)
                        (catch java.util.concurrent.ExecutionException e
                          (throw (ex-info "Datomic encountered an error"
                                          {:message (.getMessage e) :data data}))))]
    ;; db side effect has occurred
    (let [db (db/db (:conn conn-ctx))
          db-si-coll (map #(some->> (db/resolve-tempid db (:tempids txn-result) %1)
                                    (db/entity db)
                                    (recursive-ctor (:name %2)))
                          tmpids specs)
          constructor (condp #(%1 %2) new-si-coll
                        set? set, list? list, vector? vec, seq? seq
                        (throw (ex-info "Cannot recreate" {:type (type new-si-coll)})))]
      (constructor db-si-coll))))

(t/ann ^:no-check create! (t/All [a] [ConnCtx a -> a]))
(defn create!
  "Creates a new instance of the given entity on the database in the
  given connection context.  Returns a representation of the newly
  created object.

  Get the Datomic `:db/id` from the object using the `:db-ref` field.
  
  Aborts if the entity already exists in the database (use [[assoc!]] instead)."
  [conn-ctx new-si]
  (first (create-graph! conn-ctx [new-si])))

(t/ann ^:no-check assoc! (t/All [a] [ConnCtx a t/Keyword t/Any -> a]))
(defn assoc!
  "Updates the given entity in database in the given connection. 
  Returns the new entity.

  The entity must be an object representation of an entity on the
  database (for example, returned from a query, [[create!]], or an
  earlier [[assoc!]]).

  *Attempting to retract a `:component` field (by setting that field
  to `nil`) retracts the entire component instance.*
   
  Get the Datomic `:db/id` from the object using the `:db-ref` field.

  Aborts if the entity does not exist in the database (use [[create!]] instead)."
  [conn-ctx si & {:as updates}]
  (when-not (get si :db-ref)
    (throw (ex-info "entity must be on database already" {:entity si})))
  (let [spec (get-spec si)
        eid  (->> (transaction-data (:txn-fns conn-ctx)
                                    spec
                                    si
                                    updates)
                  (commit-sp-transactions! conn-ctx))]
    (->> (db/entity (db/db (:conn conn-ctx)) eid)
         (recursive-ctor (:name spec)))))

(t/ann ^:no-check update! (t/All [a] [ConnCtx a a -> a]))
(defn update! 
  "Calculate a shallow difference between the two spec instances and
  uses [[assoc!]] to change the entity on the database."
  [conn-ctx si-old si-new]
  (let [updates (mapcat (fn [[k v]] [k v]) si-new)]
    (if (empty? updates) si-old
        (if (not (even? (count updates)))
          (throw (ex-info "malformed updates -- expecting keyword-value pairs"
                          {:updates updates}))
          (apply assoc! conn-ctx si-old updates)))))

(t/ann ^:no-check refresh (t/All [a] [ConnCtx a -> a]))
(defn refresh 
  "Returns an updated representation of the Datomic entity at the
  given instance's `:db-ref`.

  The entity must be an object representation of an entity on the
  database (see [[assoc!]] for an explanation)."
  [conn-ctx si]
  (let [eid  (get-in si [:db-ref :eid])
        spec (get-spec si)]
    (when-not eid (throw (ex-info "entity without identity" {:entity si})))
    (when-not spec (throw (ex-info "entity without spec" {:entity si})))
    (let [em (db/entity (db/db (:conn conn-ctx)) eid)]
      (recursive-ctor (:name spec) em))))

(t/ann ^:no-check retract! (t/IFn [ConnCtx t/Any -> nil]
                                  [ConnCtx t/Any t/Keyword -> nil]))
(defn retract!
  "Removes the given instance from the database using
  `:db.fn/retractEntity`.  Returns `nil`."
  {:added "0.5.1"}
  [conn-ctx si & [field-name]]
  (if field-name
    (assoc! conn-ctx si field-name nil)
    (let [eid (get-in si [:db-ref :eid])]
      (when-not eid (throw (ex-info "entity not on database" {:entity si})))
      (let [data [[:db.fn/retractEntity eid]]
            data (if-let [tl (:transaction-log conn-ctx)]
                   (let [txn-id (db/tempid :db.part/tx)]
                     (->> (sp->transactions (db/db (:conn conn-ctx)) tl) ; hijack db/id to point to txn.
                          (map #(assoc % :db/id txn-id))
                          (concat data)))
                   data)]
        (do (commit-sp-transactions! conn-ctx data) nil)))))

(defn backwards [spec-name kw]
  (let [spec (get-spec spec-name)
        item (get-item spec kw)]
    (with-meta item {:spec-tacular/spec spec})))

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
                 (instance? spark.spec_tacular.spec.EnumSpec sub-spec))
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
                                                   (db/entity db (:db/id %))))]
                          [pattern
                           (if (primitive? sub-spec-name)
                             v
                             (if (= :many arity)
                               (mapv f v)
                               (f v)))])))})))
    (instance? spark.spec_tacular.spec.Item pattern)
    (let [spec (:spec-tacular/spec (meta pattern))
          kw (db-keyword spec (keyword (str "_" (name (:name pattern)))))]
      {:datomic-pattern [kw]
       :rebuild (fn [db m]
                  (when-let [v (get m kw)]
                    (let [f #(recursive-ctor spec (db/entity db (:db/id %)))]
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
                            (:spec-tacular/spec (meta kw-or-item)))
                          {[db-kw] :datomic-pattern}
                          (datomify-spec-pattern spec kw-or-item)
                          {:keys [datomic-pattern rebuild]}
                          (datomify-spec-pattern (get-spec sub-spec-name) sub-pattern)]]
                {:datomic-pattern [db-kw datomic-pattern]
                 :rebuild (fn [db m]
                            (when-let [v (get m db-kw)]
                              [kw-or-item (if (or (= arity :many)
                                                  (and (instance? spark.spec_tacular.spec.Item kw-or-item)
                                                       (not component?)))
                                            (map #(rebuild db %) v)
                                            (rebuild db v))]))})]
      {:datomic-pattern [(into {} (map :datomic-pattern rec))]
       :rebuild (fn [db m] (into {} (map #(% db m) (map :rebuild rec))))})))

(defn pull
  "Executes a Datomic pull after datomifying the given pattern with
  respect to the given instance."
  [db pattern instance]
  (let [eid (get-eid db instance)
        spec (get-spec instance)]
    (assert (vector? pattern))
    (let [{:keys [datomic-pattern rebuild]} (datomify-spec-pattern spec pattern)]
      (->> (db/pull db datomic-pattern eid)
           (rebuild db)))))

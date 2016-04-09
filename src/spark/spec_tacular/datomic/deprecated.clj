(ns spark.spec-tacular.datomic.deprecated
  "old functionality of spark.spec-tacular.datomic, retained only
  until we can port deprecated tests over to newer functionality"
  (:refer-clojure :exclude [assoc!])
  (:require [clojure.core.typed :as t]
            [clojure.data :as data]
            [clojure.set :refer [rename-keys]]
            [clojure.walk :as walk]
            [datomic.api :as db]
            [spark.spec-tacular :refer :all]
            [spark.spec-tacular.datomic :refer :all]
            [spark.spec-tacular.datomic.util :refer :all])
  (:import clojure.lang.MapEntry))

(t/defalias Mask (t/Rec [mask] (t/Map t/Keyword (t/U mask t/Bool))))

(defn db->sp ;; TODO -- this function is pointless now
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

(defn get-by-eid ;; TODO -- clean up uses of this function in user code
  "fetches the entire SpecInstance from the db for the given eid
   throws IllegalArgumentException when eid isn't found."
  [db eid & [sp-type]]
  (do (assert (instance? java.lang.Long eid) 
              (str eid " is not an eid"))
      (db->sp db (db/entity db eid) sp-type)))

(t/ann ^:no-check build-transactions
       (t/IFn [datomic.db.Db SpecInstance Mask (t/Atom1 (t/ASeq (t/Vec t/Any))) -> (t/Map t/Keyword t/Any)]
              [datomic.db.Db SpecInstance Mask (t/Atom1 (t/ASeq (t/Vec t/Any))) SpecT -> (t/Map t/Keyword t/Any)]))
(defn build-transactions
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
(defn union-masks
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

(t/ann ^:no-check shallow-plus-unions-mask [SpecT -> Mask])
(defn shallow-plus-unions-mask
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
(defn new-components-mask
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
  (valAt [t k default] (if-let [e (.entryAt t k)] (.val e) default))))

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
(defn create-sp!
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
(defn masked-create-sp!
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
          (filter identity (map filter-one sp)) ;; don't let nils thru
          (filter-one sp)))
      (if (= true mask)
        sp
        nil))))

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
    (commit-sp-transactions! conn-ctx txns)))

(t/ann ^:no-check remove-eids [SpecInstance -> SpecInstance])
(defn remove-eids
  "recursively strip all entries of :db-ref {:eid ...} from sp.
   can be used for checking equality with a non-db value."
  [sp]
  (walk/postwalk (fn [m] (if (get m :db-ref) (dissoc m :db-ref) m)) sp))

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
(defn remove-items-with-required
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

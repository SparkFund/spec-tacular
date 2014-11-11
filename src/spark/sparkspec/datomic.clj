(ns spark.sparkspec.datomic
  (:use spark.sparkspec.spec
        spark.sparkspec
        clojure.data
        [clojure.string :only [lower-case]]
        [clojure.set :only [rename-keys difference]])
  (:import clojure.lang.MapEntry)
  (:require [clojure.data :only diff]
            [clojure.walk :as walk]
            [datomic.api :as db]
            [schema.core :as s]
            [schema.macros :as m]))

(def spark-type-attr 
  "The datomic attribute holding onto a keyword-valued spec type."
  :spec-tacular/spec)

;; TODO: map data definition

;; TODO: integrate db-type->spec-type, recursiveness, and core-types
;; into sparkspec.clj somehow
(def db-type->spec-type
  ^:private
  (reduce #(assoc %1 %2 (keyword (name %2))) {}
          [:db.type/keyword :db.type/string :db.type/boolean :db.type/long
           :db.type/bigint :db.type/float :db.type/double :db.type/bigdec
           :db.type/instant :db.type/uuid :db.type/uri :db.type/bytes]))

(defn- datomic-ns
  "Returns a string representation of the db-normalized namespace for
  the given spec."
  [spec]
  (some-> spec :name name lower-case))

(defn datomic-schema 
  "generate a list of entries for a datomic schema to represent this spec."
  [spec]
  (for [{iname :name [cardinality type] :type :as item} (:items spec)]
    (merge
     {:db/id #db/id [:db.part/db]
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

(defn check-schema
  "Returns a list of errors representing discrepencies between the
  given spec and schema."
  [schema spec]
  (letfn [(reduce-component [m v] (assoc m (-> v :db/ident name keyword) v))
          (reduce-items [m v] (assoc m (:name v) v))
          (check [v m] (if v nil m))
          (all-errors [& rest] (filter some? (flatten rest)))
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

(defn get-all-eids
  "Retrives all of the eids described by the given spec from the
  database."
  [db spec]
  (let [names (map #(keyword (datomic-ns spec) (-> % :name name)) (:items spec))
        query '[:find ?eid :in $ [?attr ...] :where [?eid ?attr ?val]]]
    (apply concat (db/q query db names))))

(defn get-eid
  "Returns an EID associated with the data in the given spark type if
  it exists in the database. Looks up according to identity
  items. Returns nil if not found."
  ([db sp] (get-eid db sp (get-spec sp)))
  ([db sp spec]
     (let [eid (or (:eid (meta sp)) (get-in sp [:db-ref :eid]))]
       (if eid
         eid
         (let [sname (datomic-ns spec)
               idents (filter #(or (:identity? %) (:unique? %)) (:items spec))
               query '[:find ?eid :in $ ?attr ?val :where [?eid ?attr ?val]]
               eids
               , (for [{iname :name [_ type] :type} idents]
                   (let [sub-val (if (primitive? type)
                                   (get sp iname)
                                   (get-eid db (get sp iname)))
                         eid (when (some? sub-val)
                                (db/q query db
                                      (keyword (name sname) (name iname))
                                      sub-val))]
                     eid))]
           (first (map ffirst (filter not-empty eids))))))))

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

(defn get-by-eid [db eid & [sp-type]]
  (db->sp db (db/entity db eid) sp-type))

(defn get-all-of-type
  "Helper function that returns all items of a single spec"
  [db spec]
  (let [eids (get-all-eids db spec)]
    (map (fn [eid] (db->sp db (db/entity db eid) (:name spec))) eids)))

(defn- not-all-empty? [m] (not-any? (fn [[k v]] (some? v)) m))

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

(defn item-mask
  "Builds a mask-map representing a specific sp value,
   where missing keys represent masked out fields and sp objects
   consisting only of a db-ref are considered 'true' valued leaves
   for identity updates only."
  [spec-name sp]
  (let [spec (get-spec spec-name)]
    (if (and (some? spec) (and (some? sp)
                               (if (or (list? sp) (vector? sp) (set? sp)) ; union-masks over empty lists would result in the nil mask, but we want an empty list to mean 'true' -- i.e. explicitly empty.
                                 (not-empty sp)
                                 true)))
      (if (:elements spec)
        (let [enum-mask (fn [sp-item]
                          (let [sub-spec-name (:name (get-spec sp-item))]
                            {sub-spec-name (item-mask sub-spec-name sp-item)}))]
          (if (or (list? sp) (vector? sp) (set? sp))
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
          (if (or (list? sp) (vector? sp) (set? sp))
            (reduce union-masks (map field-mask sp)) ; only recover mask from the particular enum types used by the instance
            (field-mask sp))))
      true)))

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

(defn complete-mask [spec]
  "Builds a mask-map of the given spec for consumption by
  build-transactions. Recurs down specs to add everything in
  time."
  (CompleteMask. spec))

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
        (if (or (list? sp) (vector? sp) (set? sp))
          (map filter-one sp)
          (filter-one sp)))
      (if (= true mask)
        sp
        nil))))

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

(defn remove-eids
  "recursively strip all entries of :db-ref {:eid ...} from sp.
   can be used for checking equality with a non-db value."
  [sp]
  (walk/postwalk (fn [m] (if (get-in m [:db-ref :eid])
                           (dissoc m :db-ref)
                           m))
                 sp))

(defn remove-identity-items
  "recursively walks a sp and removes any :unique? or :identity?
  items (as those are harder to test.) We do want to come up with some
  sensible :identity? tests at some point though."
  [spec-name sp]
  (let [spec (get-spec spec-name)]
    (if (and (some? spec) (and (some? sp)
                               (if (or (list? sp) (vector? sp) (set? sp))
                                 (not-empty sp)
                                 true)))
      (if (:elements spec)
        (let [enum-remove (fn [sp-item]
                            (remove-identity-items (:name (get-spec sp-item))
                                                   sp-item))]
          (if (or (list? sp) (vector? sp) (set? sp))
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
          (if (or (list? sp) (vector? sp) (set? sp))
            (into (empty sp) (map item-remove sp))
            (item-remove sp))))
      sp)))

; TODO we could make it actually remove the keys instead of nil them, need a helper with some sentinal probably easiest?
; also we probably want to only strip these on nested things, not the toplevel thing. (i.e. THIS is the helper already.)
(defn remove-items-with-required
  "recursively walks a sp and removes any sub things that have
  required fields in the spec. Another tricky-to-test updates helper. (including top-level)"
  [sp]
  (if (or (list? sp) (vector? sp) (set? sp))
    (into (empty sp) (remove #(= % ::remove-me) (map remove-items-with-required sp)))
    (let [spec (get-spec sp)]
      (if (and (some? spec) (and (some? sp)
                                 (if (or (list? sp) (vector? sp) (set? sp))
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

(defn remove-sub-items-with-required
  "recursively walks a single (map) sp and removes any sub-sps that
  have required fields in the spec (but not the toplevel one, only
  sub-items). Another tricky-to-test updates helper.  Also, remove any
  top-level attributes that are required but have nil values."
  [sp]
  (let [spec (get-spec sp)]
    (assert (nil? (:elements sp)))
    (if (and (some? spec) (and (some? sp)
                               (if (or (list? sp) (vector? sp) (set? sp))
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

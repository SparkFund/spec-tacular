(ns spark.spec-tacular.datomic
  {:doc "Datomic interface for the spec-tacular DSL"
   :core.typed {:collect-only true}}
  (:refer-clojure :exclude [for remove assoc!])
  (:use spark.spec-tacular.spec
        spark.spec-tacular)
  (:require [clj-time.coerce :as timec]
            [clojure.core.typed :as t :refer [for]]
            [clojure.string :refer [lower-case]]
            [clojure.set :refer [rename-keys difference union]]
            [clojure.core.typed.unsafe :refer [ignore-with-unchecked-cast]]
            [clojure.data :as data]
            [clojure.walk :as walk]
            [clojure.core.match :refer [match]]
            [datomic.api :as db]
            [spark.spec-tacular.datomic.util :refer :all]
            [spark.spec-tacular.datomic.coerce :refer :all]
            [spark.spec-tacular.datomic.pull-helpers :refer :all]
            [spark.spec-tacular.datomic.query-helpers :refer :all])
  (:import clojure.lang.MapEntry
           spark.spec_tacular.spec.Spec
           spark.spec_tacular.spec.EnumSpec
           (spark.spec_tacular.spec UnionSpec)))

;; -----------------------------------------------------------------------------

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

(t/ann ^:no-check db [ConnCtx -> Database])
(defn db
  "Returns the database in a given connection context."
  [conn-ctx]
  (db/db (:conn conn-ctx)))

(defmethod database-coercion datomic.query.EntityMap [em]
  (coerce-datomic-entity em))

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

(declare transaction-data)

(defn transaction-log-data [conn-ctx]
  (when-let [tl (:transaction-log conn-ctx)]
    (let [eid (get-in tl [:db-ref :eid] (db/tempid :db.part/tx))
          spec (get-spec tl)]
      (cons [:db/add eid :spec-tacular/spec (:name spec)]
            (transaction-data {} spec {:db-ref {:eid eid}} tl)))))

(t/ann ^:no-check commit-transactions!
       [ConnCtx (t/ASeq t/Any) -> Long])
(defn ^:no-doc commit-transactions!
  "if :transaction-log is specified in conn-ctx (a regular sp object),
   we attach its attributes to the transaction."
  [conn-ctx transaction]
  (let [txn-log (transaction-log-data conn-ctx)
        tx @(db/transact (:conn conn-ctx) (concat transaction txn-log))
        eid (->> transaction meta :eid)
        entid (db/resolve-tempid (db/db (:conn conn-ctx)) (:tempids tx) eid)]
    (or entid eid (:tempids tx))))

;; =============================================================================
;; query

(defmacro q
  "Returns a set of results from the Datomic query.  See [the
  README](https://github.com/SparkFund/spec-tacular/tree/v0.5.0#querying-databases)
  for examples.

  ```
  (q :find FIND-EXPR+ :in expr :where CLAUSE+)

  FIND-EXPR = IDENT 
            | IDENT .
            | (pull IDENT pattern)
            | [IDENT+ ]
            | [IDENT ...]
  IDENT     = SpecName
  VAR       = ?variable
            | IDENT
  CLAUSE    = [IDENT MAP]
            | [(symbol VAR+)]
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
  inserts the first `SpecName` in the `FIND-EXPR` if there is only
  one, or `%1` references the first, and `%2` the second, etc.

  Every `SpecName` must be a keyword at compile-time; to dynamically
  get the spec of an entity use the special keyword
  `:spec-tacular/spec` as an entry in any `MAP`."
  [& stx]
  (let [{:keys [find-expr db-expr in-expr where-expr query-ret-type]} (parse-query stx)
        db (gensym 'db)]
    `(t/let [~db :- Database ~db-expr]
       (clojure.core.typed.unsafe/ignore-with-unchecked-cast
        (query {:find ~find-expr
                :in (cons ~''$ ~in-expr)
                :where ~where-expr}
               ~db)
        ~query-ret-type))))

;; ---------------------------------------------------------------------------------------------------
;; query runtime

(defn query [{find-elems :find clauses :where :as m} & args]
  (let [{:keys [datomic-find rebuild]} (datomify-find-elems find-elems)
        clauses (combine-where-clauses (map datomify-where-clause clauses))
        [db & _] args
        query (assoc m :find datomic-find :where (case (first clauses)
                                                   (and) (rest clauses)
                                                   [clauses]))]
    (try (rebuild db (apply db/q query args))
         (catch Exception e
           (throw (doto (ex-info "Encountered an error running Datomic query"
                                 {:query query :args args})
                    (.initCause e)))))))

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
                  (let [sub-eid  (db/tempid (or (:part parent-eid) :db.part/user))
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
        log-data (transaction-log-data conn-ctx)]
    (with-meta (concat data log-data) {:tmpids tmpids :specs specs})))

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
                          (throw (doto (ex-info "Encountered an error during Datomic transaction"
                                                {:data data})
                                   (.initCause e)))))]
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
                  (commit-transactions! conn-ctx))]
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
            log-data (transaction-log-data conn-ctx)]
        (do (commit-transactions! conn-ctx (concat data log-data)) nil)))))

(defn backwards [spec-name kw]
  (let [spec (get-spec spec-name)
        item (get-item spec kw)]
    (with-meta item {:spec-tacular/spec spec})))

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

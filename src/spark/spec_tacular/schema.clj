(ns spark.spec-tacular.schema
  (:refer-clojure :exclude [read-string read assoc!])
  (:use [spark.spec-tacular :exclude [diff]]
        spark.spec-tacular.spec
        spark.spec-tacular.datomic
        [clojure.string :only [lower-case]])
  (:require [clojure.core.typed :as t]
            [clojure.edn :as edn]
            [clojure.java.io :as io]))

(t/typed-deps spark.spec-tacular
              spark.spec-tacular.datomic)

(t/ann ^:no-check clojure.core/slurp [(t/U t/Str java.io.File) -> t/Str])
(t/ann ^:no-check clojure.edn/read-string 
       [(t/HMap :optional {:readers t/Any}) t/Str -> (t/Seq EntityMap)])
(t/ann ^:no-check clojure.edn/read
       [(t/HMap :optional {:readers t/Any}) java.io.PushbackReader -> (t/Seq EntityMap)])
(t/ann ^:no-check clojure.data/diff 
       (t/All [a b] [(t/Set a) (t/Set b) -> (t/HVec [(t/Set a) (t/Set b) (t/Set (t/I a b))])]))
(t/ann ^:no-check clojure.java.io/writer
       [java.io.File -> java.io.Writer])

(require '[datomic.api :as d])

;; An EntityMap is a (possibly partial) description of a Datomic entity
;; An InstallableEntityMap is a full description of a Datomic entity that can be installed
(t/defalias EntityMap
  (t/HMap :mandatory
          {:db/ident t/Keyword
           :db/valueType t/Any}
          :optional 
          {:db/id datomic.db.DbId
           :db/cardinality (t/U (t/Val :db.cardinality/one) (t/Val :db.cardinality/many))
           :db/fn (t/Map t/Keyword t/Any)
           :db.install/_attribute t/Keyword}))
(t/defalias InstallableEntityMap
  (t/HMap :mandatory
          {:db/id datomic.db.DbId
           :db/ident t/Keyword
           :db/valueType t/Any
           :db/cardinality (t/U (t/Val :db.cardinality/one) (t/Val :db.cardinality/many))
           :db.install/_attribute (t/U (t/Val :db.part/db))}
          :optional 
          {:db/fn (t/Map t/Keyword t/Any)
           :db/doc t/Str}))

;; A Schema is a list of mappings that can be used as a Datomic schema,
;;   see http://docs.datomic.com/schema.html
;; Schemas can be created from the specification in types.clj
;; Schemas can be recreated from files and databases
(t/defalias Schema (t/Seq EntityMap))

;; A Delta is a Schema; but it is not intended to be used as a Datomic schema.
;;   Instead, it represents the change between two Schemas
;; Deltas can be created by computing the difference between two Schemas
;; Deltas can be recreated from files
(t/defalias Delta Schema)

(t/def ^:private datomic-base-attributes :- (t/Coll t/Keyword)
  #{:db.alter/attribute :db.install/partition :db/excise :db/lang
    :db.install/function :db/noHistory :db/txInstant :spec-tacular/spec
    :db.excise/attrs :db/ident :db/cardinality :db/index :db.install/valueType
    :db/fn :db/isComponent :db/code :db/unique :db.excise/beforeT :db.excise/before
    :db/valueType :fressian/tag :db/doc :db.install/attribute :db/fulltext})

(t/def spec-tactular-map :- InstallableEntityMap
  {:db/id (d/tempid :db.part/db),
   :db/ident :spec-tacular/spec,
   :db/valueType :db.type/keyword,
   :db/cardinality :db.cardinality/one,
   :db/doc "spec-tacular/spec type tag",
   :db.install/_attribute :db.part/db})

(t/defn ^:private item->schema-map 
  [spec :- SpecT, {iname :name [cardinality type] :type :as item} :- Item] :- EntityMap
  (merge
   {:db/id          (d/tempid :db.part/db)
    :db/ident       (keyword (datomic-ns spec) (name iname))
    :db/valueType   (if (primitive? type)
                      (if (= type :calendarday)
                        (keyword "db.type" "instant")
                        (keyword "db.type" (name type)))
                      :db.type/ref)
    :db/cardinality (case cardinality
                      :one :db.cardinality/one
                      :many :db.cardinality/many)
    :db/doc         ""
    :db.install/_attribute :db.part/db}
   (if (:unique? item)
     {:db/unique (if (:identity? item)
                   :db.unique/identity
                   :db.unique/value)}
     {})))

(t/defn from-spec
  "Generates a Schema to represent the given spec"
  [spec :- (t/U SpecT t/Keyword)] :- Schema
  (let [si (if (keyword? spec) (get-spec spec) spec)]
    (assert si (str "cannot find spec for " spec))
    (t/for [item :- Item (:items si)] :- EntityMap
           (item->schema-map si item))))

(t/defn normalize
  "normalizes a Schema for comparison
   -- removes any mappings for attributes that datomic adds automatically
   -- removes :db/id and :db.install/_attribute attributes from each mapping
   -- makes sure each entry has a :db/unique attry, even if nil
   -- simplifies :db/fn field so that it is comparable with ="
  [schema :- Schema] :- Schema
  (->> schema
       (filter #(not (contains? datomic-base-attributes (:db/ident %))))
       (map (t/fn [m :- EntityMap] :- EntityMap
              (dissoc m :db/id :db.install/_attribute)))
       (map (t/fn [m :- EntityMap] :- EntityMap
              (assoc m :db/unique (:db/unique m))))
       (map (t/fn [m :- EntityMap] :- EntityMap
              (if-let [txn-fn (:db/fn m)]
                (assoc m :db/fn (dissoc (into {} txn-fn) :fnref :pending)) m)))))

(t/defn from-file
  "returns the Schema inside the given file"
  [schema-file :- java.io.File] :- Schema
  (edn/read-string {:readers *data-readers*} (slurp schema-file)))

(t/defn read-string
  "returns the Schema inside the given stream"
  [s :- t/Str] :- Schema
  (edn/read-string {:readers *data-readers*} s))

(t/defn read
  "returns the Schema inside the given stream"
  [stream :- java.io.PushbackReader] :- Schema
  (edn/read {:readers *data-readers*} stream))

(t/ann ^:no-check write [Schema java.io.Writer -> nil])
(t/defn write
  "writes a Schema to w"
  [schema w]
  (t/let [write :- [java.lang.String -> nil]
          ,#(.write ^java.io.Writer w ^java.lang.String %)
          sorted-cols :- Schema
          (map #(into (sorted-map) %) schema)
          sorted-rows :- Schema
          (sort-by :db/ident (map #(into (sorted-map) %) sorted-cols))]
    (write "[")
    (t/doseq [m :- EntityMap (cons spec-tactular-map sorted-rows)]
      (write "\n")
      ;; regexp: #db/id[:db.part/db -1003792] ==> #db/id[:db.part/db]
      ;; TODO: is that regexp qualified enough?
      (write (clojure.string/replace (str m) #"(db|user) -(\d+)" "$1")))
    (write "\n]\n")))

(t/defn to-file
  "writes a Schema to then given file"
  [schema :- Schema, file :- java.io.File] :- nil
  (with-open [f (io/writer file)] (write schema f)))

(t/defn diff
  "returns the difference between two schemas as three sets:
   -- the entries only in Schema1,
   -- the entries only in Schema2,
   -- the entries in both"
  [schema1 :- Schema, schema2 :- Schema] 
  :- (t/HVec [(t/Set EntityMap) (t/Set EntityMap) (t/Set EntityMap)])
  (clojure.data/diff (set (normalize schema1))
                     (set (normalize schema2))))

;; TODO: this is a diff not a check, checks throw errors
(t/ann ^:no-check check [Schema SpecT -> (t/ASeq t/Str)])
(defn check
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
                      (clojure.data/diff schema-keys name-keys)))
       (map diff-uniques component->item)
       ;; TODO: Add more checks! Be strict!
       ))))

(t/defn from-specs
  "Converts specs into a Schema"
  [specs :- (t/Seq (t/U SpecT t/Keyword))] :- Schema
  (->> specs (mapcat from-spec)))

(t/defn from-namespace
  "Converts all specs in a namespace into a Schema"
  [namespace :- clojure.lang.Namespace] :- Schema
  (->> namespace namespace->specs from-specs))

(t/defalias URI t/Str)
(t/ann ^:no-check datomic.api/squuid [-> java.util.UUID])
(t/ann ^:no-check datomic.api/create-database [URI -> nil])
(t/ann ^:no-check datomic.api/delete-database [URI -> nil])
(t/ann ^:no-check datomic.api/connect [URI -> Connection])
(t/ann ^:no-check datomic.api/db [Connection -> Database])

(t/ann fresh-db! (t/IFn [-> Connection]
                        [URI -> Connection]))
(defn- fresh-db!
  "Creates a fresh database.  Returns a connection to that database.
  If it already exists, requires user interaction on stdin to proceed as this could erase the db."
  ([] (fresh-db! (str "datomic:mem://" (d/squuid))))
  ([uri]
   (let [created? (d/create-database uri)]
     (when-not created?
       (let [confirm-num (+ 10 (rand-int 90))]
         (if (not= "bypass" (System/getenv "SP_DANGER_DANGER__BYPASS_DB_RESET_PROMPT"))
           (do
             (println "CAUTION: You are about to erase the db: " uri
                      "\nPlease enter the number" confirm-num
                      "to confirm you want to proceed.")
             (let [inp (read-line)]
               (assert (= inp (str confirm-num))
                       (str "stdin "inp" didn't match "confirm-num)))))
         (d/delete-database uri)
         (d/create-database uri)
         (println "Created fresh db.")))
     (d/connect uri))))

(t/defn to-database!
  "Creates a fresh db with schema installed; returns db"
  ([schema :- Schema] :- Connection
   (let [connection (fresh-db!)]
     (do @(d/transact connection (cons spec-tactular-map schema)) connection)))
  ([schema :- Schema, uri :- URI] :- Connection
   (let [connection (fresh-db! uri)]
     (do @(d/transact connection (cons spec-tactular-map schema)) connection))))

(t/ann ^:no-check from-database [(t/U Database Connection) -> Schema])
(defn from-database
  "Returns the Schema in the given database, 
   or if given a connection, the schema in the newest database in that connection"
  [db]
  (let [db (if (instance? datomic.peer.LocalConnection db) (d/db db) db)]
    (->> (t/ann-form (d/q '[:find ?attr :where [_ :db.install/attribute ?attr]] db)
                     (t/Seq (t/HVec [t/Num]))) 
         ;; -- collects all entities that have been installed
         (map (t/ann-form #(->> % first (d/entity db) (into {})) ;; doesn't typecheck
                          [(t/HVec [t/Num]) -> EntityMap]))
         ;; -- filters out those installed by datomic
         (filter (t/ann-form #(not (contains? datomic-base-attributes (:db/ident %)))
                             [EntityMap -> t/Bool])))))

(t/defn delta
  "computes Delta between two Schemas"
  [old :- Schema, new :- Schema] :- Schema
  (let [[removed-entries new-entries both] (diff old new)]
    (when removed-entries
      (throw (ex-info "Deletion and renaming not supported"
                      {:removed-entries removed-entries
                       :new-entries new-entries})))
    (let [old-idents (into #{} (map :db/ident removed-entries))
          new-idents (into #{} (map :db/ident new-entries))]
      (filter #(contains? new-idents (:db/ident %)) new))))

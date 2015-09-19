(ns spark.spec-tacular.schema
  "Creating Datomic schemas from specifications"
  (:refer-clojure :exclude [read-string read assoc!])
  (:use [spark.spec-tacular :exclude [diff]]
        spark.spec-tacular.spec
        spark.spec-tacular.datomic
        [clojure.string :only [lower-case]])
  (:require [clojure.core.typed :as t]
            [clojure.edn :as edn]
            [spark.spec-tacular.datomic :as sd]
            [clojure.java.io :as io]))

(t/typed-deps spark.spec-tacular
              spark.spec-tacular.datomic)

(require '[datomic.api :as d])

(t/defalias EntityMap
  "An EntityMap is a (possibly partial) description of a Datomic entity."
  (t/HMap :mandatory
          {:db/ident t/Keyword
           :db/valueType t/Any}
          :optional 
          {:db/id datomic.db.DbId
           :db/cardinality (t/U ':db.cardinality/one ':db.cardinality/many)
           :db/fn (t/Map t/Keyword t/Any)
           :db.install/_attribute t/Keyword}))
(t/defalias InstallableEntityMap
  "A a full description of a Datomic entity that can be installed as
  part of a Datomic schema."
  (t/HMap :mandatory
          {:db/id datomic.db.DbId
           :db/ident t/Keyword
           :db/valueType t/Any
           :db/cardinality (t/U ':db.cardinality/one ':db.cardinality/many)
           :db.install/_attribute (t/U (t/Val :db.part/db))}
          :optional 
          {:db/fn (t/Map t/Keyword t/Any)
           :db/doc t/Str}))

(t/defalias Schema
  "A list of mappings that can be used as a
  [Datomic schema](http://docs.datomic.com/schema.html).  Schemas can
  be created from sequences or namespaces containing [[defspec]]s, or
  can be recreated from files and databases."
  (t/Seq EntityMap))

;; A Delta is a Schema; but it is not intended to be used as a Datomic schema.
;;   Instead, it represents the change between two Schemas
;; Deltas can be created by computing the difference between two Schemas
;; Deltas can be recreated from files
(t/defalias ^:no-doc Delta Schema)

(t/def ^:private datomic-base-attributes :- (t/Coll t/Keyword)
  #{:db.alter/attribute :db.install/partition :db/excise :db/lang
    :db.install/function :db/noHistory :db/txInstant :spec-tacular/spec
    :db.excise/attrs :db/ident :db/cardinality :db/index :db.install/valueType
    :db/fn :db/isComponent :db/code :db/unique :db.excise/beforeT :db.excise/before
    :db/valueType :fressian/tag :db/doc :db.install/attribute :db/fulltext})

(t/ann ^:no-check spec-tacular-map InstallableEntityMap)
(def
  ^{:doc "The map for the special `:spec-tacular/spec` ident which
  must be installed on any database hoping to use spec-tacular.
  Automatically installed when using [[to-database!]]."}
  spec-tacular-map
  {:db/id (d/tempid :db.part/db),
   :db/ident :spec-tacular/spec,
   :db/valueType :db.type/keyword,
   :db/cardinality :db.cardinality/one,
   :db/doc "spec-tacular/spec type tag",
   :db.install/_attribute :db.part/db})

(t/ann ^:no-check item->schema-map [SpecT Item -> EntityMap])
(defn- item->schema-map  
  [spec {iname :name [cardinality type] :type :as item}]
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

(t/ann from-spec [(t/U SpecT t/Keyword) -> Schema])
(defn from-spec
  "Generates a [[Schema]] that represents `spec`."
  [spec]
  (let [si (if (keyword? spec) (get-spec spec) spec)]
    (assert si (str "cannot find spec for " spec))
    (t/for [item :- Item (:items si)] :- EntityMap
           (item->schema-map si item))))

(t/ann normalize [Schema -> Schema])
(defn normalize
  "Normalizes `schema` for comparison:

   * removes any mappings for attributes that Datomic adds automatically
   * removes `:db/id` and `:db.install/_attribute` attributes from each mapping
   * ensures each entry has a `:db/unique` attribute, even if it's `nil`
   * simplifies `:db/fn` fields so that they are comparable with `=`"
  [schema]
  (->> schema
       (filter #(not (contains? datomic-base-attributes (:db/ident %))))
       (map (t/fn [m :- EntityMap] :- EntityMap
              (dissoc m :db/id :db.install/_attribute)))
       (map (t/fn [m :- EntityMap] :- EntityMap
              (assoc m :db/unique (:db/unique m))))
       (map (t/fn [m :- EntityMap] :- EntityMap
              (if-let [txn-fn (:db/fn m)]
                (assoc m :db/fn (dissoc (into {} txn-fn) :fnref :pending)) m)))))

(t/ann ^:no-check from-file [java.io.File -> Schema])
(defn from-file
  "Returns the [[Schema]] inside the given file"
  [schema-file]
  (edn/read-string {:readers *data-readers*} (slurp schema-file)))

(t/ann ^:no-check read-string [t/Str -> Schema])
(defn read-string
  "Returns the [[Schema]] inside the given string"
  [s]
  (edn/read-string {:readers *data-readers*} s))

(t/ann ^:no-check read [java.io.PushbackReader -> Schema])
(defn read
  "Returns the [[Schema]] inside the given stream"
  [stream]
  (edn/read {:readers *data-readers*} stream))

(t/ann ^:no-check write [Schema java.io.Writer -> nil])
(t/defn write
  "Writes the schema to w, adding in [[spec-tacular-map]]."
  [schema w]
  (t/let [write :- [java.lang.String -> nil]
          ,#(.write ^java.io.Writer w ^java.lang.String %)
          sorted-cols :- Schema
          (map #(into (sorted-map) %) schema)
          sorted-rows :- Schema
          (sort-by :db/ident (map #(into (sorted-map) %) sorted-cols))]
    (write "[")
    (t/doseq [m :- EntityMap (cons spec-tacular-map sorted-rows)]
      (write "\n")
      ;; regexp: #db/id[:db.part/db -1003792] ==> #db/id[:db.part/db]
      ;; TODO: is that regexp qualified enough?
      (write (clojure.string/replace (str m) #"(db|user) -(\d+)" "$1")))
    (write "\n]\n")))

(t/ann ^:no-check to-file [Schema java.io.File -> nil])
(defn to-file
  "Writes `schema` to `file`, returns `nil`."
  [schema file]
  (with-open [f (io/writer file)] (write schema f)))

(t/ann ^:no-check diff [Schema Schema -> '[(t/Set EntityMap) (t/Set EntityMap) (t/Set EntityMap)]])
(defn diff
  "Returns the difference between two schemas as three sets:
   * the entries only in `schema1`,
   * the entries only in `schema2`,
   * the entries in both `schema1` and `schema2`"
  [schema1 schema2] 
  (clojure.data/diff (set (normalize schema1))
                     (set (normalize schema2))))

;; TODO: this is a diff not a check, checks throw errors
(t/ann ^:no-check check [Schema SpecT -> (t/ASeq t/Str)])
(defn ^:no-doc check
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

(t/ann from-specs [(t/Seqable (t/U SpecT t/Keyword)) -> Schema])
(defn from-specs
  "Converts a sequence of specs (which may be actual spec objects or
  keywords) into a [[Schema]]"
  [specs]
  (->> specs (mapcat from-spec)))

(t/ann from-namespace [clojure.lang.Namespace -> Schema])
(defn from-namespace
  "Converts all specs in `namespace` into a [[Schema]]"
  [namespace]
  (->> namespace namespace->specs from-specs))

(t/defalias URI "A URI is just a string, but it should probably look like `\"datomic:mem://<squiid>\"`" t/Str)
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

(t/ann ^:no-check to-database! (t/IFn [Schema -> Connection]
                                      [Schema URI -> Connection]))
(defn to-database!
  "Creates a fresh database with `schema` installed and returns a
  connection to that database.  If the schema does not already
  contain [[spec-tacular-map]], it is added.

  Installs at `uri` if supplied."
  ([schema]
   (let [connection (fresh-db!)
         schema (if (some #(= (:db/ident %) :spec-tacular/spec) schema) schema
                    (cons spec-tacular-map schema))]
     (do @(d/transact connection schema) connection)))
  ([schema uri]
   (let [connection (fresh-db! uri)
         schema (if (some #(= (:db/ident %) :spec-tacular/spec) schema) schema
                    (cons spec-tacular-map schema))]
     (do @(d/transact connection schema) connection))))

(t/ann ^:no-check from-database [(t/U Database Connection ConnCtx) -> Schema])
(defn from-database
  "Returns the Schema in the given [[Database]], [[Connection]], or
  [[spark.spec-tacular.datomic/ConnCtx]]."
  [db]
  (let [db (cond
             (instance? datomic.peer.LocalConnection db) (d/db db)
             (map? db) (sd/db db)
             :else db)]
    (->> (d/q '[:find ?attr :where [_ :db.install/attribute ?attr]] db)
         ;; -- collects all entities that have been installed
         (map #(->> % first (d/entity db) (into {})))
         ;; -- filters out those installed by datomic
         (filter #(not (contains? datomic-base-attributes (:db/ident %)))))))

(t/defn ^:no-doc delta
  "Computes Delta between two Schemas"
  [old :- Schema, new :- Schema] :- Schema
  (let [[removed-entries new-entries both] (diff old new)]
    (when removed-entries
      (throw (ex-info "Deletion and renaming not supported"
                      {:removed-entries removed-entries
                       :new-entries new-entries})))
    (let [old-idents (into #{} (map :db/ident removed-entries))
          new-idents (into #{} (map :db/ident new-entries))]
      (filter #(contains? new-idents (:db/ident %)) new))))

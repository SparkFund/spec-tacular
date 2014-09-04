(ns spark.test-utils
  (:require [datomic.api :as db]))

;;;; Macro for using fresh datomic instances for every test.

(def ^:dynamic *conn*)
(defn db [] (db/db *conn*))
(defn uri [] (str "datomic:mem://" (gensym "temporary-database")))

(defn make-db [schema] 
  (let [uri (uri), db (db/create-database uri), c (db/connect uri)]
    @(db/transact c schema)
    c))

(defmacro with-test-db [schema & body]
  `(binding [*conn* (make-db ~schema)]
     ~@body))


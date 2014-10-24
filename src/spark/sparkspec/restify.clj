(ns spark.sparkspec.restify
  (:require [io.pedestal.http :as http]
            [io.pedestal.http.route :as route]
            [io.pedestal.http.body-params :as body-params]
            [io.pedestal.http.route.definition :refer [defroutes expand-routes]]
            [io.pedestal.log :as log]
            [io.pedestal.service-tools.dev :as dev]
            [ring.util.response :as ring-resp]
            [datomic.api :as d]
            [cheshire.core :as json]
            [clojure.string :refer [lower-case]]
            [clojure.walk :as walk]
            [io.pedestal.interceptor :as i]
            [spark.sparkspec.datomic :as spd]
            [spark.sparkspec :as sp]
            [clojure-csv.core :as csv]
            [clojure.string :as str])
  (:import java.lang.Throwable))

(defn explicitly-tag
  "deep-walks a sp object adding explicit :spec-tacular/spec
   spec name tags to the object and all its child items."
  [sp]
  (let [spec (sp/get-spec sp)
        {recs :rec non-recs :non-rec} (group-by sp/recursiveness (:items spec))
        sub-kvs (->> recs
                     (map (fn [{[arity sub-spec-name] :type :as item}]
                            (let [sub-sp (get sp (:name item))]
                              (cond
                               (and (= arity :one) (some? sub-sp)) ; only build non-nil sub-sps
                               , [(:name item) (explicitly-tag sub-sp)]
                               (and (= arity :many) (some? sub-sp))
                               , [(:name item) (map #(explicitly-tag %) sub-sp)]
                               :else nil))))
                     (filter some?))]
    (into (merge sp {:spec-tacular/spec (:name spec)}) sub-kvs)))

(defn to-json-friendly
  "converts sp object to json representation with explicit spec tags
  with the un-namespaced :spec-tacular-spec keyword"
  [sp]
  (->> (explicitly-tag sp)
       (walk/postwalk
        (fn [o] (if (and (map? o) (contains? o :spec-tacular/spec))
                  (-> o
                      (assoc :spec-tacular-spec (get o :spec-tacular/spec))
                      (dissoc :spec-tacular/spec))
                  o)))))

(defn from-json-friendly
  "converts from the json converted rep to the given spark record re-namespacing the 
  spec tag :spec-tacular-spec to :spec-tacular/spec.
  Also converts the spec tag back into a keyword, (json would have stringed it)"
  [spec-name jf]
  (->> jf
       (walk/postwalk
        (fn [o] (if (and (map? o) (get o :spec-tacular-spec))
                  (-> o
                      (assoc :spec-tacular/spec (keyword (get o :spec-tacular-spec)))
                      (dissoc :spec-tacular-spec))
                  o)))
       (sp/recursive-ctor spec-name)))

(defn- handler
  "Helper function for making Pedestal handlers from request->response
  functions and some data."
  [n spec f]
  (i/handler
   (keyword (-> *ns* ns-name str) (str (-> spec :name name) \- n)) f))

;; Utility functions for flattening a list of items into CSV output

(defn csv-extract-fields
  "returns list of pairs of colName and colValues, where colName is a list of 'item' names representing the nesting structure.
  eg ([(:phones) 'List of 0 phones'] [(:first-name) nil] [(:some-nested-thing :first-name) nil])"
  [spec-name sp]
  (let [spec (sp/get-spec spec-name)]
    (if (:elements spec)
      [[(list spec-name) (:name (sp/get-spec sp))]]
      (let [{recs :rec non-recs :non-rec} (group-by sp/recursiveness (:items spec))
            rec-cols (->> recs
                          (map (fn [{[arity sub-spec-name] :type :as item}]
                                 (let [sub-sp (get sp (:name item))]
                                   (cond
                                     (= arity :one)
                                     ;TODO: extract this and share w/ :many-arity case?
                                     , (->> (csv-extract-fields sub-spec-name sub-sp) ; :grandparentAttribute -> [[[:parentAttribute :subAttribute] col-value],...]
                                            (map (fn [[colName colVal]]
                                                   [(cons (:name item) colName) colVal])))
                                     (= arity :many)
                                     , (let [key-name (name (:name item))
                                             ;for arrays, we want to expand columns for the FIRST element in the array
                                             ;TODO: extract this and share w/ :one-arity case?
                                             sub-fields (->> (csv-extract-fields sub-spec-name (first sub-sp))
                                                            (map (fn [[colName colVal]]
                                                                   [(cons (str key-name "[0]") colName) colVal])))]
                                         (concat [[(list (str key-name "[].count")) (count sub-sp)]] sub-fields))
                                     :else nil))))
                          (filter some?)
                          (apply concat))
            non-rec-cols (->> non-recs
                              (map (fn [item]
                                     [(list (:name item)) (get sp (:name item))])))]
        (concat non-rec-cols rec-cols)))))

(defn csv-build
  "returns a comma delimited string listing all instances of a given spec.
   usage like (build-csv :User (sd/get-all-of-type (db/get-db) (sp/get-spec :User)))"
  [sp-name sp-list]
  (let [col-names (->> (csv-extract-fields sp-name nil)
                       (map first)
                       (map #(clojure.string/join "." (map name %))))
        vals (map (fn [sp]
                    (->> (csv-extract-fields sp-name sp)
                         (map (comp str second))))
                  sp-list)]
    (csv/write-csv (cons col-names vals))))

(defn- mk-coll-get-response-json
  "Returns JSON describing the collection, in a ring-response. If ?simple=true is passed as a query param, then the items
  will go through a transformation function which returns {:value value :display \"display\"} representation"
  [req eids sp-list simple-repr-fn]
  (let [tagged (map to-json-friendly sp-list)
        simple-mode (and (fn? simple-repr-fn)
                         (= (-> req :query-params :simple) "true"))
        result (if simple-mode (map simple-repr-fn tagged) tagged)]
    (ring-resp/response result)))

(defn- mk-coll-get-response-csv
  "Returns CSV describing the collection, in a ring-response.  :many-arity sub-items are ignored, and instead
  are output as a list indicating the number of values present."
  [spec-name sp-list]
  (-> (csv-build spec-name sp-list)
    (ring-resp/response)
    (ring-resp/content-type "text/csv")))

(defn- make-downloadable
  "utility function which accepts a ring-response and a filename, and wraps
  the response with a content-disposition: attachment header if filename isn't nil,
  forcing the response to show up as a save-as. This is useful for export functions.
  Note that \n, \r,  and double quotes in the file name are disallowed and will be removed"
  [ring-resp filename]
  (let [filename-escaped (some-> filename (str/replace "\"" "") (str/replace "\n" "") (str/replace "\r" ""))]
    (if (and (some? filename-escaped) (some? ring-resp))
      ; if a filename is provided in the query param (?filename=foo.csv), force a download
      (ring-resp/header ring-resp "Content-Disposition" (str "attachment; filename=\"" filename-escaped "\""))
      ; otherwise we want to just return the raw text to display in-browser
      ring-resp)))

(defn- mk-coll-list-all
  "Helper function for building handlers that return all elements of a certain spec."
  [spec get-conn-ctx-fn simple-repr-fn]
  (handler
    "coll-list-all"
    spec
    (fn [req]
      (let [db (d/db (:conn (get-conn-ctx-fn)))
            eids (spd/get-all-eids db spec)
            spec-name (:name spec)
            sp-list (map #(spd/db->sp db (d/entity db %) spec-name) eids)
            format (-> req :query-params :format)
            filename (some-> req :query-params :filename)]
        (cond
          (= format "csv")
          (-> (mk-coll-get-response-csv spec-name sp-list)
              (make-downloadable filename))
          (or (= format "json") (nil? format))
          (-> (mk-coll-get-response-json req eids sp-list simple-repr-fn)
              (make-downloadable filename))
          :else
          (-> "ERROR: Invalid output format (choices: json, csv)"
              (ring-resp/response)
              (ring-resp/content-type "text/plain")
              (ring-resp/status 400)))))))

(defn- mk-elem-get
  "Helper function for building handlers that get a particular element
  according to its EID and type."
  ;TODO: Respond with 404 if no entity with that ID exists in the DB
  [spec get-conn-ctx-fn]
  (handler
    "elem-get"
    spec
    (fn [{{id :id} :path-params}]
      (let [id (java.lang.Long/valueOf id)
            db (d/db (:conn (get-conn-ctx-fn)))
            ent (d/entity db id)]
        (assert (some? ent) "entity should exist in the database")
        (ring-resp/response
          (to-json-friendly (spd/db->sp db ent (:name spec))))))))

(defn- mk-elem-create
  "Helper function for building handlers that add a particular element
  to the database."
  [spec get-conn-ctx-fn parent-route]
  (handler
   "coll-post"
   spec
   (fn [req]
     (let [sp (from-json-friendly (:name spec) (:json-params req))
           conn-ctx (get-conn-ctx-fn)
           db (d/db (:conn conn-ctx))]
       (assert (not (spd/get-eid db sp)) "object must not already be in the db")
       (let [txs (spd/sp->transactions db sp)
             _ (log/info :msg "about to commit" :data txs)
             eid (spd/commit-sp-transactions conn-ctx txs)
             url (str "/api/v1" parent-route "/" eid)] ;TODO: don't hardcode /api/v1?
         (ring-resp/created url))))))

(defn- mk-elem-update
  "Helper function for building handlers that modify a particular
  element (by EID) in the database with information given in the
  body."
  ;TODO: Respond with 404 if no entity with that ID exists in the DB
  [spec get-conn-ctx-fn]
  (handler
   "elem-put"
   spec
   (fn [{{id :id} :path-params json :json-params :as req}]
     (let [id (java.lang.Long/valueOf id)
           conn-ctx (get-conn-ctx-fn)
           db (d/db (:conn conn-ctx))
           new (from-json-friendly (:name spec) json)]
       (spd/commit-sp-transactions conn-ctx (spd/sp->transactions db new))
       (ring-resp/response (to-json-friendly new))))))

(defn- mk-elem-delete 
  "Helper function for building handlers that delete a particular
  element (by EID) in the database with information given in the
  body."
  ;TODO: Respond with 404 if no entity with that ID exists in the DB
  [spec get-conn-ctx-fn]
  (handler
   "elem-delete"
   spec
   (fn [{{id :id} :path-params}]
     (spd/commit-sp-transactions (get-conn-ctx-fn) [[:db.fn/retractEntity (Long/valueOf id)]])
     (ring-resp/response ""))))

(defn make-routes
  "Creates a list of routes for a RESTful API for the given
  spec. Allows for get/post on the collection and get/put/delete on
  individual resources.  expects body-params, html-body and json-body
  interceptors."
  [parent-route spec get-conn-ctx-fn & [simple-repr-fn]]
  [parent-route
   {:get (mk-coll-list-all spec get-conn-ctx-fn simple-repr-fn)
    :post (mk-elem-create spec get-conn-ctx-fn parent-route)}
   ["/:id" 
    {:get (mk-elem-get spec get-conn-ctx-fn)
     :put (mk-elem-update spec get-conn-ctx-fn)
     :delete (mk-elem-delete spec get-conn-ctx-fn)}]])

;; TODO: document this better in terms of how
(defn make-expanded-routes
  "Creates a list of routes for a RESTful API for the given
  spec. Allows for get/post on the collection and get/put/delete on
  individual resources."
  [spec get-conn-ctx-fn]
  (let [parent-route (str "/" (-> spec :name name lower-case))
        api-name (keyword (str (name (:name spec)) "-API"))]
    (expand-routes
     [[api-name
       (conj (make-routes parent-route spec get-conn-ctx-fn)
             ^:interceptors
             [(body-params/body-params)
              http/html-body
              http/json-body])]])))

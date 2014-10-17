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
            [spark.sparkspec :as sp])
  (:import java.lang.Throwable))

(defn make-url 
  "Helper function for building resource URLs."
  [{:keys [remote-addr servlet-path]} eid]
  (format "http://%s%s/%s"
          remote-addr servlet-path eid))

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

(defn- mk-coll-get
  "Helper function for building handlers that return all elements of a
  certain spec.  If ?simple=true is passed as a query param, then the items
  will go through a transformation function which returns {:value value :display \"display\"} representation"
  [spec get-conn-ctx-fn simple-repr-fn]
  (handler
   "coll-get"
   spec
   (fn [req]
     (let [db (d/db (:conn (get-conn-ctx-fn)))
           eids (spd/get-all-eids db spec)]
       (ring-resp/response
        {:data {:locations (map #(make-url req %) eids)
                :items (let [result (map (fn [eid] (to-json-friendly (spd/db->sp db (d/entity db eid) (:name spec))))
                                         eids)]
                         (if (and (fn? simple-repr-fn) (= (-> req :query-params :simple) "true")) (map simple-repr-fn result) result))}})))))

(defn- mk-coll-post 
  "Helper function for building handlers that add a particular element
  to the database."
  [spec get-conn-ctx-fn]
  (handler
   "coll-post"
   spec
   (fn [req]
     (let [sp (from-json-friendly (:name spec) (:data (:json-params req)))
           conn-ctx (get-conn-ctx-fn)
           db (d/db (:conn conn-ctx))]
       (assert (not (spd/get-eid db sp)) 
               "object must not already be in the db")
       (let [txs (spd/sp->transactions db sp)
             _ (log/info :msg "about to commit" :data txs)
             eid (spd/commit-sp-transactions conn-ctx txs)
             url (make-url req eid)]
         (ring-resp/created url))))))

(defn- mk-elem-get 
  "Helper function for building handlers that get a particular element
  according to its EID and type."
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
        {:data (to-json-friendly (spd/db->sp db ent (:name spec)))})))))

(defn- mk-elem-put
  "Helper function for building handlers that modify a particular
  element (by EID) in the database with information given in the
  body."
  [spec get-conn-ctx-fn]
  (handler
   "elem-put"
   spec
   (fn [{{id :id} :path-params json :json-params :as req}]
     (let [id (java.lang.Long/valueOf id)
           conn-ctx (get-conn-ctx-fn)
           db (d/db (:conn conn-ctx))
           new (from-json-friendly (:name spec) (:data json))]
       (spd/commit-sp-transactions conn-ctx (spd/sp->transactions db new))
       (ring-resp/response {:body {:new new}})))))

(defn- mk-elem-delete 
  "Helper function for building handlers that delete a particular
  element (by EID) in the database with information given in the
  body."
  [spec get-conn-ctx-fn]
  (handler
   "elem-delete"
   spec
   (fn [{{id :id} :path-params}]
     (spd/commit-sp-transactions (get-conn-ctx-fn) [[:db.fn/retractEntity (Long/valueOf id)]])
     (ring-resp/response
      {:body {:deleted id}}))))

(defn make-routes
  "Creates a list of routes for a RESTful API for the given
  spec. Allows for get/post on the collection and get/put/delete on
  individual resources.  expects body-params, html-body and json-body
  interceptors."
  [parent-route spec get-conn-ctx-fn & [simple-repr-fn]]
  [parent-route
   {:get (mk-coll-get spec get-conn-ctx-fn simple-repr-fn)
    :post (mk-coll-post spec get-conn-ctx-fn)}
   ["/:id" 
    {:get (mk-elem-get spec get-conn-ctx-fn)
     :put (mk-elem-put spec get-conn-ctx-fn)
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

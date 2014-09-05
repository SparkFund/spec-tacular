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
            [io.pedestal.interceptor :as i]
            [spark.sparkspec.datomic :as spd]
            [spark.sparkspec :as sp])
  (:import java.lang.Throwable))

(defn make-url 
  "Helper function for building resource URLs."
  [{:keys [remote-addr servlet-path]} eid]
  (format "http://%s%s/%s"
          remote-addr servlet-path eid))

(defn- handler
  "Helper function for making Pedestal handlers from request->response
  functions and some data."
  [n spec f]
  (i/handler
   (keyword (-> *ns* ns-name str) (str (-> spec :name name) \- n)) f))

(defn- mk-coll-get
  "Helper function for building handlers that return all elements of a
  certain spec."
  [spec get-conn-fn]
  (handler
   "coll-get"
   spec
   (fn [req]
     (let [db (d/db (get-conn-fn))
           eids (spd/get-all-eids db spec)]
       (ring-resp/response
        {:data {:locations (map #(make-url req %) eids)
                :items (map (fn [eid]
                              (spd/db->sp db (d/entity db eid) (:name spec)))
                            eids)}})))))

(defn- mk-coll-post 
  "Helper function for building handlers that add a particular element
  to the database."
  [spec get-conn-fn]
  (handler
   "coll-post"
   spec
   (fn [req]
     (let [sp (sp/recursive-ctor (:name spec) (:data (:json-params req)))
           conn (get-conn-fn)
           db (d/db (get-conn-fn))]
       (assert (not (spd/get-eid db sp)) 
               "object must not already be in the db")
       (let [txs (spd/sp->transactions db sp)
             _ (log/info :msg "about to commit" :data txs)
             eid (spd/commit-sp-transactions conn txs)
             url (make-url req eid)]
         (ring-resp/created url))))))

(defn- mk-elem-get 
  "Helper function for building handlers that get a particular element
  according to its EID and type."
  [spec get-conn-fn]
  (handler
   "elem-get"
   spec
   (fn [{{id :id} :path-params}]
     (let [id (java.lang.Long/valueOf id)
           db (d/db (get-conn-fn))
           ent (d/entity db id)]
       (assert (some? ent) "entity should exist in the database")
       (ring-resp/response 
        {:data (spd/db->sp db ent (:name spec))})))))

(defn- mk-elem-put
  "Helper function for building handlers that modify a particular
  element (by EID) in the database with information given in the
  body."
  [spec get-conn-fn]
  (handler
   "elem-put"
   spec
   (fn [{{id :id} :path-params json :json-params :as req}]
     (let [id (java.lang.Long/valueOf id)
           db (d/db (get-conn-fn))
           ctor (sp/get-ctor spec)
           new (ctor (:data json))]
       @(d/transact (get-conn-fn) (spd/sp->transactions db new))
       (ring-resp/response {:body {:new new}})))))

(defn- mk-elem-delete 
  "Helper function for building handlers that delete a particular
  element (by EID) in the database with information given in the
  body."
  [spec get-conn-fn]
  (handler
   "elem-delete"
   spec
   (fn [{{id :id} :path-params}]
     @(d/transact (get-conn-fn) [[:db.fn/retractEntity (Long/valueOf id)]])
     (ring-resp/response
      {:body {:deleted id}}))))

(defn make-routes
  "Creates a list of routes for a RESTful API for the given
  spec. Allows for get/post on the collection and get/put/delete on
  individual resources.  expects body-params, html-body and json-body
  interceptors."
  [parent-route spec get-conn-fn]
  [parent-route
   {:get (mk-coll-get spec get-conn-fn)
    :post (mk-coll-post spec get-conn-fn)}
   ["/:id" 
    {:get (mk-elem-get spec get-conn-fn)
     :put (mk-elem-put spec get-conn-fn)
     :delete (mk-elem-delete spec get-conn-fn)}]])

;; TODO: document this better in terms of how
(defn make-expanded-routes
  "Creates a list of routes for a RESTful API for the given
  spec. Allows for get/post on the collection and get/put/delete on
  individual resources."
  [spec get-conn-fn]
  (let [parent-route (str "/" (-> spec :name name lower-case))
        api-name (keyword (str (name (:name spec)) "-API"))]
    (expand-routes
     [[api-name
       (conj (make-routes parent-route spec get-conn-fn)
             ^:interceptors
             [(body-params/body-params)
              http/html-body
              http/json-body])]])))

(ns spark.sparkspec.restify-test
  (:require [spark.sparkspec.restify :refer :all]
            [io.pedestal.http :as http]
            [io.pedestal.http.route :as route]
            [io.pedestal.http.body-params :as body-params]
            [io.pedestal.http.route.definition :refer [defroutes expand-routes]]
            [io.pedestal.service-tools.dev :as dev]
            [ring.util.response :as ring-resp]
            [datomic.api :as d]
            [cheshire.core :as json]
            [clojure.string :refer [lower-case]]
            [io.pedestal.interceptor :as i]
            [spark.sparkspec.datomic :as spd]
            [spark.sparkspec.test-utils :refer :all]
            [spark.sparkspec :as sp]
            [clojure.test :refer :all]
            [io.pedestal.test :refer :all]))

(def schema
  [{:db/id #db/id [:db.part/db]
    :db/unique :db.unique/identity
    :db/ident :testspec/val1
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one
    :db/doc ""
    :db.install/_attribute :db.part/db}
   {:db/id #db/id [:db.part/db]
    :db/ident :testspec/val2
    :db/valueType :db.type/long
    :db/cardinality :db.cardinality/many
    :db/doc ""
    :db.install/_attribute :db.part/db}
   {:db/id #db/id [:db.part/db]
    :db/ident :testspec/val3
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/many
    :db/doc ""
    :db.install/_attribute :db.part/db}
   {:db/id #db/id [:db.part/db]
    :db/ident :spec-tacular/spec
    :db/valueType :db.type/keyword
    :db/cardinality :db.cardinality/one
    :db/doc ""
    :db.install/_attribute :db.part/db}])

(sp/defspec TestSpec 
  [val1 :is-a :string :unique :identity]
  [val2 :is-many :long]
  [val3 :is-a :TestSpec])

(def seed 
  [{:db/id #db/id[:db.part/user -1] 
    :spec-tacular/spec :TestSpec
    :testspec/val1 "hi"
    :testspec/val2 13124}])

;(def routes (make-expanded-routes (sp/get-spec :TestSpec) (fn [] {:conn *conn*})))

(def service {::http/routes #(make-expanded-routes (sp/get-spec :TestSpec) (fn [] {:conn *conn*})) 
              ::http/port 8080})

(def server (::http/service-fn (-> service dev/init http/create-server)))

(defmacro with-new-db [& body]
  `(with-test-db schema
    @(d/transact *conn* seed)
    ~@body))

(deftest get-collection
  (with-new-db
    (let [{:keys [body status] :as res} (response-for server :get "/testspec")
          body (json/parse-string body true)]
      (is (= 200 status))
      (is (= 1 (count (get-in body [:data :locations])))))))

(deftest post-collection
  (with-new-db
    (let [{status :status {loc "Location"} :headers}
          (response-for server :post "/testspec"
                        :headers {"Content-Type" "application/json"}
                        :body (json/encode {:data {:val1 "woah" :val2 [124]}}))
          eid (Long/valueOf (second (re-matches #"http://.*/testspec/(\d+)" loc)))]
      (is (= 201 status)
          "When we create an entity, we should get back the appropriate
        HTTP response (201).")
      (is (and loc (re-matches #"http://.*/testspec/(\w+)" loc))
             "We should get something that vaguely looks like a URL for the
        resource we created.")
      (is (= "woah" (:testspec/val1 (d/entity (db) eid)))))))

(deftest get-element
  (with-new-db
    (let [query '[:find ?eid :where [?eid :testspec/val1 "hi"]]
          seed-eid (ffirst (d/q query (db)))
          {:keys [status body]} (response-for server :get 
                                              (str "/testspec/" seed-eid))]
      (is (-> body (json/parse-string true) testspec testspec?)))))

(deftest put-element
  (testing "when :old matches the DB, we commit the data"
    (with-new-db
      (let [query '[:find ?eid :where [?eid :testspec/val1 "hi"]]
            seed-eid (ffirst (d/q query (db)))
            orig (spd/db->sp (db) (d/entity (db) seed-eid) :TestSpec)
            {:keys [status body]} (response-for server :put (str "/testspec/" seed-eid)
                                                :headers {"Content-Type" "application/json"}
                                                :body
                                                (json/encode
                                                 {:old orig
                                                  :new {:val1 "hi" :val2 #{45552}}}))]
        (is (= status 200))
        (is (= (contains? (:testspec/val2 (d/entity (db) (ffirst (d/q query (db)))))
                          45552))))))
#_
  (testing "when :old does not match the DB, we do not commit the data"
    (with-new-db
      (let [query '[:find ?eid :where [?eid :testspec/val1 "hi"]]
            seed-eid (ffirst (d/q query (db)))
            orig (spd/db->sp (db) (d/entity (db) seed-eid) :TestSpec)
            {:keys [status body]} (response-for server :put (str "/testspec/" seed-eid)
                                                :headers {"Content-Type" "application/json"}
                                                :body
                                                (json/encode
                                                 {:old (assoc orig :val2 #{00000})
                                                  :new {:val1 "hi" :val2 #{45552}}}))]
        (is (not (= status 200)))
        (is (= (set (:testspec/val2 (d/entity (db) (ffirst (d/q query (db))))))
               (set (:val2 orig))))))))

(deftest delete-element
  (with-new-db
    (let [query '[:find ?eid :where [?eid :testspec/val1 "hi"]]
          seed-eid (ffirst (d/q query (db)))
          {:keys [status body]} (response-for server :delete
                                              (str "/testspec/" seed-eid))]
      (is (= status 200) "DELETE expects a 200")
      (is (= (-> body (json/parse-string true) :body :deleted Long/valueOf) seed-eid)
          "We should get the entity ID of what we deleted")
      (is (= (into {} (d/entity (db) seed-eid)) {}) "The entity we delete should go away"))))

(deftest test-json
  (let [sp (testspec {:val1 "atest" :val2 [111 222] :val3 (testspec {})})]
    (is (= sp (from-json-friendly :TestSpec (to-json-friendly sp))))))

(ns spark.spec-tacular.readme-test
  (:refer-clojure :exclude [cat])
  (:use clojure.test)
  (:require [spark.spec-tacular.schema :as schema]
            [spark.spec-tacular.datomic :as sd]
            [spark.spec-tacular :as sp :refer [defspec defunion defenum]]))

;; Sets up a House entity containing a mandantory color and optionally
;; a Mailbox. It may also link in any number of Occupants.
(defspec House
  (:link [occupants :is-many :Occupant])
  [mailbox :is-a :Mailbox]               
  [color :is-a :Color :required])       

(defenum Color
  green, orange)

(defspec Mailbox
  [has-mail? :is-a :boolean])

;; Houses can be occupied by either People or Pets.
(defunion Occupant :Person :Pet)

;; Each Person has a name that serves as an identifying field
;; (implemented as Datomic's notion of identity), and an age.
(defspec Person
  [name :is-a :string :identity :unique]
  [age :is-a :long])

(defunion Pet :Dog :Cat :Porcupine)

(defspec Dog
  [fleas? :is-a :boolean])

;; Cats can contain links (passed by reference to the database) to all
;; the occupants of the house that they hate.  For their nefarious
;; plots, no doubt.
(defspec Cat
  [hates :is-many :Occupant :link])

(defspec Porcupine) ;; No fields, porcupines are boring

;;; Creating Databases

(deftest test-readme-creating-databases
  (is (every? map? (schema/from-namespace *ns*)))
  (is (instance? datomic.peer.LocalConnection (schema/to-database! (schema/from-namespace *ns*)))))

(deftest test-readme-interfacing-databases

  ;; Use the House schema to create a database and connection
  (def conn-ctx {:conn (schema/to-database! (schema/from-namespace 'spark.spec-tacular.readme-test))})
  (def h (sd/create! conn-ctx (house {:color :Color/green})))

  ;; Some quick semantics:
  (is (= (:color h) :Color/green))
  (is (= (= h (house {:color :Color/green})) false))
  (is (= (sp/refless= h (house {:color :Color/green})) true))
  (is (thrown-with-msg? clojure.lang.ExceptionInfo #"field"
                        (assoc h :random-kw 42)))
  (is (= (set [h h]) #{h}))
  (is (= (set [h (house {:color :Color/green})]) #{h (house {:color :Color/green})}))

  (def joe     (sd/create! conn-ctx (person {:name "Joe" :age 32})))
  (def bernard (sd/create! conn-ctx (person {:name "Bernard" :age 25})))

  (def new-h (sd/assoc! conn-ctx h :occupants [joe bernard]))

  (is (= (sp/refless= h (house {:color :Color/green})) true))
  (is (= (sd/refresh conn-ctx h) new-h))

  (def zuzu (sd/create! conn-ctx (cat {:hates (:occupants new-h)})))
  (sd/assoc! conn-ctx h :occupants (conj (:occupants new-h) zuzu))

  (let [mb (mailbox {:has-mail? false})
        h1 (sd/assoc! conn-ctx h :mailbox mb)
        h2 (sd/create! conn-ctx (house {:color :Color/orange :mailbox mb}))]
    ;; But since Mailboxes are passed by value,
    ;; the Mailbox get duplicated
    (is (= (= (:mailbox h1) (:mailbox h2)) false))

    (def mb1 (sd/assoc! conn-ctx (:mailbox h1) :has-mail? true))
    (def db (sd/db conn-ctx))
    (is (= (sd/q :find [:Mailbox ...] :in db :where [% {:has-mail? false}]) #{(:mailbox h2)}))
    (is (= (sd/q :find [:Mailbox ...] :in db :where [% {:has-mail? true}])  #{mb1}))
    (is (= (sd/q :find [:House ...] :in db :where
                 [% {:mailbox {:has-mail? false}}])
           #{h2}))
    (is (= (sd/q :find :House, :Person :in db :where
                 [%1 {:occupants %2 :mailbox {:has-mail? true}}])
           #{[(sd/refresh conn-ctx h1) joe] [(sd/refresh conn-ctx h1) bernard]}))
    (is (= (sd/q :find [:string ...] :in db :where
                 [:House {:occupants [:Person {:name %}]}])
           #{"Joe" "Bernard"}))))

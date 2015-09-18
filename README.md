# spec-tacular

Write spectacular data definitions!  Our goal is to make the border
between Clojure and Datomic a more convenient, safe, and place to live.

Define your Datomic schemas using spec-tactular's spec DSL and receive
the following in return:

* **Representation of Datomic entities as maps** that verify (upon
   creation and association) that entity attributes have the correct
   fields, and in turn, the correct types
   
* **core.typed aliases for each spec**

* **Specialized query language** with a map-like syntax that allows
   queries to be expressed with domain-specific spec keywords instead
   of Datomic attribute-keywords.  Entities returned from queries are
   lazily constructed and can be used in typed code without extra
   casts.

* **Simple transaction interface with Datomic**, using `create!` as a
   constructor, and `assoc!` as an update function.

***WARNING:*** spec-tactular is under active development, and makes no
 claims of stability.  It currently anticipates single-threaded,
 single-peer interactions with Datomic, and may act strangely if
 either of those invariants are broken.

## Quick Start

```clojure
[spec-tacular "0.5.0"] ;; unstable
```

```maven
<dependency>
  <groupId>spec-tacular</groupId>
  <artifactId>spec-tacular</artifactId>
  <version>0.5.0</version>
</dependency>
```

## Usage

### Creating Specs

```clojure
(require '[spark.spec-tacular :as sp :refer [defspec defunion]])

;; Sets up a House entity containing a mandantory color and optionally
;; a Mailbox. It may also link in any number of Occupants.
(defspec House
  (:link [occupants :is-many :Occupant])
  [mailbox :is-a :Mailbox]               
  [color :is-a :string :required])       

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
```

### Creating Databases
```clojure
(require '[spark.spec-tacular.schema :as schema])

;; Returns a schema with entries for each spec defined in my-ns
(schema/from-namespace *ns*)
;; => ({:db/id ....,
;;      :db/ident :house/occupants,
;;      :db/valueType :db.type/ref,
;;      :db/cardinality :db.cardinality/many,
;;      ....}
;;     ....)

;; Creates a database with the earlier schema installed.
;; Returns a connection to that database.
(schema/to-database! (schema/from-namespace *ns*))
;; => #<LocalConnection datomic.peer.LocalConnection@....>
```

### Interfacing with Databases
```clojure
(require '[spark.spec-tacular.datomic :as sd])

;; Use the House schema to create a database and connection
(def conn-ctx {:conn (schema/to-database! (schema/from-namespace *ns*))})

;; Create a red house:
(def h (sd/create! conn-ctx (house {:color "Red"})))

;; Some quick semantics:
(:color h)                             ;; => "Red"
(= h (house {:color "Red"}))           ;; => false
(sp/refless= h (house {:color "Red"})) ;; => true
(assoc h :random-kw 42)                ;; => error
(set [h h])                            ;; => #{h}
(set [h (house {:color "Red"})])       ;; => #{h (house {:color "Red"})}

;; Let some people move in:
(def joe     (sd/create! conn-ctx (person {:name "Joe" :age 32})))
(def bernard (sd/create! conn-ctx (person {:name "Bernard" :age 25})))

(def new-h (sd/assoc! conn-ctx h :occupants [joe bernard]))
;; => assoc! returns a new House with the new field

h ;; => is still the simple red house
(sd/refresh conn-ctx h) ;; => new-h
;; In most cases, you can forego the `refresh` and just use the return
;; value of `assoc!`

;; Bernard and Joe get a cat, who hates both of them,
(def zuzu (sd/create! conn-ctx (cat {:hates (:occupants new-h)})))
(sd/assoc! conn-ctx h :occupants (conj (:occupants new-h) zuzu))

;; They build a mailbox, and try to put it up in another House:
(let [mb (mailbox {:has-mail? false})
      h1 (sd/assoc! conn-ctx h :mailbox mb)
      h2 (sd/create! conn-ctx (house {:color "Blue" :mailbox mb}))]
  ;; But since Mailboxes are passed by value,
  ;; the Mailbox get duplicated
  (= (:mailbox h1) (:mailbox h2)) ;; => false
  ....)
````

### Queries

TODO

## License

Copyright © 2014-2015 SparkFund Community Investment

Distributed under the Apache License Version 2.0

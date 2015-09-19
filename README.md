# <a href="https://github.com/SparkFund/spec-tacular"><img src="https://avatars2.githubusercontent.com/u/7240335?v=3&s=200"></a>spec-tacular

Write spectacular data definitions!  Our goal is to make the border
between Clojure and Datomic a more convenient and safe place to live.
[Browse the API](http://sparkfund.github.io/spec-tacular) or continue
scrolling.

Define your Datomic schemas using spec-tacular's spec DSL and receive
the following in return:

* **Representation of Datomic entities as maps** that verify (upon
   creation and association) that entity attributes have the correct
   fields, and in turn, the correct types
   
* **Core Typed aliases for each spec**

* **Specialized query language** with a map-like syntax that allows
   queries to be expressed with domain-specific spec keywords instead
   of Datomic attribute-keywords.  Entities returned from queries are
   lazily constructed and can be used in typed code without extra
   casts.

* **Simple transaction interface with Datomic**, using `create!` as a
   constructor, and `assoc!` as an update function.

***WARNING:*** spec-tacular is under active development, and makes no
 claims of stability.  It currently anticipates single-threaded,
 single-peer interactions with Datomic, and may act strangely if
 either of those invariants are broken.

## Quick Start

```clojure
[spec-tacular "0.5.0"] ;; unstable
```

```xml
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
```

```clojure

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
```

```clojure
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

### Changing Databases
```clojure
(require '[spark.spec-tacular.datomic :as sd])
```

```clojure
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

### Querying Databases
```clojure
(require '[spark.spec-tacular.datomic :as sd])
```

```clojure

;; First let's distinguish the mailboxes -- let's say Joe and Bernard
;; get some mail
(def mb1 (sd/assoc! conn-ctx (:mailbox h1) :has-mail? true))

;; Get the database
(def db (sd/db conn-ctx))

;; Use % to look for the only find variable
(sd/q :find [:Mailbox ...] :in db :where [% {:has-mail? false}])
;; => #{(:mailbox h2)}, the mailbox from house h2
(sd/q :find [:Mailbox ...] :in db :where [% {:has-mail? true}])
;; => #{mb1}, that's Joe and Bernard's mailbox

;; Find the Houses without mail
(sd/q :find [:House ...] :in db :where
      [% {:mailbox {:has-mail false}}])
;; => #{h2}

;; Find the House and it's human occupants when the mailbox has mail
;; Use %1 and %2 to to look for multiple find variables
(sd/q :find :House :Person :in db :where
      [%1 {:occupants %2 :mailbox {:has-mail true}}])
;; => #{[h1 joe] [h2 bernard]}
```

This last example means we're looking for any `:occupants` that are
`:Person`s.  Even though we represent Datomic's cardinality "many" as
a collection in Clojure, we still use a relation to search for members
of that collection on the database.  Those familiar with Datomic may
understand that this part of the query (roughly) expands to

```clojure
[.... [?house :house/occupants ?person] ....]
```

When we get the result of the query back in Clojure, we take that
result and return it as a set.  Onwards!

```clojure

;; If you want to get the spec name of entities on the database, you
;; can use the special :spec-tacular/spec keyword.  Here we restrict
;; the occupants to the :Pet spec and then return all kinds of Pet's
;; that live in houses:
(sd/q :find [:string ...] :in db :where
      [:House {:occupants [:Person {:name %}]}])
;; => #{"Joe" "Bernard"}

```

Although maps work as you would expect in a query, the vector form
`[<spec> <map>]` is protected syntax meaning the `map` should be
restricted to things of type `<spec>`.

## Updating from v.0.4.x

* Replace all `spark.sparkspec` namespaces with `spark.spec-tacular`
* Check all calls to `=` to see if `refless=` is more appropriate
* Check all `set`s if you mix local instances and instances on the
  database; these are nolonger `=` nor do they hash to the same number
  even if they are otherwise equivalent.
* Rename `defenum` to `defunion`

## Short Term Roadmap

* Create `defenum` that mirrors Datomic's enumerations
* Create `:component` spec option
* Create `defattr` that can be used as a field type to allow shared
Datomic namespaces between fields of different specs

## License

Copyright © 2014-2015 [Spark Community Investment](https://www.sparkfund.co)

Distributed under the Apache License Version 2.0

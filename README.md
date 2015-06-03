# spec-tacular

Write spectacular data definitions!  Our goal is to make the border
between Clojure and Datomic a more convenient, safe, and place to live.

Define your Datomic schemas using spec-tactular's spec DSL and receive
the following in return:

* *Representation of Datomic entities as maps* that verify (upon
   creation and association) that entity attributes have the correct
   fields, and in turn, the correct types
   
* *core.typed aliases for each spec*

* *Specialized query language* with a map-like syntax that allows
   queries to be expressed with domain-specific spec keywords instead
   of Datomic attribute-keywords.  Entities returned from queries are
   lazily constructed and can be used in typed code without extra
   casts.

* *Simple transaction interface with Datomic*, using `create!` as a
   constructor, and `assoc!` as an update function.

*WARNING:* spec-tactular is under active development, and makes no
 claims of stability.  It currently anticipates single-threaded,
 single-peer interactions with Datomic.

## Quick Start

```clojure
[spec-tacular "0.4.13"]
```

```maven
<dependency>
  <groupId>spec-tacular</groupId>
  <artifactId>spec-tacular</artifactId>
  <version>0.4.13</version>
</dependency>
```

## Usage

### Creating Specs

```clojure
(require '[spark.sparkspec :refer [defspec]])

;; Sets up a House entity containing a mandantory color and optionally
;; a Mailbox. It may also link in any number of Occupants.
(defspec House
  (:link [occupants :is-many :Occupant])
  [mailbox :is-a :Mailbox]
  [color :is-a :string :required])

(defspec Mailbox
  [has-mail? :is-a :bool])

;; Houses can be occupied by either People or Pets.
(defenum Occupant :Person :Pet)

;; Each Person has a name that serves as an identifying field
;; (implemented as Datomic's notion of identity), and an age.
(defspec Person
  [name :is-a :string :identity]
  [age :is-a number])

(defenum Pet :Dog :Cat :Porcupine)

(defspec Dog
  [fleas? :is-a :bool])

;; Cats can contain links (passed by reference) to all the occupants
;; of the house that they hate.  For their nefarious plots, no doubt.
(defspec Cat
  [hates :is-many :Occupant :link])

(defspec Porcupine)
```

### Creating Databases
```clojure
(require '[spark.sparkspec.schema :as schema])

;; Returns a schema with entries for each spec defined in my-ns
(schema/from-namespace 'my-ns)

;; Creates a database with the earlier schema installed.
;; Returns a connection to that database.
(schema/to-database! (schema/from-namespace 'my-ns))
```

### Interfacing with Databases
```clojure
(require '[spark.sparkspec.datomic :as sd])

(let [conn (schema/to-database! *ns*) ;; Use the House schema

      ;; Create a red House
      h (create! conn (house {:color "Red"}))

      ;; Let some people move in
      j (create! conn (person {:name Joe, :age 32}))
      b (create! conn (person {:name Bernard, :age 25}))
      h (assoc! conn h :occupants [j b])

      ;; They get a cat, who hates both of them
      c (cat {:hates (:occupants h)})
      h (assoc! conn h :occupants (conj (:occupants h) c))])
```

## License

Copyright © 2014-2015 SparkFund Community Investment
Distributed under the Apache License Version 2.0

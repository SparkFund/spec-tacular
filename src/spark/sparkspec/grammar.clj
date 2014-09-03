(ns spark.sparkspec.grammar
  (:use [n01se.syntax]
        [spark.sparkspec.spec])
  (:require [n01se.seqex :refer [cap recap]])
  (:refer-clojure :exclude [name type]))

;;;; Macro Syntax Definitions

(declare item type-rule item-args)

(defterminal name symbol?)
(defterminal type keyword?)
(defterminal func symbol?)
(defterminal lambda list?)
(defterminal funcs vector?)
(defterminal data (fn [_] true))

(defrule func (alt name lambda funcs))

(defrule defspec-rule
  (recap (cat (cap name)
              (cap (rep+ (vec-form (delay item)))))
         (fn [[name] & items] 
           (map->Spec {:name (keyword name) :items (rest items)}))))

(defrule item
  (recap (cat (cap name)
              (cap (opt (delay type-rule))
                   (fn [[cardinality type]]
                     [(case cardinality
                        (:is-a :is-an) :one
                        (:is-many) :many
                        nil)
                      type]))
              (cap (rep* (delay item-args))))
         (fn [[name] type & args]
           (let [arg-info (if args (apply merge (rest args)) {})]
             (map->Item 
              (merge {:name (keyword name) :type type} arg-info))))))

(defrule type-rule (cat (alt :is-a :is-an :is-many) type))

(defrule item-args 
  (recap (alt (cap (cat :precondition func))
              (cap :required)
              (cap :identity)
              (cap :unique)
              (cap :optional)
              (cap (cat :default-value (alt func data))))
         (fn [[v p]]
           (cond (= v :precondition) {v p}
                 (= v :required) {:required? true}
                 (= v :unique) {:unique? true}
                 (= v :identity) {:identity? true}
                 (= v :default-value) {:default-value p}
                 :else {v true}))))

(defrule defenum-rule 
  (recap (cat (cap name) (vec-form (cap (rep+ name))))
         (fn [[name] elements]
           (map->EnumSpec
            {:name (keyword name)
             :elements (map keyword elements)}))))

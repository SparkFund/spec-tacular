(ns spark.spec-tacular.spec
  (:require [clojure.pprint :as pp]
            [clj-time.core :as time]
            [clj-time.format :as timef]
            [clj-time.coerce :as timec]))

;; -----------------------------------------------------------------------------
;; records

(defrecord Spec [name opts items syntax])
(defrecord Item [name type precondition required? unique? optional? identity? default-value parent-name])
(defrecord UnionSpec [name elements])
(defrecord SpecType [name type type-symbol coercion])
(defrecord EnumSpec [name values])

;; -----------------------------------------------------------------------------
;; printing

(defmethod pp/simple-dispatch Spec [spec]
  (let [stx (.syntax spec)]
    (pp/pprint-logical-block
     :prefix "(" :suffix ")"
     (pp/simple-dispatch (symbol (str (first stx) " " (second stx) " ")))
     (pp/pprint-indent :block 1)
     (doseq [item-stx (rest (rest stx))]
       (pp/pprint-newline :linear)
       (if (= (first item-stx) :link)
         (pp/pprint-logical-block
          :prefix "(" :suffix ")"
          (pp/simple-dispatch (symbol (str :link " ")))
          (pp/pprint-indent :block 0)
          (doseq [link (rest item-stx)]
            (pp/pprint-newline :mandatory)
            (pp/simple-dispatch link)))
         (pp/simple-dispatch item-stx))))))

;; -----------------------------------------------------------------------------
;; helpers

(defn make-name
  "Creates a symbol using the name of the spec and the given
  `append-fn`."
  [spec append-fn]
  (-> spec :name name append-fn symbol))

(defn spec->class
  "Returns the symbol for the spec's class"
  [spec]
  (make-name spec #(str "i_" %)))

(defn spec->ctor
  "Returns the symbol for the spec's ctor"
  [spec]
  (make-name spec clojure.string/lower-case))

(defn spec->huh
  [spec]
  (make-name spec #(str (clojure.string/lower-case %) "?")))

(defn spec->alias
  [spec]
  (make-name spec identity))

;; -----------------------------------------------------------------------------
;; primitive types

;;;; There is no existing Java class for a primitive byte array
(def Bytes (class (byte-array [1 2])))

(def type-map
  (reduce
   (fn [m [n t ts c]]
     (assoc m n (map->SpecType {:name n :type t :type-symbol ts :coercion c})))
          {}
          [[:keyword clojure.lang.Keyword `clojure.lang.Keyword keyword]
           [:string String `String nil] ; str Q: Do we lean on "str" coercion?
           [:boolean Boolean `Boolean boolean]
           [:long Long `Long long]
           [:bigint java.math.BigInteger `java.math.BigInteger bigint]
           [:float Float `Float float]
           [:double Double `Double double]
           [:bigdec java.math.BigDecimal `java.math.BigDecimal bigdec]
           [:instant java.util.Date `java.util.Date nil]
           [:calendarday org.joda.time.DateTime `org.joda.time.DateTime timec/to-date-time]
           [:uuid java.util.UUID `java.util.UUID #(if (string? %) (java.util.UUID/fromString %) %)]
           [:uri java.net.URI `java.net.URI nil]
           [:bytes Bytes `Bytes nil]]))

(def ^:constant core-types (into #{} (keys type-map)))

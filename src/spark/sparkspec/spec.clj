(ns spark.sparkspec.spec)

(defrecord Spec [name opts items])
#_(defmethod print-method Spec [v ^java.io.Writer w]
    (.write w (str (:name v))))

(defrecord Item
    [name type precondition required? unique? optional? identity? default-value])

(defrecord EnumSpec [name elements])

;;;; There is no existing Java class for a primitive byte array
(def Bytes (class (byte-array [1 2])))

(defrecord SpecType [name type type-symbol coercion])

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
           [:uuid java.util.UUID `java.util.UUID #(if (string? %) (java.util.UUID/fromString %) %)]
           [:uri java.net.URI `java.net.URI nil]
           [:bytes Bytes `Bytes nil]
           ;; :ref could maybe be datomic.db.DbId ? But it seems Datomic accepts raw integers too?
           ;; TODO: Longs don't seem to be datomic.db.DbIds
           [:ref Object `Object nil]])) 

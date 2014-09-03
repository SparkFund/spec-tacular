(ns spark.sparkspec.spec)

(defrecord Spec [name opts items])
(defrecord Item
    [name type precondition required? unique? optional? identity? default-value])

(defrecord EnumSpec [name elements])

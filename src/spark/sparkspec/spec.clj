(ns spark.sparkspec.spec)

(defrecord Spec [name opts items])
#_(defmethod print-method Spec [v ^java.io.Writer w]
    (.write w (str (:name v))))

(defrecord Item
    [name type precondition required? unique? optional? identity? default-value])

(defrecord EnumSpec [name elements])

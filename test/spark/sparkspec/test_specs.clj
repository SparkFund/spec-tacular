(ns spark.sparkspec.test-specs
  {:core.typed {:collect-only true}}
  (:use spark.sparkspec
        spark.sparkspec.spec
        spark.sparkspec.datomic))

(defspec Scm2
  [val1 :is-a :long :unique :identity])

(defspec Scm
  [val1 :is-a :string :unique :identity]
  [val2 :is-a :long]
  [multi :is-many :string]
  [scm2 :is-a :Scm2])

(defspec Scm3)

(defenum ScmEnum :Scm2 :Scm3)

(defspec ScmOwnsEnum
  [enum :is-a :ScmEnum]
  [enums :is-many :ScmEnum])

(defspec ScmM
  [identity :is-a :string :unique :identity]
  [vals :is-many :Scm2])

(defspec ScmParent
  [scm :is-a :Scm])

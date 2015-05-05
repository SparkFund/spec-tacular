(ns spark.sparkspec.test-specs
  {:core.typed {:collect-only true}}
  (:use spark.sparkspec
        spark.sparkspec.spec
        spark.sparkspec.datomic))

(defspec Scm2
  (:link [val1 :is-a :long :unique :identity]))

(defspec Scm
  (:link
   [val1 :is-a :string :unique :identity]
   [val2 :is-a :long]
   [multi :is-many :string]
   [scm2 :is-a :Scm2]))

(defspec Scm3)

(defenum ScmEnum :Scm2 :Scm3)

(defspec ScmOwnsEnum
  (:link
   [enum :is-a :ScmEnum]
   [enums :is-many :ScmEnum]))

(defspec ScmM
  (:link
   [identity :is-a :string :unique :identity]
   [vals :is-many :Scm2]))

(defspec ScmParent
  (:link 
   [scm :is-a :Scm]))

(defspec ScmReq
  (:link
   [name :is-a :string :required]))

(defspec ScmLink
  (:link [link1 :is-a :Scm]
         [link2 :is-many :Scm2])
  [val1 :is-a :ScmParent])

(defspec ScmMWrap
  (:link
   [name :is-a :string]
   [val :is-a :ScmM]))

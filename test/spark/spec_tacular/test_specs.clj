(ns spark.spec-tacular.test-specs
  {:core.typed {:collect-only true}}
  (:require [clojure.core.typed :as t])
  (:use spark.spec-tacular))

(defspec Scm2
  [val1 :is-a :long])

(defspec Scm
  [val1 :is-a :string :unique :identity]
  [val2 :is-a :long]
  [multi :is-many :string]
  (:link
   [scm2 :is-a :Scm2]))

(defspec Scm3)

(defunion ScmEnum :Scm2 :Scm3 :Scm)

(defspec ScmOwnsEnum
  (:link
   [enum :is-a :ScmEnum]
   [enums :is-many :ScmEnum]))

(defspec ScmM
  [identity :is-a :string :unique :identity]
  (:link [val :is-a :ScmEnum])
  (:link [vals :is-many :Scm2]))

(defspec ScmParent
  (:link [scm :is-a :Scm]))

(defspec ScmReq
  [name :is-a :string :required])

(defspec ScmLink
  (:link 
   [link1 :is-a :Scm]
   [link2 :is-many :Scm2])
  [val1 :is-a :ScmParent])

(defspec ScmMWrap
  [name :is-a :string]
  (:link [val :is-a :ScmM]))

(defspec ScmKw
  [item :is-a :keyword])

(defspec Ferret
  [name :is-a :string])

(defspec Mouse
  [name :is-a :string])

(defunion Animal :Ferret :Mouse)

(defspec Birthday
  [date :is-a :calendarday])

(defspec Container
  [one :is-a :Container :component]
  [many :is-many :Container :component])

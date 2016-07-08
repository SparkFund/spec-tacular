(defproject spec-tacular "0.6.2-SNAPSHOT"
  :description "First-class data specifications for Clojure and Datomic."
  :url "https://github.com/SparkFund/spec-tacular"
  :license {:name "Apache License, Version 2.0"
            :url "http://www.apache.org/licenses/LICENSE-2.0.html"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [com.datomic/datomic-free "0.9.5385"
                  :exclusions [joda-time]]
                 [prismatic/schema "0.2.4"]
                 [org.clojure/test.check "0.9.0"]
                 [org.clojure/tools.macro "0.1.2"]
                 [org.clojure/core.typed "0.3.0"
                  :exclusions [org.clojure/clojure]]
                 [org.clojure/core.match "0.3.0-alpha4"
                  :exclusions [org.ow2.asm/asm-all
                               org.clojure/tools.analyzer
                               org.clojure/tools.reader
                               org.clojure/tools.analyzer.jvm]]
                 [clojure-csv/clojure-csv "2.0.1"]
                 [clj-time "0.9.0"]]
  :plugins [[lein-typed "0.3.5"]]
  :test-selectors {:default (complement :loud)
                   :loud :loud
                   :all (constantly true)}
  :core.typed {:check [spark.spec-tacular.schema
                       spark.spec-tacular.typecheck-test]}
  :codox {:metadata {:doc/format :markdown}
          :source-paths ["src" "test"]
          :namespaces [spark.spec-tacular
                       spark.spec-tacular.datomic
                       spark.spec-tacular.schema
                       spark.spec-tacular.generators]
          :source-uri "https://github.com/SparkFund/spec-tacular/blob/develop/{filepath}#L{line}"}
  :pedantic? :abort
  )

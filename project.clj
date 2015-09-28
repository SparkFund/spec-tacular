(defproject spec-tacular "0.6.0-SNAPSHOT"
  :description "First-class data specifications for Clojure and Datomic."
  :url "https://github.com/SparkFund/spec-tacular"
  :license {:name "Apache License, Version 2.0"
            :url "http://www.apache.org/licenses/LICENSE-2.0.html"}
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [com.datomic/datomic-free "0.9.5130"
                  :exclusions [joda-time]]
                 [prismatic/schema "0.2.4"]
                 [org.clojure/test.check "0.7.0"]
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
  :codox {:defaults {:doc/format :markdown}
          :sources ["src" "test"]
          :include [spark.spec-tacular
                    spark.spec-tacular.datomic
                    spark.spec-tacular.schema
                    spark.spec-tacular.generators]
          :src-dir-uri "https://github.com/SparkFund/spec-tacular/tree/develop/"
          :src-linenum-anchor-prefix "L"}
  :pedantic? :abort)

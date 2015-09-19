(defproject spec-tacular "0.5.0"
  :description "First-class, extendable data specifications for clojure."
  :url "https://github.com/SparkFund/spec-tacular"
  :license {:name "Apache License, Version 2.0"
            :url "http://www.apache.org/licenses/LICENSE-2.0.html"}
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [com.datomic/datomic-free "0.9.5130"
                  :exclusions [joda-time]]
                 [prismatic/schema "0.2.4"]
                 [org.clojure/test.check "0.7.0"]
                 [org.clojure/tools.macro "0.1.2"]
                 [org.clojure/core.typed "0.2.77"]
                 [org.clojure/core.match "0.3.0-alpha4"
                  :exclusions [org.ow2.asm/asm-all
                               org.clojure/tools.analyzer
                               org.clojure/tools.reader
                               org.clojure/tools.analyzer.jvm]]
                 [clj-time "0.9.0"]

                 ;; Restify
                 [clojure-csv/clojure-csv "2.0.1"]
                 [io.pedestal/pedestal.jetty "0.3.0"
                  :exclusions [ch.qos.logback/logback-classic]]
                 [io.pedestal/pedestal.service "0.3.0"
                  :exclusions [commons-codec
                               org.clojure/tools.reader
                               org.slf4j/slf4j-api
                               ch.qos.logback/logback-classic]]
                 [io.pedestal/pedestal.service-tools "0.3.0"
                  :exclusions [org.slf4j/log4j-over-slf4j
                               org.slf4j/jul-to-slf4j
                               org.slf4j/jcl-over-slf4j
                               org.clojure/tools.reader
                               ch.qos.logback/logback-classic
                               commons-codec]]
                 [org.immutant/immutant "1.1.3"
                  :exclusions [commons-codec
                               org.jgroups/jgroups
                               org.jboss.logging/jboss-logging]]
                 [clj-http "0.9.1"
                  :exclusions [commons-codec
                               potemkin]]
                 [ring/ring-core "1.3.0"
                  :exclusions [commons-codec]]]
  :plugins [[lein-typed "0.3.5"]]
  :test-selectors {:default (complement :loud)
                   :loud :loud
                   :all (constantly true)}
  :core.typed {:check [spark.spec-tacular.datomic
                       spark.spec-tacular.schema
                       spark.spec-tacular.test-specs
                       spark.spec-tacular.typecheck-test]}
  :codox {:defaults {:doc/format :markdown}
          :include [spark.spec-tacular
                    spark.spec-tacular.datomic
                    spark.spec-tacular.schema
                    spark.spec-tacular.generators]}
  :pedantic? :abort)

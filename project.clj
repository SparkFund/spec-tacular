(defproject spec-tacular "0.4.1"
  :description "First-class, extendable data specifications for clojure."
  :url "https://github.com/SparkFund/spec-tacular"
  :license {:name "Apache License, Version 2.0"
            :url "http://www.apache.org/licenses/LICENSE-2.0.html"}
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [com.datomic/datomic-free "0.9.4556"]
                 [n01se/seqex "2.0.1"]
                 [prismatic/schema "0.2.4"]
                 [io.pedestal/pedestal.jetty "0.3.0"]
                 [io.pedestal/pedestal.service "0.3.0"]
                 [io.pedestal/pedestal.service-tools "0.3.0"]
                 [ring/ring-core "1.3.0"]
                 [clj-http "0.9.1"]
                 [clojure-csv/clojure-csv "2.0.1"]
                 [org.clojure/tools.macro "0.1.2"]
                 [org.clojure/core.typed "0.2.77"]
                 [org.immutant/immutant "1.1.3"]])

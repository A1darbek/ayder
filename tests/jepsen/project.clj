(defproject jepsen.ayder "0.1.0-SNAPSHOT"
  :description "Jepsen test suite for Ayder"
  :url "https://github.com/A1darbek/ayder"
  :license {:name "Apache-2.0"}
  :dependencies [[org.clojure/clojure "1.11.1"]
                 [org.clojure/tools.cli "1.0.219"]
                 [jepsen "0.3.8"]
                 [cheshire "5.11.0"]
                 [clj-http "3.12.3"]]
  :main jepsen.ayder.runner
  :repl-options {:init-ns jepsen.ayder.runner})

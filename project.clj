(defproject com.wotbrew/relic "0.1.3-SNAPSHOT"
  :description "Functional relational programming for Clojure/Script"
  :url "https://github.com/wotbrew/relic"
  :license {:name "MIT"
            :url "https://opensource.org/licenses/MIT"}
  :dependencies [[org.clojure/clojure "1.10.0"]]
  :repl-options {:init-ns com.wotbrew.relic}
  :profiles {:dev {:source-paths ["dev"]
                   :dependencies [[com.clojure-goes-fast/clj-async-profiler "0.5.1"]
                                  [criterium "0.4.6"]
                                  [org.clojure/data.csv "1.0.0"]
                                  [thheller/shadow-cljs "2.15.13"]
                                  [datascript "1.2.10"]
                                  [reagent "1.1.0"]
                                  [io.airlift.tpch/tpch "0.10"]
                                  [org.clojure/test.check "1.1.0"]]
                   :repl-options {:nrepl-middleware [shadow.cljs.devtools.server.nrepl/middleware]}
                   :injections [(try
                                  (println "Loading dev.clj")
                                  (println "...")
                                  (require 'dev)
                                  (catch Throwable e
                                    (println "An error occurred loading dev")
                                    (.printStackTrace e)))]}})
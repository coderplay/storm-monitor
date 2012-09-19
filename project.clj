(defproject storm-monitor "1.1.0-SNAPSHOT"
  :description "Storm monitor"
  :extra-classpath-dirs ["conf"]
  :dev-dependencies [[lein-exec "0.1"]]
  :dependencies [[org.clojure/clojure "1.2.1"]
                 [org.clojure/clojure-contrib "1.2.0"]
                 [storm/storm "0.7.1"]
                 [org.yaml/snakeyaml "1.9"]
                 [tron   "0.5.2"]])

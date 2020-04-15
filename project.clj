(defproject twarc "0.1.13"
  :description "Doing Quartz the right way"
  :url "https://github.com/prepor/twarc"
  :license {:name "Eclipse Public License",
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [org.quartz-scheduler/quartz "2.3.2"]
                 [org.quartz-scheduler/quartz-jobs "2.3.2"]
                 [org.clojure/tools.logging "1.0.0"]
                 [com.stuartsierra/component "1.0.0"]
                 [prismatic/plumbing "0.5.5"]
                 [org.clojure/core.async "1.1.587"]
                 [org.clojure/java.jdbc "0.7.11"]]
  :javac-options ["-source" "1.6" "-target" "1.6" "-g"]
  :java-source-paths ["java"]
  :deploy-repositories [["releases" :clojars]]
  :profiles {:dev {:dependencies [[org.slf4j/slf4j-api "1.7.30"]
                                  [ch.qos.logback/logback-classic "1.2.3"]
                                  [org.postgresql/postgresql "42.2.12"]]}})

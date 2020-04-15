(ns twarc.test-utils
  (:require [twarc.core :as twarc]
            [clojure.core.async :as async]
            [clojure.java.jdbc :as sql]
            [clojure.java.io :as io])
  (:import [org.quartz JobKey]))


(defn async-res
  ([ch] (async-res ch 5))
  ([ch seconds]
     (async/alt!!
       (async/timeout (* seconds 5000)) (throw (Exception. "Timeout"))
       ch ([v ch] v))))


(def pg-db {:connection-uri (System/getenv "DB_URL")
            :user (System/getenv "DB_USER")
            :password (System/getenv "DB_PASSWORD")})


(defn db-available? []
  (->> pg-db vals (every? some?)))


(defn exec-sql-file
  [resource]
  (sql/db-do-prepared pg-db
                      (-> resource
                          io/resource
                          slurp)))


(def ^:dynamic *scheduler*)


(def props
  {:threadPool.class "org.quartz.simpl.SimpleThreadPool"
   :threadPool.threadCount 1
   :plugin.triggHistory.class
   "org.quartz.plugins.history.LoggingTriggerHistoryPlugin"

   :plugin.jobHistory.class
   "org.quartz.plugins.history.LoggingJobHistoryPlugin"})


(def persistent-props
  (assoc props
         :jobStore.class "org.quartz.impl.jdbcjobstore.JobStoreTX"
         :jobStore.dataSource "db"
         :dataSource.db.driver "org.postgresql.Driver"
         :dataSource.db.URL (:connection-uri pg-db)
         :dataSource.db.user (:user pg-db)
         :dataSource.db.password (:password pg-db)
         :jobStore.driverDelegateClass
         "org.quartz.impl.jdbcjobstore.PostgreSQLDelegate"))


(defn run-with-scheduler [f scheduler]
  (binding [*scheduler* (twarc/start scheduler)]
    (try
      (f)
      (finally
        (twarc/stop *scheduler*)))))


(defn with-scheduler
  [f]
  (when (db-available?)
    (exec-sql-file "tables_postgres.sql")
    (run-with-scheduler f (twarc/make-scheduler persistent-props)))
  (run-with-scheduler f (twarc/make-scheduler props)))

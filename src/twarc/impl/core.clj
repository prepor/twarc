(ns twarc.impl.core
  (:require [com.stuartsierra.component :as component]
            [clojure.tools.logging :as log])
  (:import [java.util UUID]
           [org.quartz.spi JobFactory]
           [org.quartz SchedulerException]
           [org.quartz.impl StdSchedulerFactory SchedulerRepository]))

(defn uuid [] (-> (UUID/randomUUID) str))

(defn map->properties
  [m]
  (let [p (java.util.Properties.)]
    (doseq [[k v] m]
      (.setProperty p (name k) (str v)))
    p))

(defn make-job-factory
  [scheduler]
  (reify JobFactory
    (newJob [this bundle quartz]
      (let [detail (.getJobDetail bundle)
            data-map (.getJobDataMap detail)
            class (.getJobClass detail)
            sym (symbol (get data-map "ns") (get data-map "name"))]
        (try
          (log/debugf "Producing instance of Job '%s', symbol=%s" (.getKey detail) sym)
          (let [var (resolve sym)
                ctor (.getDeclaredConstructor class (into-array [Object Object]))]
            (.newInstance ctor (into-array Object [@var scheduler])))
          (catch Exception e
            (throw (SchedulerException. (format "Problem resolving symbol '%s'" sym)))))))))

(defrecord Scheduler []
  component/Lifecycle
  (start [this]
    (.setJobFactory (:twarc/quartz this) (make-job-factory this))
    (.start (:twarc/quartz this))
    this)
  (stop [this]
    (.shutdown (:twarc/quartz this) true)
    (-> (SchedulerRepository/getInstance) (.remove (:twarc/name this)))
    this))

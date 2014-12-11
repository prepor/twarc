(ns twarc.core
  (:require [com.stuartsierra.component :as component]
            [plumbing.core :refer :all]
            [clojure.tools.logging :as log])
  (:import [org.quartz.impl StdSchedulerFactory]
           [org.quartz.spi JobFactory]
           [org.quartz Job JobBuilder SchedulerException StatefulJob JobDataMap TriggerBuilder
            SimpleScheduleBuilder CronScheduleBuilder]
           [java.util UUID]))

(defrecord TwarcJob [f context]
  Job
  (execute [_ execution-context]
    (let [detail (.getJobDetail execution-context)
          data-map (.getJobDataMap detail)]
      (apply f (assoc context ::execution-context execution-context)
             (get data-map "arguments")))))

(defrecord TwarcStatefullJob [f context]
  StatefulJob
  (execute [_ execution-context]
    (let [detail (.getJobDetail execution-context)
          data-map (.getJobDataMap detail)
          result (apply f (assoc context ::execution-context execution-context)
                        (get data-map "state")
                        (get data-map "arguments"))]
      (.put data-map "state" result))))

(defn map->properties
  [m]
  (let [p (java.util.Properties.)]
    (doseq [[k v] m]
      (.setProperty p (name k) (str v)))
    p))

(defn make-job
  [{:keys [ns name arguments identity group desc recovery durably state]
    :or {identity (-> (UUID/randomUUID) str)}
    :as opts}]
  (-> (JobBuilder/newJob (if (contains? opts :state) TwarcStatefullJob TwarcJob))
      (.usingJobData (JobDataMap. {"ns" ns
                                   "name" name
                                   "arguments" arguments
                                   "state" state}))
      (cond->
       group (.withIdentity identity group)
       (not group) (.withIdentity identity)
       desc (.withDescription desc)
       durably (.withDurably)
       recovery (.withRecovery))
      (.build)))

(defn prepare-simple
  [{:keys [repeat interval misfire-handling]}]
  (-> (SimpleScheduleBuilder/simpleSchedule)
      (cond->
       (= :inf repeat) (.repeatForever)
       (number? repeat) (.withRepeatCount repeat)
       interval (.withIntervalInMilliseconds interval)
       misfire-handling
       (as-> schedule
             (case misfire-handling
               :fire-now
               (.withMisfireHandlingInstructionFireNow schedule)
               :ignore-misfires
               (.withMisfireHandlingInstructionIgnoreMisfires schedule)
               :next-with-existing-count
               (.withMisfireHandlingInstructionNextWithExistingCount schedule)
               :next-with-remaining-count
               (.withMisfireHandlingInstructionNextWithRemainingCount schedule)
               :now-with-existing-count
               (.withMisfireHandlingInstructionNowWithExistingCount schedule)
               :now-with-remaining-count
               (.withMisfireHandlingInstructionNowWithRemainingCount schedule))))))

(defn prepare-cron
  [options]
  (-> (if (map? options)
        (-> (if (:nonvalidated? options)
              (CronScheduleBuilder/cronScheduleNonvalidatedExpression (:expression options))
              (CronScheduleBuilder/cronSchedule (:expression options)))
            (as-> schedule
                  (case (:misfire-handling options)
                    :do-nothing
                    (.withMisfireHandlingInstructionDoNothing schedule)
                    :fire-and-process
                    (.withMisfireHandlingInstructionFireAndProceed schedule)
                    :ignore-misfires
                    (.withMisfireHandlingInstructionIgnoreMisfires schedule)))
            (?> (:time-zone options)
                (.inTimeZone (:time-zone options))))
        (CronScheduleBuilder/cronSchedule options))))

(defn make-trigger
  [{:keys [start-at start-now end-at for-job modified-by-calendars identity group description
           job-data priority simple cron]
    :or {identity (-> (UUID/randomUUID) str)}}]
  (-> (TriggerBuilder/newTrigger)
      (cond->
       group (.withIdentity identity group)
       (not group) (.withIdentity identity)
       start-at (.startAt start-at)
       end-at (.endAt end-at)
       start-now (.startNow)
       modified-by-calendars (as-> trigger
                                   (doseq [c modified-by-calendars]
                                     (.modifiedByCalendar trigger c)))
       priority (.withPriority priority)
       job-data (.usingJobData (JobDataMap. job-data))
       simple (.withSchedule (prepare-simple simple))
       cron (.withSchedule (prepare-cron cron)))
      (.build)))

(defn make-job-factory
  [context]
  (reify JobFactory
    (newJob [this bundle scheduler]
      (let [detail (.getJobDetail bundle)
            data-map (.getJobDataMap detail)
            class (.getJobClass detail)
            sym (symbol (get data-map "ns") (get data-map "name"))]
        (try
          (log/debugf "Producing instance of Job '%s', symbol=%s" (.getKey detail) sym)
          (let [var (resolve sym)
                ctor (.getDeclaredConstructor class (into-array [Object Object]))]
            (.newInstance ctor (into-array Object [@var context])))
          (catch Exception e
            (throw (SchedulerException. (format "Problem resolving symbol '%s'" sym)))))))))

(defn job-from-var
  [var options]
  (make-job (assoc options
              :ns (-> var meta :ns ns-name str)
              :name (-> var meta :name str))))

(defn schedule-job
  [scheduler var arguments & {:keys [job trigger]}]
  (let [job (job-from-var var (assoc job :arguments arguments))
        trigger (make-trigger trigger)]
    (.scheduleJob (:scheduler scheduler) job trigger)))

(defmacro defjob
  [n binding & body]
  (let [generated-f (symbol (str n "*"))]
    `(do (defn ~generated-f
           ~binding
           ~@body)
         (defn ~n
           [scheduler# args# & params#]
           (apply schedule-job scheduler# (var ~generated-f) args# params#)))))

(defrecord Scheduler [scheduler]
  component/Lifecycle
  (start [this]
    (.start scheduler)
    this)
  (stop [this]
    (.shutdown scheduler true)
    this))

(defn start
  [scheduler]
  (component/start scheduler))

(defn stop
  [scheduler]
  (component/stop scheduler))

(defn make-scheduler
  ([] (make-scheduler {}))
  ([context] (make-scheduler context {}))
  ([context properties] (make-scheduler context properties {}))
  ([context properties options]
     (let [factory (StdSchedulerFactory.
                    (map->properties (map-keys #(str "org.quartz." (name %)) properties)))
           scheduler (.getScheduler factory)]
       (when-let [cals (:calendars options)]
         (doseq [[name cal replace update-triggers] cals]
           (.addCalendar scheduler name cal (boolean replace) (boolean update-triggers))))
       (.setJobFactory scheduler (make-job-factory context))
       (map->Scheduler {:scheduler scheduler}))))

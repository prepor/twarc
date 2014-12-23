(ns twarc.core
  (:require [com.stuartsierra.component :as component]
            [plumbing.core :refer :all]
            [clojure.tools.logging :as log]
            [clojure.core.async :as a])
  (:import [org.quartz.impl StdSchedulerFactory SchedulerRepository]
           [org.quartz.impl.matchers OrMatcher NotMatcher NameMatcher KeyMatcher
            AndMatcher StringMatcher GroupMatcher
            EverythingMatcher]
           [org.quartz.utils Key]
           [org.quartz.spi JobFactory]
           [org.quartz Job JobBuilder SchedulerException StatefulJob JobDataMap TriggerBuilder
            SimpleScheduleBuilder CronScheduleBuilder JobListener JobKey TriggerKey]
           [java.util UUID]
           [twarc TwarcJob TwarcStatefullJob]))

(defn uuid [] (-> (UUID/randomUUID) str))

(defn map->properties
  [m]
  (let [p (java.util.Properties.)]
    (doseq [[k v] m]
      (.setProperty p (name k) (str v)))
    p))

(defn make-job
  [{:keys [ns name arguments identity group desc recovery durably state]
    :or {identity (uuid)}
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
    :or {identity (uuid)}}]
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

(defn job-from-var
  [var options]
  (make-job (assoc options
              :ns (-> var meta :ns ns-name str)
              :name (-> var meta :name str))))

(defn schedule-job
  [scheduler var arguments & {:keys [job trigger replace] :or {replace false}}]
  (let [job (job-from-var var (assoc job :arguments arguments))
        trigger (make-trigger trigger)]
    (.scheduleJob (::quartz scheduler) job #{trigger} replace)))

(defmacro defjob
  [n binding & body]
  (let [generated-f (symbol (str n "*"))]
    `(do (defn ~generated-f
           ~binding
           ~@body)
         (defn ~n
           [scheduler# args# & params#]
           (apply schedule-job scheduler# (var ~generated-f) args# params#)))))

(defrecord Scheduler []
  component/Lifecycle
  (start [this]
    (.setJobFactory (::quartz this) (make-job-factory this))
    (.start (::quartz this))
    (assoc this ::listeners (atom {})))
  (stop [this]
    (.shutdown (::quartz this) true)
    (-> (SchedulerRepository/getInstance) (.remove (::name this)))
    this))

(defn start
  [scheduler]
  (component/start scheduler))

(defn stop
  [scheduler]
  (component/stop scheduler))

(defn make-scheduler
  ([] (make-scheduler {}))
  ([properties] (make-scheduler properties {}))
  ([properties options]
     (let [n (get options :name (uuid))
           factory (StdSchedulerFactory.
                    (->> (assoc properties
                           :scheduler.instanceName n)
                         (map-keys #(str "org.quartz." (name %)))
                         (map->properties)))
           quartz (.getScheduler factory)]
       (when-let [cals (:calendars options)]
         (doseq [[name cal replace update-triggers] cals]
           (.addCalendar quartz name cal (boolean replace) (boolean update-triggers))))
       (map->Scheduler {::quartz quartz ::name n}))))

;; TODO should be extendable
(defn matcher
  [spec & [scope]]
  "Constructor of Quartz's matchers. Can be used in Liteners, for example.

  Supported matchers:

  {:key [\"some group\" \"some identity\"]}

  {:name [:contains \"foo\"]}

  {:group [:contains \"foo\"]}

  {:everything true}

  {:and [matcher1 matcher2 ... matcherN]}

  {:or [matcher1 matcher2 ... matcherN]}

  {:not matcher}

   :contains in :name and :group matchers also can be :equals, :ends-with and :starts-with "
  (cond
   (:and spec) (reduce #(AndMatcher/and (matcher %1) (matcher %2)) (:and spec))
   (:or spec) (reduce #(OrMatcher/or (matcher %1) (matcher %2)) (:or spec))
   (:not spec) (NotMatcher/not (matcher (:not spec)))
   (:group spec) (let [s (second (:group spec))]
                   (case (first (:group spec))
                     :contains (GroupMatcher/groupContains s)
                     :ends-with (GroupMatcher/groupEndsWith s)
                     :equals (GroupMatcher/groupEquals s)
                     :starts-with (GroupMatcher/groupStartsWith s)))
   (:name spec) (let [s (second (:name spec))]
                  (case (first (:name spec))
                    :contains (NameMatcher/nameContains s)
                    :ends-with (NameMatcher/nameEndsWith s)
                    :equals (NameMatcher/nameEquals s)
                    :starts-with (NameMatcher/nameStartsWith s)))
   (:key spec) (let [group (first (:key spec))
                     n (second (:key spec))]
                 (KeyMatcher/keyEquals (case scope
                                         :job (JobKey. n group)
                                         :trigger (TriggerKey. n group)
                                         (Key. n group))))
   (= :everything spec) (EverythingMatcher/allJobs)))

(defn add-listener
  "Registers Quartz listener and return core.async channel with events from
  listener. Warning: events are written via >!! so, you should either read from this
  channel or set non-blocking buffer.

  Possible listener-types: (JobListener see
  http://www.quartz-scheduler.org/api/2.2.1/org/quartz/JobListener.html)
  :execution-vetoed, :to-be-executed, :was-executed

  For matcher-spec syntax see matcher fn"
  ([scheduler matcher-spec listener-type]
     (add-listener scheduler matcher-spec listener-type nil))
  ([scheduler matcher-spec listener-type buf-or-n]
     (let [ch (a/chan buf-or-n)
           n (uuid)
           listener-scope (case listener-type
                            (:execution-vetoed :to-be-executed :was-executed)
                            :job)
           ;; TODO implement Trigger and Scheduler listeners
           listener (case listener-scope
                      :job
                      (reify JobListener
                        (getName [_] n)
                        (jobExecutionVetoed [_ context]
                          (when (= :execution-vetoed listener-type)
                            (a/>!! ch context)))
                        (jobToBeExecuted [_ context]
                          (when (= :to-be-executed listener-type)
                            (a/>!! ch context)))
                        (jobWasExecuted [_ context exc]
                          (when (= :was-executed listener-type)
                            (a/>!! ch context)))))
           built-matcher (matcher matcher-spec listener-scope)]
       (-> (::quartz scheduler)
           (.getListenerManager)
           (.addJobListener listener built-matcher))
       (swap! (::listeners scheduler)
              assoc ch {:listener-name n :listener-type listener-type})
       ch)))

(defn remove-listener
  [scheduler listener]
  (let [m (-> (::quartz scheduler)
              (.getListenerManager))
        meta (get-in scheduler ::listeners listener)]
    (case (:listener-type meta)
      (:execution-vetoed :to-be-executed :was-executed)
      (.removeJobListener m (:listener-name meta)))
    (swap! (::listeners scheduler) dissoc listener)))

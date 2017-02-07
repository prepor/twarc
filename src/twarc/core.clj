(ns twarc.core
  (:require [com.stuartsierra.component :as component]
            [plumbing.core :refer :all]
            [clojure.tools.logging :as log]
            [clojure.core.async :as a]
            [twarc.impl.core :as impl])
  (:import [org.quartz.impl StdSchedulerFactory]
           [org.quartz.impl.matchers OrMatcher NotMatcher NameMatcher KeyMatcher
            AndMatcher StringMatcher GroupMatcher
            EverythingMatcher]
           [org.quartz.utils Key]
           [org.quartz JobBuilder JobDataMap TriggerBuilder
            SimpleScheduleBuilder CronScheduleBuilder JobListener JobKey TriggerKey]
           [twarc TwarcJob TwarcStatefullJob]))


(defn make-job
  "If state is passed, job will be stateful, i.e. state will be passed to the job
  on each invocation. Job execution will result in new state."
  [{:keys [ns name arguments identity group desc recovery durably state]
    :or {identity (impl/uuid)}
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
  "Most of the options are obvious and are simply passed to TriggerBuilder. See
  http://www.quartz-scheduler.org/api/2.2.1/org/quartz/TriggerBuilder.html for details.

  The most interesting part of a trigger is defining a scheduler. It may be either
  simple or cron. For example:

  (make-trigger {:simple {:repeat :inf :interval 10000}})

  (make-trigger {:cron \"0 0 3 * 2 ?\"})

  Cron can be a string or a map:

  (make-trigger {:cron {:expression \"0 0 3 * 2 ?\" :misfire-handling :ignore-misfires
                        :time-zone (TimeZone/getTimeZone \"America/Los_Angeles\")}})"
  [{:keys [start-at start-now end-at for-job modified-by-calendars identity group description
           job-data priority simple cron]
    :or {identity (impl/uuid)}}]
  (-> (TriggerBuilder/newTrigger)
      (cond->
       group (.withIdentity identity group)
       (not group) (.withIdentity identity)
       for-job (.forJob for-job)
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

(defn job-key
  ([group name] (JobKey. name group))
  ([name] (JobKey. name)))

(defn trigger-key
  ([group name] (TriggerKey. name group))
  ([name] (TriggerKey. name)))

(defn job-from-var
  [var options]
  (make-job (assoc options
              :ns (-> var meta :ns ns-name str)
              :name (-> var meta :name str))))

(defn schedule-job
  "Adds job and trigger to scheduler. Job will be executed as: (apply @var scheduler
  arguments), and for stateful jobs as: (apply @var scheduler state arguments).
  For job and trigger parameters see make-job and make-trigger.

  Set replace to true to update existing job and trigger."
  [scheduler var arguments & {:keys [job trigger replace] :or {replace false}}]
  (let [job (job-from-var var (assoc job :arguments arguments))
        trigger (make-trigger trigger)]
    (.scheduleJob (:twarc/quartz scheduler) job #{trigger} replace)
    job))

(defn delete-job
  [scheduler & key]
  (.deleteJob (:twarc/quartz scheduler) (apply job-key key)))

(defn check-job-exists
  [scheduler & key]
  (.checkExists (:twarc/quartz scheduler) (apply job-key key)))

(defn check-trigger-exists
  [scheduler & key]
  (.checkExists (:twarc/quartz scheduler) (apply trigger-key key)))

(defmacro defjob
  [n binding & body]
  (let [generated-f (symbol (str n "*"))]
    `(do (defn ~generated-f
           ~binding
           ~@body)
         (defn ~n
           [scheduler# args# & params#]
           (apply schedule-job scheduler# (var ~generated-f) args# params#)))))

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
     (let [n (get options :name (impl/uuid))
           factory (StdSchedulerFactory.
                    (->> (assoc properties
                           :scheduler.instanceName n)
                         (map-keys #(str "org.quartz." (name %)))
                         (impl/map->properties)))
           quartz (.getScheduler factory)]
       (when-let [cals (:calendars options)]
         (doseq [[name cal replace update-triggers] cals]
           (.addCalendar quartz name cal (boolean replace) (boolean update-triggers))))
       (impl/map->Scheduler {:twarc/quartz quartz
                             :twarc/name n
                             :twarc/listeners (atom {})}))))

;; TODO should be extendable
(defn matcher
  [spec & [scope]]
  "Constructor of Quartz matchers. Can be used in Liteners, for example.

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
   (:key spec) (KeyMatcher/keyEquals (case scope
                                       :job (apply job-key (:key spec))
                                       :trigger (apply trigger-key (:key spec))
                                       (Key. (second (:key spec)) (first (:key spec)))))
   (= :everything spec) (EverythingMatcher/allJobs)))

(defn add-listener
  "Registers a Quartz listener and returns a core.async channel with events from the
  listener. Warning: events are written via >!! so you should either read from this
  channel or set a non-blocking buffer.

  Possible listener-types: (JobListener see
  http://www.quartz-scheduler.org/api/2.2.1/org/quartz/JobListener.html)
  :execution-vetoed, :to-be-executed, :was-executed

  For matcher-spec syntax, see matcher"
  ([scheduler matcher-spec listener-type]
     (add-listener scheduler matcher-spec listener-type nil))
  ([scheduler matcher-spec listener-type buf-or-n]
     (let [ch (a/chan buf-or-n)
           n (impl/uuid)
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
       (-> (:twarc/quartz scheduler)
           (.getListenerManager)
           (.addJobListener listener built-matcher))
       (swap! (:twarc/listeners scheduler)
              assoc ch {:listener-name n :listener-type listener-type})
       ch)))

(defn remove-listener
  [scheduler listener]
  (let [m (-> (:twarc/quartz scheduler)
              (.getListenerManager))
        meta (-> scheduler :twarc/listeners deref (get listener))]
    (case (:listener-type meta)
      (:execution-vetoed :to-be-executed :was-executed)
      (.removeJobListener m (:listener-name meta)))
    (swap! (:twarc/listeners scheduler) dissoc listener)))

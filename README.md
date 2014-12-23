# twarc

[![Travis status](https://secure.travis-ci.org/prepor/twarc.png)](http://travis-ci.org/prepor/twarc)

A Simple Clojure wrapper around [Quartz Scheduler](http://www.quartz-scheduler.org/).

## Artifacts

[![Clojars Project](http://clojars.org/twarc/latest-version.svg)](http://clojars.org/twarc)

## Why?

There are a few Clojure libraries for working with Quartz, but everyone has fatal flaw (at least one):

* [Quartzite](http://clojurequartz.info/)
  * Global state, thread bindings
  * You can't explicity pass some state (db connection, for example) to jobs
  * Complex API
* [Immutant Scheduling](https://github.com/immutant/immutant/tree/thedeuce/scheduling)
  * You can't explicity pass some state (db connection, for example) to jobs
  * You can't use it with persistent JobStores and in Clustered environments
  * Complex dependencies


The main disadventage of these libs is that you can't use it in right way, ie without any global state but with strong dependency management (see [stuartsierra/component](https://github.com/stuartsierra/component))

## Features

* No global state
* You can pass any instance-aware context to jobs
* Datastructure-centric API (see examples)
* Jobs are usual vars with function (ie defn)
* Statefull jobs
* No magic
* stuartsierra.component support out of box
* Quartz's Listeners support via core.async channels

## Usage

Basic config (see [Quartz Configuration Reference](http://quartz-scheduler.org/documentation/quartz-2.2.x/configuration/)):
```clojure

(require '[twarc.core :as twarc])

(def props {:threadPool.class "org.quartz.simpl.SimpleThreadPool"
            :threadPool.threadCount 1
            :plugin.triggHistory.class "org.quartz.plugins.history.LoggingTriggerHistoryPlugin"
            :plugin.jobHistory.class "org.quartz.plugins.history.LoggingJobHistoryPlugin"})

;; Scheduler supports component/Lifecycle protocol and clojure.lang.Associative (its
;; clojure record), so you can simply drop it to your system map. Or use some other DI
;; system

(def sched (-> (twarc/make-scheduler props) (twarc/start)))

```

`defjob` macro defines two functions, in this case `test-job` and `test-job*`. `test-job*` is actual job with body provided by you and executes in Quartz's thread pool. Generated `test-job` is helper function, which can be used for schedule jobs.

Job function accepts scheduler instance as first argument, and the rest of arguments are passed on job scheduling.

```clojure
(twarc/defjob test-job
  [scheduler name message]
  (prn "Message for!" name message))
```

Let's run it!
```clojure
;; If you use cider, note that Quartz threads know nothing about repl's stdout. So watch
;; messages in nrepl-server buffer

(test-job sched ["Andrew" "Hello world"])
```

That's all. First argument is scheduler instance, second is vector of arguments and optional tail arguments are options for `schedule-job` function (job and trigger params actually, see Quartz documentation for details).

You can schedule execution of any defn without helper:

```clojure
(defn test-job2
  [scheduler name message]
  (prn "Message  from!" name message))

(twarc/schedule-job sched #'test-job2 ["Petr" "Hi world!"])
```

Define simple or cron trigger via map:

```clojure
(test-job sched ["Andrew" "Hello world"] :trigger {:simple {:repeat 5 :interval 1000}})

(test-job sched ["Andrew" "Hello world"]
          :job {:identity "eternal job"}
          :trigger {:cron "*/10 * * * * ?"})

(twarc/delete-job sched "eternal job")
```




### Persistent JobStore

You can persist your jobs and triggers in JDBC-store.

First of all, you need create tables, see these scripts - http://svn.terracotta.org/svn/quartz/tags/quartz-2.2.1/distribution/src/main/assembly/root/docs/dbTables/

Secondly, configure Quartz for your store. You also should use well defined name for your scheduler:

```clojure
(def persistent-props
  (assoc props
    :jobStore.class "org.quartz.impl.jdbcjobstore.JobStoreTX"
    :jobStore.driverDelegateClass "org.quartz.impl.jdbcjobstore.PostgreSQLDelegate"
    :jobStore.tablePrefix "QRTZ_"
    :jobStore.dataSource "db"
    :dataSource.db.driver "org.postgresql.Driver"
    :dataSource.db.URL "jdbc:postgresql://localhost:5432/db_name"
    :dataSource.db.user "user"
    :dataSource.db.password "pass"))

(def persistent-sched (-> (twarc/make-scheduler persistent-props {:name "main-sched"})
                          (twarc/start)))
```

In this example we also can see how configure job and trigger. With `:state` param job becomes Statefull job, job function accepts state as second argument and should return updated state.

```clojure
(twarc/defjob test-statefull-job
  [scheduler state i]
  (prn "State!" state)
  (update-in state [:counter] + i))

(test-statefull-job persistent-sched [4]
                    :job {:state {:counter 1}}
                    :trigger {:simple {:repeat :inf :interval 1000}})

```

And now stop and start new scheduler without scheduling task. Our previously scheduled task will continue executing

```clojure
(twarc/stop persistent-sched)
(def persistent-sched2 (-> (twarc/make-scheduler persistent-props {:name "main-sched"})
                           (twarc/start)))
```

### Listeners

You can define listeners of some events with core.async channels.

```clojure
(require '[clojure.core.async :as a])
(def executed (twarc/add-listener persistent-sched2 {:everything true} :was-executed))

(loop []
  (prn "--EXECUTED!" (->  (a/<!! executed) .getJobDetail .getJobDataMap (get "state")))
  (recur))
```

## License

Copyright © 2014 Andrew Rudenko

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.

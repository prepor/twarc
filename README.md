# twarc

A Simple Clojure wrapper around [Quartz Scheduler](http://www.quartz-scheduler.org/).

## Artifacts

```clojure
[twarc "0.1.0"]
```

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
* Jobs are usual vars with function (defn)
* Statefull jobs
* No magic
* stuartsierra.component support out of box
* Less then 200 lines of code

## Usage

```clojure
(require '[twarc.core :as twarc])
;; the little helper for running. You can use your own infrastructure, of course
(defmacro with-sched
  [binding props & body]
  `(let [~binding (-> (twarc/make-scheduler {} ~props)
                      (twarc/start))]
     (try
       ~@body
       (finally
       (twarc/stop ~binding)))))
```

`defjob` macro defines two functions, in this case `test-job` and `test-job*`. `test-job*` is actual job with body provided by you and executes in Quartz's thread pool. Generated `test-job` is helper function, which can be used for schedule jobs.

Job function accepts context passed to `make-scheduler` as first argument, and the rest of arguments are passed on job scheduling.

```clojure
(twarc/defjob test-job
  [ctx name message]
  (prn "Message for!" name message))
```

Basic config (see [Quartz Configuration Reference](http://quartz-scheduler.org/documentation/quartz-2.2.x/configuration/)):
```clojure
(def props {:threadPool.class "org.quartz.simpl.SimpleThreadPool"
            :threadPool.threadCount 1
            :plugin.triggHistory.class "org.quartz.plugins.history.LoggingTriggerHistoryPlugin"
            :plugin.jobHistory.class "org.quartz.plugins.history.LoggingJobHistoryPlugin"})
```

Let's run it!
```clojure
(with-sched sched props
  (test-job sched ["Andrew" "Hello world"])
  (Thread/sleep 1000))
```

That's all. First argument is scheduler instance, second is vector of arguments and optional tail arguments are options for `schedule-job` function (job and trigger params actually, see Quartz documentation for details).

### Persistent JobStore

You can persist your jobs and triggers in JDBC-store.

First of all, you need create tables, see these scripts - http://svn.terracotta.org/svn/quartz/tags/quartz-2.2.1/distribution/src/main/assembly/root/docs/dbTables/

Secondly, configure Quartz for your store:

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
```

In this example we also can see how configure job and trigger. With `:state` param job becomes Statefull job, job function accepts state as second argument and should return updated state.

```clojure
(twarc/defjob test-statefull-job
  [ctx state i]
  (prn "State!" state)
  (update-in state [:counter] + i))

(with-sched sched persistent-props
  (test-statefull-job sched [4]
                      :job {:state {:counter 1}}
                      :trigger {:simple {:repeat 10 :interval 1000}})

  (Thread/sleep 4000))
```

And now start new scheduler again without scheduling task. Our previously scheduled task will continue executing

```clojure
(with-sched sched persistent-props
  (Thread/sleep 4000))
```


## License

Copyright © 2014 Andrew Rudenko

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.

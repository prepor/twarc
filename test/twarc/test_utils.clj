(ns twarc.test-utils
  (:require [twarc.core :as twarc]
            [clojure.core.async :as async])
  (:import [org.quartz JobKey]))


(defn async-res
  ([ch] (async-res ch 5))
  ([ch seconds]
     (async/alt!!
       (async/timeout (* seconds 5000)) (throw (Exception. "Timeout"))
       ch ([v ch] v))))

(def ^:dynamic *scheduler*)

(defn with-scheduler
  [f]
  (binding [*scheduler* (-> (twarc/make-scheduler {} {:threadPool.threadCount 1})
                            (twarc/start))]
    (try
      (f)
      (finally
        (twarc/stop *scheduler*)))))

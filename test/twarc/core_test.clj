(ns twarc.core-test
  (:require [clojure.test :refer :all]
            [twarc.core :as twarc]
            [twarc.test-utils :refer [async-res with-scheduler *scheduler*]]))

(twarc/defjob simple-job
  [scheduler first-name last-name]
  nil)

(twarc/defjob statefull-job
  [scheduler state]
  (str state "(.)"))

(use-fixtures :each with-scheduler)

(deftest defjob-test
  (testing "Simple job"
    (let [listener (twarc/add-listener *scheduler*
                                       {:key ["test-suite" "task-1"]} :to-be-executed)]
      (simple-job *scheduler* ["Petr" "Yanovich"]
                  :job {:identity "task-1"
                        :group "test-suite"})
      (let [res (async-res listener)
            data-map (-> (.getJobDetail res) (.getJobDataMap))]
        (is (= ["Petr" "Yanovich"] (get data-map "arguments")))
        (is (= nil (get data-map "state"))))))

  (testing "Statefull job"
    (let [listener (twarc/add-listener *scheduler*
                                       {:key ["test-suite" "task-2"]} :was-executed)]
      (statefull-job *scheduler* []
                     :job {:identity "task-2"
                           :group "test-suite"
                           :state "(.)(.)"})

      (let [res (async-res listener)
            data-map (-> (.getJobDetail res) (.getJobDataMap))]
        (is (= "(.)(.)(.)" (get data-map "state"))))))

  (testing "named jobs"
    (is (false? (twarc/check-job-exists *scheduler* "task-3")))
    (simple-job *scheduler* ["Petr" "Yanovich"]
                :job {:identity "task-3"}
                :trigger {:cron "*/10 * * * * ?"})
    (is (true? (twarc/check-job-exists *scheduler* "task-3")))
    (twarc/delete-job *scheduler* "task-3")
    (is (false? (twarc/check-job-exists *scheduler* "task-3")))))

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

(twarc/defjob check-listeners-job
  [scheduler]
  (.setResult
   (:twarc/execution-context scheduler)
   (set (keys scheduler))))

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
            data-map (.getMergedJobDataMap res)]
        (is (= "(.)(.)(.)" (get data-map "state"))))))

  (testing "named jobs"
    (is (false? (twarc/check-job-exists *scheduler* "task-3")))
    (simple-job *scheduler* ["Petr" "Yanovich"]
                :job {:identity "task-3"}
                :trigger {:cron "*/10 * * * * ?"})
    (is (true? (twarc/check-job-exists *scheduler* "task-3")))
    (twarc/delete-job *scheduler* "task-3")
    (is (false? (twarc/check-job-exists *scheduler* "task-3"))))

  (testing "trigger can override job parameters"
    (let [listener (twarc/add-listener *scheduler*
                                       {:key ["test-suite" "task-4"]} :to-be-executed)]
      (simple-job *scheduler* []
                  :job {:identity "task-4"
                        :group "test-suite"}
                  :trigger {:job-data {"arguments" ["Petr" "Yanovich"]}
                            :cron "*/10 * * * * ?"})
      (let [res (async-res listener)
            data-map (.getMergedJobDataMap res)]
        (is (= ["Petr" "Yanovich"] (get data-map "arguments")))
        (is (= nil (get data-map "state")))))))

(deftest remove-listener-test
  (testing "Remove a listener"
    (let [listener (twarc/add-listener *scheduler*
                                       {:everything true} :to-be-executed)]
      (twarc/remove-listener *scheduler* listener)
      (is (empty? (-> *scheduler* :twarc/listeners deref))))))

(deftest listeners-available
  (testing "Listeners are available inside job"

    (let [listener (twarc/add-listener *scheduler*
                                       {:key ["test-suite" "task-listener"]} :was-executed)]
      (check-listeners-job *scheduler* []
                           :job {:identity "task-listener" :group "test-suite"})
      (let [res        (async-res listener)
            job-result (.getResult res)]
        (is (:twarc/listeners job-result))))))

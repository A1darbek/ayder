(ns jepsen.ayder.broker-model-test
  (:require [clojure.test :refer [deftest is]]
            [jepsen.ayder.broker-model :as broker-model]))

(deftest append-invoke-is-ignored
  (let [m0 (broker-model/model)
        m1 (.step m0 {:type :invoke :f :append :value "m-0"})]
    (is (= m0 m1))))

(deftest read-at-invoke-is-ignored
  (let [m0 (broker-model/model)
        m1 (.step m0 {:type :invoke :f :read-at :offset 0 :value nil})]
    (is (= m0 m1))))

(deftest append-ok-advances-model
  (let [m0 (broker-model/model)
        m1 (.step m0 {:type :ok :f :append :value {:offset 0 :msg "m-0"}})
        m2 (.step m1 {:type :ok :f :read-at :offset 0 :value {:offset 0 :value "m-0"}})]
    (is (= 1 (:next-offset m1)))
    (is (= "m-0" (get (:values m1) 0)))
    (is (= m1 m2))))

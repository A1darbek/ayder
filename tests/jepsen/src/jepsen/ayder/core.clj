(ns jepsen.ayder.core
  (:require [jepsen [checker :as checker]
                    [generator :as gen]
                    [tests :as tests]]
            [jepsen.checker.timeline :as timeline]
            [jepsen.ayder.client :as kv-client]
            [jepsen.ayder.broker-client :as broker-client]
            [jepsen.ayder.broker-model :as broker-model]
            [jepsen.ayder.nemesis :as ayder-nemesis]
            [jepsen.ayder.local-remote :as local-remote]
            [knossos.model :as model]))

(defn- nemesis-generator [interval-sec]
  (cycle [{:f :partition}
          (gen/sleep interval-sec)
          {:f :heal}
          (gen/sleep interval-sec)
          {:f :kill}
          (gen/sleep interval-sec)
          {:f :restart}
          (gen/sleep interval-sec)]))

(defn- op-stagger-sec [{:keys [client-stagger-ms]}]
  (let [ms (long (or client-stagger-ms 10))]
    (/ (double (max 1 ms)) 1000.0)))

(defn- workload-body
  [{:keys [time-limit-sec nemesis-enabled nemesis-interval-sec nemesis-startup-sec]} client-ops final-ops]
  (let [startup-sec (long (max 0 (or nemesis-startup-sec 10)))
        mixed-sec (if nemesis-enabled
                    (max 1 (- (long time-limit-sec) startup-sec))
                    (long time-limit-sec))
        client-body (gen/clients client-ops)
        mixed-body (if nemesis-enabled
                     (gen/mix [client-body
                               (gen/nemesis (nemesis-generator nemesis-interval-sec))])
                     client-body)
        warmup-body (when (and nemesis-enabled (pos? startup-sec))
                      (->> client-body
                           (gen/time-limit startup-sec)))
        steady-body (->> mixed-body
                         (gen/time-limit mixed-sec))]
    (apply gen/phases
           (concat
             (when warmup-body [warmup-body])
             [steady-body
              (gen/log "Healing nemesis")
              (gen/nemesis {:f :heal})
              (->> final-ops
                   (gen/clients)
                   (gen/time-limit 5))]))))

(defn- kv-write-generator []
  (->> (range)
       (map (fn [v] {:f :write :value (str v)}))))

(defn- kv-read-generator []
  (repeat {:f :read}))

(defn- kv-workload [opts]
  (let [client-ops (->> (gen/mix [(kv-write-generator)
                                  (kv-read-generator)])
                        (gen/stagger (op-stagger-sec opts)))]
    (workload-body opts client-ops (repeat {:f :read}))))

(defn- broker-append-generator []
  (->> (range)
       (map (fn [v] {:f :append :value (str "m-" v)}))))

(defn- broker-read-at-generator []
  (->> (range)
       (map (fn [off] {:f :read-at :offset off}))))

(defn- broker-workload [opts]
  (let [client-ops (->> (gen/mix [(broker-append-generator)
                                  (broker-read-at-generator)])
                        (gen/stagger (op-stagger-sec opts)))]
    (workload-body opts client-ops (broker-read-at-generator))))

(defn- client-events
  "Knossos expects integer process ids; drop nemesis/log process events."
  [history]
  (filter #(integer? (:process %)) history))

(defn- completed-client-history
  "Keep client ops which have both invoke and completion events, including
   :info completions (indeterminate outcomes), while preserving event order."
  [history]
  (let [ops (vec (client-events history))
        keep-indices
        (loop [i 0
               pending {}
               keep #{}]
          (if (>= i (count ops))
            keep
            (let [op (nth ops i)
                  p (:process op)
                  t (:type op)]
              (case t
                :invoke (recur (inc i) (assoc pending p op) keep)
                :ok (if-let [inv (get pending p)]
                      (recur (inc i) (dissoc pending p)
                             (conj keep (:index inv) (:index op)))
                      (recur (inc i) pending keep))
                :fail (if-let [inv (get pending p)]
                        (recur (inc i) (dissoc pending p)
                               (conj keep (:index inv) (:index op)))
                        (recur (inc i) pending keep))
                :info (if-let [inv (get pending p)]
                        (recur (inc i) (dissoc pending p)
                               (conj keep (:index inv) (:index op)))
                        (recur (inc i) pending keep))
                (recur (inc i) pending keep)))))]
    (->> ops
         (filter (fn [op] (contains? keep-indices (:index op))))
         vec)))

(defn- history-for-checker [history history-mode]
  (case history-mode
    :completed-only (completed-client-history history)
    :all (client-events history)
    (client-events history)))

(defn- linearizable-clients-checker [m history-mode]
  (let [base (checker/linearizable {:model m})]
    (reify checker/Checker
      (check [_ test history opts]
        (checker/check base test (history-for-checker history history-mode) opts)))))

(defn- min-ok-checker [min-ok]
  (reify checker/Checker
    (check [_ _ history _]
      (let [ok-count (count (filter #(and (integer? (:process %))
                                          (= :ok (:type %)))
                                   history))]
        {:valid? (>= ok-count min-ok)
         :ok-count ok-count
         :min-ok min-ok}))))

(defn kv-test [opts]
  (let [nodes (:nodes opts)
        history-mode (:linearizable-history-mode opts)
        name (str "ayder-kv-register-" (if (:nemesis-enabled opts) "nemesis" "steady"))]
    (merge tests/noop-test
           {:name      name
            :nodes     nodes
            :remote    (local-remote/remote)
            :client    (kv-client/client opts)
            :nemesis   (ayder-nemesis/shell-nemesis opts)
            :generator (kv-workload opts)
            :checker   (checker/compose
                         {:timeline (timeline/html)
                          :linearizable (linearizable-clients-checker (model/register) history-mode)
                          :min-ok (min-ok-checker 1)
                          :stats (checker/stats)})})))

(defn broker-test [opts]
  (let [nodes (:nodes opts)
        history-mode (:linearizable-history-mode opts)
        name (str "ayder-broker-log-" (if (:nemesis-enabled opts) "nemesis" "steady"))]
    (merge tests/noop-test
           {:name      name
            :nodes     nodes
            :remote    (local-remote/remote)
            :client    (broker-client/client opts)
            :nemesis   (ayder-nemesis/shell-nemesis opts)
            :generator (broker-workload opts)
            :checker   (checker/compose
                         {:timeline (timeline/html)
                          :linearizable (linearizable-clients-checker (broker-model/model) history-mode)
                          :min-ok (min-ok-checker 1)
                          :stats (checker/stats)})})))

;; Backward compatibility for callers expecting ayder-test as KV workload.
(defn ayder-test [opts]
  (kv-test opts))

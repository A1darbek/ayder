(ns jepsen.ayder.runner
  (:gen-class)
  (:require [cheshire.core :as json]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.tools.cli :refer [parse-opts]]
            [jepsen.core :as jepsen]
            [jepsen.ayder.core :as core]))

(def cli-options
  [["-n" "--nodes URLS" "Comma-separated node URLs"
    :default "http://127.0.0.1:7001,http://127.0.0.1:8001,http://127.0.0.1:9001,http://127.0.0.1:10001,http://127.0.0.1:11001"]
   ["-t" "--token TOKEN" "Bearer token" :default "dev"]
   ["-w" "--workload TYPE" "Workload: broker-log | kv-register"
    :default "broker-log"]

   ["-N" "--namespace NS" "KV namespace" :default "jepsen"]
   ["-K" "--key KEY" "KV key" :default "reg"]

   ["-T" "--topic TOPIC" "Broker topic name" :default "jepsen_log"]
   ["-G" "--group GROUP" "Broker consumer group" :default "jepsen_g"]
   ["-P" "--partition PARTITION" "Broker partition"
    :default 0
    :parse-fn #(Integer/parseInt %)]
   ["-Q" "--topic-partitions N" "Create topic with N partitions in setup"
    :default 1
    :parse-fn #(Integer/parseInt %)]
   ["-D" "--direct-url URL" "Force all client ops to this URL (bypass per-node endpoint)"
    :default ""]

   ["-S" "--client-stagger-ms MS" "Delay between client ops (ms); higher lowers checker pressure"
    :default 10
    :parse-fn #(Integer/parseInt %)]
   ["-Y" "--linearizable-history-mode MODE" "Linearizability history mode: all | completed-only"
    :default "all"]

   ["-L" "--time-limit-sec SECONDS" "Test runtime"
    :default 120
    :parse-fn #(Integer/parseInt %)]
   ["-E" "--nemesis-enabled BOOL" "Enable nemesis"
    :default "true"]
   ["-I" "--nemesis-interval-sec SECONDS" "Seconds between nemesis actions"
    :default 6
    :parse-fn #(Integer/parseInt %)]
   ["-Z" "--nemesis-startup-sec SECONDS" "Warmup seconds before first nemesis action"
    :default 10
    :parse-fn #(Integer/parseInt %)]
   ["-A" "--partition-cmd CMD" "Shell command to apply partition"
    :default ""]
   ["-H" "--heal-cmd CMD" "Shell command to heal partition"
    :default ""]
   ["-X" "--kill-cmd CMD" "Shell command to kill one node process"
    :default ""]
   ["-R" "--restart-cmd CMD" "Shell command to restart killed node"
    :default ""]

   ["-O" "--artifact-dir DIR" "Write run artifacts (summary/result/opts/argv) into this directory"
    :default ""]
   ["-U" "--run-id ID" "Caller-provided run id for manifest"
    :default ""]

   ["-h" "--help" "Show help"]])

(defn- parse-bool [s]
  (contains? #{"1" "true" "yes" "on"}
             (str/lower-case (str (or s "")))))

(defn- parse-history-mode [s]
  (case (str/lower-case (str (or s "")))
    "all" :all
    "completed-only" :completed-only
    :invalid))

(defn- split-csv [s]
  (->> (str/split (str s) #",")
       (map str/trim)
       (remove str/blank?)
       vec))

(defn- nemesis-op? [op]
  (= :nemesis (:process op)))

(defn- nemesis-errors [history]
  (->> history
       (filter nemesis-op?)
       (keep (fn [op]
               (let [v (:value op)]
                 (when (and (map? v) (contains? v :error))
                   {:f (:f op)
                    :type (:type op)
                    :error (:error v)
                    :exit (:exit v)
                    :err (:err v)}))))
       vec))

(defn- nemesis-summary [history]
  (let [ops (filter nemesis-op? history)
        errors (nemesis-errors history)
        by-f (frequencies (map :f ops))
        by-value (frequencies (keep (fn [op]
                                      (let [v (:value op)]
                                        (when (keyword? v) v)))
                                    ops))]
    {:event-count (count ops)
     :by-f by-f
     :by-value by-value
     :error-count (count errors)
     :errors errors}))

(defn- first-present
  [& xs]
  (first (drop-while nil? xs)))

(defn- linearizable-valid
  [result]
  (first-present (get-in result [:results :linearizable :valid?])
                 (get-in result [:results :linear :valid?])
                 (get-in result [:linearizable :valid?])
                 (get-in result [:linear :valid?])))

(defn- pass? [result nemesis]
  (let [lin-valid? (linearizable-valid result)
        overall-valid? (first-present (:valid? result)
                                      (get-in result [:results :valid?]))]
    (and (= true lin-valid?)
         (= true overall-valid?)
         (zero? (long (or (:error-count nemesis) 0))))))
(defn- choose-test [opts]
  (case (:workload opts)
    "broker-log" (core/broker-test opts)
    "kv-register" (core/kv-test opts)
    nil))

(defn- now-iso []
  (.toString (java.time.Instant/now)))

(defn- redact-opts [opts]
  (if (contains? opts :token)
    (assoc opts :token "<redacted>")
    opts))

(defn- maybe-path [result]
  (or (get result :dir)
      (get result :path)
      (get-in result [:store :dir])
      (get-in result [:store :path])
      (get-in result [:results :timeline :path])
      (get-in result [:results :timeline :html])
      nil))

(defn- write-artifacts! [opts args result nemesis overall-pass?]
  (let [artifact-dir (:artifact-dir opts)]
    (when-not (str/blank? artifact-dir)
      (let [dir-file (io/file artifact-dir)
            _ (.mkdirs dir-file)
            summary {:run_id         (:run-id opts)
                     :timestamp_utc  (now-iso)
                     :workload       (:workload opts)
                     :pass           overall-pass?
                     :linearizable   (linearizable-valid result)
                     :result_path    (maybe-path result)
                     :nemesis        nemesis
                     :nemesis-ok     (zero? (long (or (:error-count nemesis) 0)))
                     :argv           (vec args)
                     :opts           (redact-opts opts)}]
        (spit (io/file dir-file "summary.json") (json/generate-string summary {:pretty true}))
        (spit (io/file dir-file "result.edn") (pr-str result))
        (spit (io/file dir-file "opts.edn") (pr-str (redact-opts opts)))
        (spit (io/file dir-file "argv.txt") (str (str/join " " args) "\n"))))))

(defn -main [& args]
  (let [{:keys [options errors summary]} (parse-opts args cli-options)]
    (when (:help options)
      (println summary)
      (System/exit 0))

    (when (seq errors)
      (doseq [e errors] (binding [*out* *err*] (println e)))
      (binding [*out* *err*] (println summary))
      (System/exit 2))

    (let [opts (-> options
                   (update :nodes split-csv)
                   (update :nemesis-enabled parse-bool)
                   (update :linearizable-history-mode parse-history-mode))]
      (when (empty? (:nodes opts))
        (binding [*out* *err*] (println "No nodes provided"))
        (System/exit 2))

      (when (= :invalid (:linearizable-history-mode opts))
        (binding [*out* *err*]
          (println "Invalid --linearizable-history-mode. Allowed: all, completed-only"))
        (System/exit 2))

      (when (<= (long (:client-stagger-ms opts)) 0)
        (binding [*out* *err*]
          (println "Invalid --client-stagger-ms. Must be > 0"))
        (System/exit 2))

      (when (< (long (:nemesis-startup-sec opts)) 0)
        (binding [*out* *err*]
          (println "Invalid --nemesis-startup-sec. Must be >= 0"))
        (System/exit 2))

      (when-not (choose-test opts)
        (binding [*out* *err*]
          (println "Unknown workload:" (:workload opts))
          (println "Allowed: broker-log, kv-register"))
        (System/exit 2))

      (println "Running Ayder Jepsen test")
      (println "Workload:" (:workload opts))
      (println "Nodes:" (:nodes opts))
      (println "Nemesis enabled:" (:nemesis-enabled opts))
      (println "Nemesis startup sec:" (:nemesis-startup-sec opts))
      (println "Client stagger ms:" (:client-stagger-ms opts))
      (println "Linearizable history mode:" (:linearizable-history-mode opts))

      (let [result (jepsen/run! (choose-test opts))
            nemesis (nemesis-summary (:history result))
            ok? (pass? result nemesis)]
        (try
          (write-artifacts! opts args result nemesis ok?)
          (catch Throwable t
            (binding [*out* *err*]
              (println "Artifact write failed:" (.getMessage t)))))

        (if ok?
          (do
            (println "RESULT: PASS")
            (System/exit 0))
          (do
            (println "RESULT: FAIL")
            (System/exit 3)))))))

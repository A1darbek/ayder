(ns jepsen.ayder.nemesis
  (:require [clojure.java.shell :as sh]
            [clojure.string :as str]
            [jepsen.nemesis :as nemesis]))

(defn- run-shell!
  [cmd]
  (if (str/blank? cmd)
    {:exit 0 :out "" :err "" :skipped true}
    (sh/sh "bash" "-lc" cmd)))

(defn- run-shell-detached!
  [cmd]
  (if (str/blank? cmd)
    {:exit 0 :out "" :err "" :skipped true}
    ;; Launch in background and return immediately so :restart cannot block nemesis progression.
    (sh/sh "bash" "-lc"
           (str "nohup bash -lc "
                (pr-str cmd)
                " >/tmp/ayder-jepsen-restart.log 2>&1 < /dev/null &"))))

(defrecord ShellNemesis [partition-cmd heal-cmd kill-cmd restart-cmd]
  nemesis/Nemesis

  (setup! [this _test]
    this)

  (invoke! [_this _test op]
    (case (:f op)
      :partition
      (let [r (run-shell! partition-cmd)]
        (if (zero? (:exit r))
          (assoc op :type :info :value :partitioned)
          (assoc op :type :info :value {:error :partition-failed :exit (:exit r) :err (:err r)})))

      :heal
      (let [r (run-shell! heal-cmd)]
        (if (zero? (:exit r))
          (assoc op :type :info :value :healed)
          (assoc op :type :info :value {:error :heal-failed :exit (:exit r) :err (:err r)})))

      :kill
      (let [r (run-shell! kill-cmd)]
        (if (zero? (:exit r))
          (assoc op :type :info :value :killed)
          (assoc op :type :info :value {:error :kill-failed :exit (:exit r) :err (:err r)})))

      :restart
      (let [r (run-shell-detached! restart-cmd)]
        (if (zero? (:exit r))
          (assoc op :type :info :value :restarted)
          (assoc op :type :info :value {:error :restart-failed :exit (:exit r) :err (:err r)})))

      (assoc op :type :info :value :noop)))

  (teardown! [_this _test]
    (run-shell! heal-cmd)
    nil))

(defn shell-nemesis
  [{:keys [partition-cmd heal-cmd kill-cmd restart-cmd]}]
  (->ShellNemesis partition-cmd heal-cmd kill-cmd restart-cmd))
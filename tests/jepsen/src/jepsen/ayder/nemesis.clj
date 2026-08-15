(ns jepsen.ayder.nemesis
  (:require [clojure.java.shell :as sh]
            [clojure.string :as str]
            [jepsen.nemesis :as nemesis]))

(defn- run-shell!
  [cmd]
  (if (str/blank? cmd)
    {:exit 0 :out "" :err "" :skipped true}
    (sh/sh "bash" "-lc" cmd)))

(defn no-op-command?
  "True for an unconfigured command or a shell command which only reports
   success.  Such commands must never be counted as an injected fault."
  [cmd]
  (contains? #{"" ":" "true" "/bin/true" "/usr/bin/true"}
             (str/trim (str (or cmd "")))))

(defn- unavailable-op [op fault]
  (assoc op :type :info
         :value {:error (keyword (str (name fault) "-command-unconfigured"))
                 :exit 2
                 :err "blank/no-op fault command rejected"}))

(defrecord ShellNemesis [partition-cmd heal-cmd kill-cmd restart-cmd]
  nemesis/Nemesis

  (setup! [this _test]
    this)

  (invoke! [_this _test op]
    (case (:f op)
      :partition
      (if (no-op-command? partition-cmd)
        (unavailable-op op :partition)
        (let [r (run-shell! partition-cmd)]
          (if (zero? (:exit r))
            (assoc op :type :info :value :partitioned)
            (assoc op :type :info :value {:error :partition-failed :exit (:exit r) :err (:err r)}))))

      :heal
      (if (no-op-command? heal-cmd)
        (unavailable-op op :heal)
        (let [r (run-shell! heal-cmd)]
          (if (zero? (:exit r))
            (assoc op :type :info :value :healed)
            (assoc op :type :info :value {:error :heal-failed :exit (:exit r) :err (:err r)}))))

      :kill
      (if (no-op-command? kill-cmd)
        (unavailable-op op :kill)
        (let [r (run-shell! kill-cmd)]
          (if (zero? (:exit r))
            (assoc op :type :info :value :killed)
            (assoc op :type :info :value {:error :kill-failed :exit (:exit r) :err (:err r)}))))

      :restart
      (if (no-op-command? restart-cmd)
        (unavailable-op op :restart)
        ;; A restart is complete only when the command itself has verified it.
        ;; Detaching this shell used to turn launch failures into false success.
        (let [r (run-shell! restart-cmd)]
          (if (zero? (:exit r))
            (assoc op :type :info :value :restarted)
            (assoc op :type :info :value {:error :restart-failed :exit (:exit r) :err (:err r)}))))

      (assoc op :type :info :value :noop)))

  (teardown! [_this _test]
    (when-not (no-op-command? heal-cmd)
      (run-shell! heal-cmd))
    nil))

(defn shell-nemesis
  [{:keys [partition-cmd heal-cmd kill-cmd restart-cmd]}]
  (->ShellNemesis partition-cmd heal-cmd kill-cmd restart-cmd))

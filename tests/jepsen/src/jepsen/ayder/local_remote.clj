(ns jepsen.ayder.local-remote
  (:require [clojure.java.shell :as sh]
            [clojure.string :as str]
            [jepsen.control.core :as core]))

(defn- paths->seq [x]
  (if (sequential? x) x [x]))

(defn- shell-cmd [ctx cmd]
  (let [cmd (if-let [dir (:dir ctx)]
              (if (str/blank? dir)
                cmd
                (str "cd " (core/escape dir) " && " cmd))
              cmd)]
    (if-let [sudo-user (:sudo ctx)]
      (str "sudo -n -u " (core/escape sudo-user)
           " bash -lc " (core/escape cmd))
      cmd)))

(defn- run-bash
  ([cmd]
   (run-bash cmd nil))
  ([cmd in]
   (if (nil? in)
     (sh/sh "bash" "-lc" cmd)
     (sh/sh "bash" "-lc" cmd :in in))))

(defrecord LocalRemote [conn-spec]
  core/Remote

  (connect [this conn-spec]
    (assoc this :conn-spec conn-spec))

  (disconnect! [_this]
    nil)

  (execute! [_this ctx action]
    (let [cmd (shell-cmd ctx (:cmd action))
          resp (run-bash cmd (:in action))]
      (assoc action
             :exit (:exit resp)
             :out (:out resp)
             :err (:err resp))))

  (upload! [_this _ctx local-paths remote-path _opts]
    (let [srcs (paths->seq local-paths)
          cmd (str "cp -a "
                   (str/join " " (map core/escape srcs))
                   " "
                   (core/escape remote-path))
          resp (run-bash cmd)]
      (when-not (zero? (:exit resp))
        (throw (ex-info "local upload failed"
                        {:exit (:exit resp)
                         :err (:err resp)
                         :cmd cmd})))
      nil))

  (download! [_this _ctx remote-paths local-path _opts]
    (let [srcs (paths->seq remote-paths)
          cmd (str "cp -a "
                   (str/join " " (map core/escape srcs))
                   " "
                   (core/escape local-path))
          resp (run-bash cmd)]
      (when-not (zero? (:exit resp))
        (throw (ex-info "local download failed"
                        {:exit (:exit resp)
                         :err (:err resp)
                         :cmd cmd})))
      nil)))

(defn remote
  []
  (->LocalRemote nil))

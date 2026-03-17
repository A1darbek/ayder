(ns jepsen.ayder.broker-model
  (:require [knossos.model :as model])
  (:import [knossos.model Model]))

(defn- safe-get [x k]
  (try
    (get x k)
    (catch Throwable _ nil)))

(defn- parse-read-at-op [op]
  (let [v (safe-get op :value)
        req-direct (safe-get op :offset)]
    (if (map? v)
      {:req (or req-direct (safe-get v :offset))
       :got (safe-get v :value)}
      {:req req-direct
       :got v})))

(defn- ignore-op?
  [op]
  (contains? #{:invoke :fail :info} (:type op)))

(defrecord AppendOnlyLog [next-offset values]
  Model

  (step [this op]
    (case (:f op)
      :append
      (let [ret (:value op)
            off (safe-get ret :offset)
            msg (safe-get ret :msg)]
        (cond
          (ignore-op? op)
          this

          (not (map? ret))
          (model/inconsistent (str "append returned non-map: " (pr-str ret)))

          (not (integer? off))
          (model/inconsistent (str "append returned non-integer offset: " (pr-str ret)))

          (not= off next-offset)
          (model/inconsistent (str "append expected offset " next-offset " got " off))

          :else
          (->AppendOnlyLog (inc next-offset)
                           (assoc values off msg))))

      :read-at
      (if (ignore-op? op)
        this
        (let [{:keys [req got]} (parse-read-at-op op)]
          (cond
            (not (integer? req))
            (model/inconsistent (str "read-at missing integer offset, op=" (pr-str op)))

            (nil? got)
            (if (>= req next-offset)
              this
              (model/inconsistent (str "read-at(" req ") returned nil, but next-offset=" next-offset)))

            (>= req next-offset)
            (model/inconsistent (str "read-at(" req ") returned value before append, next-offset=" next-offset))

            (not (contains? values req))
            (model/inconsistent (str "read-at(" req ") not present in model map"))

            (= got (get values req))
            this

            :else
            (model/inconsistent (str "read-at(" req ") expected=" (pr-str (get values req))
                                     " got=" (pr-str got))))))

      this))

  Object
  (toString [_]
    (str "AppendOnlyLog{next-offset=" next-offset ", entries=" (count values) "}")))

(defn model
  []
  (->AppendOnlyLog 0 {}))

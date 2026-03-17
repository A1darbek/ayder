(ns jepsen.ayder.broker-client
  (:require [cheshire.core :as json]
            [clj-http.client :as http]
            [clojure.string :as str]
            [jepsen.client :as client]))

(import '(java.net URI URLEncoder)
        '(java.util Base64 UUID))

(def uncertain-statuses
  #{0 408 425 429 500 502 503 504})

(def uncertain-errors
  #{"commit_timeout" "timeout" "request_timeout" "no_quorum"})

(def failover-errors
  #{"not_leader" "redirect_to_leader" "read_only_follower" "no_quorum" "commit_timeout" "timeout" "request_timeout"})

(defn- auth-headers [token content-type]
  (if (str/blank? token)
    {"Content-Type" content-type}
    {"Authorization" (str "Bearer " token)
     "Content-Type" content-type}))

(defn- parse-json-safe [s]
  (when (and s (not (str/blank? s)))
    (try
      (json/parse-string s true)
      (catch Throwable _ nil))))

(defn- decode-b64 [s]
  (when (string? s)
    (try
      (String. (.decode (Base64/getDecoder) ^String s) "UTF-8")
      (catch Throwable _ nil))))

(defn- env-long
  [k default-v]
  (let [raw (System/getenv k)]
    (if (str/blank? raw)
      (long default-v)
      (try
        (Long/parseLong raw)
        (catch Throwable _
          (long default-v))))))

(def http-socket-timeout-ms
  (delay (max 1000 (env-long "AYDER_JEPSEN_HTTP_SOCKET_TIMEOUT_MS" 15000))))

(def http-conn-timeout-ms
  (delay (max 500 (env-long "AYDER_JEPSEN_HTTP_CONN_TIMEOUT_MS" 5000))))

(def broker-produce-timeout-ms
  (delay (max 1000 (env-long "AYDER_JEPSEN_BROKER_PRODUCE_TIMEOUT_MS" 5000))))

(def broker-read-timeout-ms
  (delay (max 500 (env-long "AYDER_JEPSEN_BROKER_READ_TIMEOUT_MS" 3000))))

(def append-resolve-attempts
  (delay (max 1 (env-long "AYDER_JEPSEN_APPEND_RESOLVE_ATTEMPTS" 10))))

(def append-resolve-initial-sleep-ms
  (delay (max 25 (env-long "AYDER_JEPSEN_APPEND_RESOLVE_INITIAL_SLEEP_MS" 150))))

(def append-resolve-max-sleep-ms
  (delay (max 100 (env-long "AYDER_JEPSEN_APPEND_RESOLVE_MAX_SLEEP_MS" 1500))))

(defn- request* [{:keys [method url headers body socket-timeout-ms conn-timeout-ms]}]
  (try
    (let [resp (http/request {:method method
                              :url url
                              :headers headers
                              :body body
                              :socket-timeout (long (or socket-timeout-ms @http-socket-timeout-ms))
                              :conn-timeout (long (or conn-timeout-ms @http-conn-timeout-ms))
                              :throw-exceptions false
                              :as :text})]
      (assoc resp :json (parse-json-safe (:body resp))))
    (catch Throwable t
      {:status 0
       :body nil
       :json nil
       :exception (.getMessage t)})))

(defn- redirected-url [resp fallback-url]
  (or (get-in resp [:headers "location"])
      (get-in resp [:json :redirect :location])
      (when-let [leader-url (get-in resp [:json :leader_url])]
        (if-let [path (re-find #"/broker/.*$" fallback-url)]
          (str leader-url path)
          fallback-url))))

(defn- maybe-follow-redirect [{:keys [method url headers body socket-timeout-ms conn-timeout-ms]}]
  (let [socket-ms (long (max 500 (or socket-timeout-ms @http-socket-timeout-ms)))
        conn-ms (long (max 250 (or conn-timeout-ms @http-conn-timeout-ms)))
        resp0 (request* {:method method
                         :url url
                         :headers headers
                         :body body
                         :socket-timeout-ms socket-ms
                         :conn-timeout-ms conn-ms})
        status (:status resp0)
        err (get-in resp0 [:json :error])
        should-follow (or (#{307 308} status)
                          (and (= 503 status)
                               (#{"not_leader" "redirect_to_leader" "read_only_follower"} err)))]
    (if should-follow
      (if-let [u (redirected-url resp0 url)]
        (request* {:method method
                   :url u
                   :headers headers
                   :body body
                   :socket-timeout-ms socket-ms
                   :conn-timeout-ms conn-ms})
        resp0)
      resp0)))

(defn- indeterminate? [resp]
  (or (contains? uncertain-statuses (:status resp))
      (contains? uncertain-errors (get-in resp [:json :error]))
      (string? (:exception resp))))

(defn- should-failover? [resp]
  (or (= 0 (:status resp))
      (string? (:exception resp))
      (contains? failover-errors (get-in resp [:json :error]))
      (contains? uncertain-statuses (:status resp))))

(defn- path-and-query [u]
  (try
    (let [uri (URI. u)
          p (or (.getRawPath uri) "/")
          q (.getRawQuery uri)]
      (if (str/blank? q) p (str p "?" q)))
    (catch Throwable _
      u)))

(defn- failover-candidates [cluster-nodes base-url]
  (->> cluster-nodes
       (map str)
       (remove str/blank?)
       distinct
       (remove #(= % (str (or base-url ""))))
       shuffle
       vec))

(defn- request-with-failover [req cluster-nodes]
  (let [full-socket-ms (long (max 1000 (or (:socket-timeout-ms req) @http-socket-timeout-ms)))
        full-conn-ms (long (max 250 (or (:conn-timeout-ms req) @http-conn-timeout-ms)))
        resp0 (maybe-follow-redirect (assoc req
                                            :socket-timeout-ms full-socket-ms
                                            :conn-timeout-ms full-conn-ms))]
    (if (and (should-failover? resp0)
             (seq cluster-nodes))
      (let [pq (path-and-query (:url req))
            candidates (failover-candidates cluster-nodes (:base-url req))
            failover-socket-ms (long (max 1200 (min 2500 full-socket-ms)))
            failover-conn-ms (long (max 400 (min 1000 full-conn-ms)))]
        (loop [nodes candidates
               last-resp resp0]
          (if-let [n (first nodes)]
            (let [resp (maybe-follow-redirect {:method (:method req)
                                               :url (str n pq)
                                               :headers (:headers req)
                                               :body (:body req)
                                               :socket-timeout-ms failover-socket-ms
                                               :conn-timeout-ms failover-conn-ms})]
              (if (should-failover? resp)
                (recur (rest nodes) resp)
                resp))
            last-resp)))
      resp0)))

(defn- as-int [x]
  (when (number? x) (long x)))

(defn- q-encode [s]
  (URLEncoder/encode (str s) "UTF-8"))

(defn- append-idempotency-key [op]
  (let [p (:process op)
        idx (:index op)]
    ;; Unique across distinct appends, stable across retries for one op.
    (str "p" p ":i" (if (integer? idx) idx "na")
         ":u" (str (UUID/randomUUID)))))
(defn- append-ok-meta [resp]
  (let [j (:json resp)
        off (as-int (:offset j))]
    (when (and (= 200 (:status resp)) (= true (:ok j)) (integer? off))
      {:offset off :json j})))

(defn- resolve-append-indeterminate [req candidates]
  (let [resolve-socket-ms (long (max 3000 (* 2 @broker-produce-timeout-ms)))
        resolve-conn-ms (long (max 800 @http-conn-timeout-ms))
        max-attempts (long @append-resolve-attempts)
        initial-sleep-ms (long @append-resolve-initial-sleep-ms)
        max-sleep-ms (long @append-resolve-max-sleep-ms)]
    (loop [attempt 0
           sleep-ms initial-sleep-ms
           last-resp nil]
      (if (>= attempt max-attempts)
        last-resp
        (do
          (Thread/sleep sleep-ms)
          (let [resp (request-with-failover (assoc req
                                              :socket-timeout-ms resolve-socket-ms
                                              :conn-timeout-ms resolve-conn-ms)
                                            candidates)]
            (if (or (append-ok-meta resp) (not (indeterminate? resp)))
              resp
              (recur (inc attempt)
                     (long (min max-sleep-ms (max 25 (* 2 sleep-ms))))
                     resp))))))))

(defrecord BrokerClient [token topic group partition topic-partitions base-url direct-url cluster-nodes]
  client/Client

  (open! [this test node]
    (assoc this :base-url node
                :cluster-nodes (vec (:nodes test))))

  (setup! [this _test]
    (let [target (if (str/blank? direct-url) base-url direct-url)
          setup-socket-ms (long (max 1500 (min 5000 @http-socket-timeout-ms)))
          setup-conn-ms (long (max 500 (min 1500 @http-conn-timeout-ms)))
          body (json/generate-string {:name topic :partitions topic-partitions})
          req {:method :post
               :url (str target "/broker/topics")
               :headers (auth-headers token "application/json")
               :body body
               :base-url target
               :socket-timeout-ms setup-socket-ms
               :conn-timeout-ms setup-conn-ms}
          _ (if (str/blank? direct-url)
              (request-with-failover req cluster-nodes)
              (maybe-follow-redirect req))]
      this))

  (invoke! [_this _test op]
    (let [target (if (str/blank? direct-url) base-url direct-url)
          candidates (if (str/blank? direct-url) cluster-nodes [target])]
      (case (:f op)
        :append
        (let [msg (str (:value op))
              idk (append-idempotency-key op)
              url (str target "/broker/topics/" topic "/produce"
                       "?partition=" partition
                       "&timeout_ms=" @broker-produce-timeout-ms
                       "&idempotency_key=" (q-encode idk))
              req {:method :post
                   :url url
                   :headers (auth-headers token "application/octet-stream")
                   :body msg
                   :base-url target}
              resp0 (request-with-failover req candidates)
              resp (if (indeterminate? resp0)
                     (or (resolve-append-indeterminate req candidates) resp0)
                     resp0)
              j (:json resp)
              off (as-int (:offset j))]
          (cond
            (and (= 200 (:status resp)) (= true (:ok j)) (integer? off))
            (assoc op :type :ok :value {:offset off :msg msg})

            (indeterminate? resp)
            (assoc op :type :info :error {:status (:status resp)
                                          :error (:error j)
                                          :body (:body resp)
                                          :exception (:exception resp)})

            :else
            (assoc op :type :fail :error {:status (:status resp)
                                          :error (:error j)
                                          :body (:body resp)
                                          :exception (:exception resp)})))

        :read-at
        (let [req-off (long (:offset op))
              url (str target "/broker/consume/" topic "/" group "/" partition
                       "?limit=1&offset=" req-off "&encoding=b64"
                       "&linearizable_timeout_ms=" @broker-read-timeout-ms)
              resp (request-with-failover {:method :get
                                           :url url
                                           :headers (auth-headers token "application/json")
                                           :base-url target}
                                          candidates)
              j (:json resp)
              msgs (when (map? j) (:messages j))]
          (cond
            (and (= 200 (:status resp)) (vector? msgs))
            (if (seq msgs)
              (let [m (first msgs)
                    got-off (as-int (:offset m))
                    got-val (or (decode-b64 (:value_b64 m))
                                (:value m))]
                (if (= got-off req-off)
                  (assoc op :type :ok :value {:offset req-off :value got-val})
                  (assoc op :type :fail :error {:status 200
                                                :error :offset_mismatch
                                                :requested req-off
                                                :got got-off
                                                :body (:body resp)})))
              (assoc op :type :ok :value {:offset req-off :value nil}))

            (indeterminate? resp)
            (assoc op :type :info :error {:status (:status resp)
                                          :error (:error j)
                                          :body (:body resp)
                                          :exception (:exception resp)})

            :else
            (assoc op :type :fail :error {:status (:status resp)
                                          :error (:error j)
                                          :body (:body resp)
                                          :exception (:exception resp)})))

        (assoc op :type :fail :error :unsupported-op))))

  (teardown! [this _test] this)
  (close! [this _test] this))

(defn client
  [{:keys [token topic group partition topic-partitions direct-url]}]
  (->BrokerClient token topic group partition topic-partitions nil direct-url nil))

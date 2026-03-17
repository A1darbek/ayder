(ns jepsen.ayder.client
  (:require [cheshire.core :as json]
            [clj-http.client :as http]
            [clojure.string :as str]
            [jepsen.client :as client]))

(import '(java.util Base64))

(def uncertain-statuses
  #{0 408 425 429 500 502 503 504})

(def uncertain-errors
  #{"commit_timeout" "timeout" "request_timeout"})

(defn- auth-headers [token]
  (if (str/blank? token)
    {"Content-Type" "application/octet-stream"}
    {"Authorization" (str "Bearer " token)
     "Content-Type" "application/octet-stream"}))

(defn- parse-json-safe [s]
  (when (and s (not (str/blank? s)))
    (try
      (json/parse-string s true)
      (catch Throwable _ nil))))

(defn- decode-b64 [s]
  (when s
    (try
      (String. (.decode (Base64/getDecoder) ^String s) "UTF-8")
      (catch Throwable _ nil))))

(defn- request* [{:keys [method url headers body]}]
  (try
    (let [resp (http/request {:method method
                              :url url
                              :headers headers
                              :body body
                              :socket-timeout 4000
                              :conn-timeout 4000
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
        (str leader-url (re-find #"/kv/.*$" fallback-url)))))

(defn- maybe-follow-redirect
  [{:keys [method url headers body]}]
  (let [resp0 (request* {:method method
                         :url url
                         :headers headers
                         :body body})]
    (if (#{307 308} (:status resp0))
      (if-let [u (redirected-url resp0 url)]
        (request* {:method method
                   :url u
                   :headers headers
                   :body body})
        resp0)
      resp0)))

(defn- indeterminate? [resp]
  (or (contains? uncertain-statuses (:status resp))
      (contains? uncertain-errors (get-in resp [:json :error]))
      (string? (:exception resp))))

(defrecord KVClient [token namespace key base-url]
  client/Client

  (open! [this _test node]
    (assoc this :base-url node))

  (setup! [this _test]
    (let [url (str base-url "/kv/" namespace "/" key)]
      (request* {:method :delete
                 :url url
                 :headers (auth-headers token)})
      this))

  (invoke! [_this _test op]
    (let [url (str base-url "/kv/" namespace "/" key)]
      (case (:f op)
        :read
        (let [resp (maybe-follow-redirect {:method :get
                                           :url url
                                           :headers (auth-headers token)})
              code (:status resp)
              j (:json resp)]
          (cond
            (and (= 200 code) (string? (:value j)))
            (assoc op :type :ok :value (decode-b64 (:value j)))

            (= "not_found" (:error j))
            (assoc op :type :ok :value nil)

            (indeterminate? resp)
            (assoc op :type :info :error {:status code :error (:error j) :body (:body resp)})

            :else
            (assoc op :type :fail :error {:status code :error (:error j) :body (:body resp)})))

        :write
        (let [body (str (:value op))
              resp (maybe-follow-redirect {:method :post
                                           :url (str url "?timeout_ms=3000")
                                           :headers (auth-headers token)
                                           :body body})
              j (:json resp)]
          (cond
            (and (= 200 (:status resp)) (= true (:ok j)))
            (assoc op :type :ok)

            (indeterminate? resp)
            (assoc op :type :info :error {:status (:status resp)
                                          :error (:error j)
                                          :body (:body resp)})

            :else
            (assoc op :type :fail :error {:status (:status resp)
                                          :error (:error j)
                                          :body (:body resp)})))

        (assoc op :type :fail :error :unsupported-op))))

  (teardown! [this _test] this)
  (close! [this _test] this))

(defn client
  [{:keys [token namespace key]}]
  (->KVClient token namespace key nil))
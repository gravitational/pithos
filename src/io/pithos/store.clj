(ns io.pithos.store
  "Generic cassandra cluster connection services."
  (:import com.datastax.driver.core.exceptions.InvalidQueryException)
  (:require [qbits.alia            :as alia]
            [qbits.hayt            :refer [use-keyspace create-keyspace with]]
            [clojure.tools.logging :refer [debug]]
            [clojure.java.io       :as io]))

(defprotocol Convergeable
  (converge! [this]))

(defprotocol Crudable
  (fetch [this k] [this k1 k2] [this k1 k2 k3])
  (update! [this k v] [this k1 k2 v] [this k1 k2 k3 v])
  (delete! [this k] [this k1 k2] [this k1 k2 k3])
  (create! [this k v] [this k1 k2 v] [this k1 k2 k3 v]))

(defn cassandra-store
  "Connect to a cassandra cluster, and use a specific keyspace.
   When the keyspace is not found, try creating it"
  [{:keys [cassandra-options cluster keyspace hints repfactor username password tls tls-options socket-options pooling] :as config}]
  (debug "building cassandra store for: " cluster keyspace hints)
  (let [hints   (or hints
                    {:replication {:class              "SimpleStrategy"
                                   :replication_factor (or repfactor 1)}})
        cluster (if (sequential? cluster) cluster [cluster])
        tls (or tls false)
        tls-options (or tls-options
                        ;; List of Mozilla's recommended suites for TLSv1.2(maximum supported in Java 8)
                        ;; https://wiki.mozilla.org/Security/Server_Side_TLS#Intermediate_compatibility_.28recommended.29
                        {:cipher-suites ["TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256" "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256" "TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384" "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384" "TLS_DHE_RSA_WITH_AES_128_GCM_SHA256" "TLS_DHE_RSA_WITH_AES_256_GCM_SHA384"]
                         :keystore-path (-> (io/file (. (. System getProperties) (get "java.home")) "lib" "security" "cacerts") (.getPath))
                         :keystore-password "changeit"})
        socket-options (or socket-options
                           ;; Default options for datastacks Java driver.
                           ;; https://docs.datastax.com/en/drivers/java/3.1/com/datastax/driver/core/SocketOptions.html#DEFAULT_CONNECT_TIMEOUT_MILLIS
                           {:connect-timeout-millis 5000
                            :read-timeout-millis 12000})
        pooling (or pooling
                    ;; Default values for pooling option supported by alia 3.3.0
                    ;; https://docs.datastax.com/en/drivers/java/3.1/com/datastax/driver/core/PoolingOptions.html#setCoreConnectionsPerHost-com.datastax.driver.core.HostDistance-int-
                    {:core-connections-per-host {:local 1 :remote 1}
                     :max-connections-per-host {:local 1 :remote 1}
                     :connection-thresholds {:local 800 :remote 200}})
        session (-> (assoc cassandra-options :contact-points cluster)
                    (cond-> (and username password)
                      (assoc :credentials {:user     username
                                           :password password})
                      (true? tls) (assoc :ssl-options tls-options))
                    (assoc :ssl? tls)
                    (assoc :socket-options socket-options)
                    (assoc :pooling-options pooling)
                    (alia/cluster)
                    (alia/connect))]
    (try (alia/execute session (use-keyspace keyspace))
         session
         (catch clojure.lang.ExceptionInfo e
           (let [{:keys [exception]} (ex-data e)]
             (if (and (= (class exception) InvalidQueryException)
                      (re-find #"^[kK]eyspace.*does not exist$"
                               (.getMessage exception)))
               (do (alia/execute session
                                 (create-keyspace keyspace (with hints)))
                 (alia/execute session (use-keyspace keyspace))
                 session)
               (throw e)))))))

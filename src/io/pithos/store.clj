(ns io.pithos.store
  "Generic cassandra cluster connection services."
  (:import com.datastax.driver.core.exceptions.InvalidQueryException)
  (:require [qbits.alia            :as alia]
            [qbits.hayt            :refer [use-keyspace create-keyspace with]]
            [clojure.tools.logging :refer [debug]]))

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
  [{:keys [cassandra-options cluster keyspace hints repfactor username password tls tls-options] :as config}]
  (debug "building cassandra store for: " cluster keyspace hints)
  (let [hints   (or hints
                    {:replication {:class              "SimpleStrategy"
                                   :replication_factor (or repfactor 1)}})
        cluster (if (sequential? cluster) cluster [cluster])
        tls (or tls false)
        tls-options (or tls-options
                        ;; List of Mozilla's recommended suites for TLSv1.2(maximum supported in Java 8)
                        {:cipher-suites ["TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256" "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256" "TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384" "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384" "TLS_DHE_RSA_WITH_AES_128_GCM_SHA256" "TLS_DHE_RSA_WITH_AES_256_GCM_SHA384"]
                         :keystore-path (str (. (. System getProperties) (get "java.home")) "/lib/security/cacerts")
                         :keystore-password "changeit"})
        session (-> (assoc cassandra-options :contact-points cluster)
                    (cond-> (and username password)
                      (assoc :credentials {:user     username
                                           :password password})
                      (true? tls) (assoc :ssl-options tls-options))
                    (assoc :ssl? tls)
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

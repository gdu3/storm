;; Licensed to the Apache Software Foundation (ASF) under one
;; or more contributor license agreements.  See the NOTICE file
;; distributed with this work for additional information
;; regarding copyright ownership.  The ASF licenses this file
;; to you under the Apache License, Version 2.0 (the
;; "License"); you may not use this file except in compliance
;; with the License.  You may obtain a copy of the License at
;;
;; http://www.apache.org/licenses/LICENSE-2.0
;;
;; Unless required by applicable law or agreed to in writing, software
;; distributed under the License is distributed on an "AS IS" BASIS,
;; WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
;; See the License for the specific language governing permissions and
;; limitations under the License.

(ns backtype.storm.config
  (:import [java.io FileReader File IOException]
           [backtype.storm.generated StormTopology])
  (:import [backtype.storm Config ConfigValidation$FieldValidator])
  (:import [backtype.storm.utils Utils LocalState])
  (:import [org.apache.commons.io FileUtils])
  (:require [clojure [string :as str]])
  (:use [backtype.storm log util]))

(def RESOURCES-SUBDIR "resources")

(defn- clojure-config-name [name]
  (.replace (.toUpperCase name) "_" "-"))

;; define clojure constants for every configuration parameter
(doseq [f (seq (.getFields Config))]
  (let [name (.getName f)
        new-name (clojure-config-name name)]
    (eval
      `(def ~(symbol new-name) (. Config ~(symbol name))))))

(def ALL-CONFIGS
  (dofor [f (seq (.getFields Config))]
         (.get f nil)))

(defmulti get-FieldValidator class-selector)

(defmethod get-FieldValidator nil [_]
  (throw (IllegalArgumentException. "Cannot validate a nil field.")))

(defmethod get-FieldValidator
  ConfigValidation$FieldValidator [validator] validator)

(defmethod get-FieldValidator Object
  [klass]
  {:pre [(not (nil? klass))]}
  (reify ConfigValidation$FieldValidator
    (validateField [this name v]
                   (if (and (not (nil? v))
                            (not (instance? klass v)))
                     (throw (IllegalArgumentException.
                              (str "field " name " '" v "' must be a '" (.getName klass) "'")))))))

;; Create a mapping of config-string -> validator
;; Config fields must have a _SCHEMA field defined
(def CONFIG-SCHEMA-MAP
  (->> (.getFields Config)
       (filter #(not (re-matches #".*_SCHEMA$" (.getName %))))
       (map (fn [f] [(.get f nil)
                     (get-FieldValidator
                       (-> Config
                           (.getField (str (.getName f) "_SCHEMA"))
                           (.get nil)))]))
       (into {})))

(defn cluster-mode
  [conf & args]
  (keyword (conf STORM-CLUSTER-MODE)))

(defn local-mode?
  [conf]
  (let [mode (conf STORM-CLUSTER-MODE)]
    (condp = mode
      "local" true
      "distributed" false
      (throw (IllegalArgumentException.
               (str "Illegal cluster mode in conf: " mode))))))

(defn sampling-rate
  [conf]
  (->> (conf TOPOLOGY-STATS-SAMPLE-RATE)
       (/ 1)
       int))

(defn mk-stats-sampler
  [conf]
  (even-sampler (sampling-rate conf)))

; storm.zookeeper.servers:
;     - "server1"
;     - "server2"
;     - "server3"
; nimbus.host: "master"
;
; ########### These all have default values as shown
;
; ### storm.* configs are general configurations
; # the local dir is where jars are kept
; storm.local.dir: "/mnt/storm"
; storm.zookeeper.port: 2181
; storm.zookeeper.root: "/storm"

(defn read-default-config
  []
  (clojurify-structure (Utils/readDefaultConfig)))

(defn validate-configs-with-schemas
  [conf]
  (doseq [[k v] conf
          :let [schema (CONFIG-SCHEMA-MAP k)]]
    (if (not (nil? schema))
      (.validateField schema k v))))

(defn read-storm-config
  []
  (let [conf (clojurify-structure (Utils/readStormConfig))]
    (validate-configs-with-schemas conf)
    conf))

(defn read-yaml-config
  ([name must-exist]
     (let [conf (clojurify-structure (Utils/findAndReadConfigFile name must-exist))]
       (validate-configs-with-schemas conf)
       conf))
  ([name]
     (read-yaml-config true)))

(defn absolute-storm-local-dir [conf]
  (let [storm-home (System/getProperty "storm.home")
        path (conf STORM-LOCAL-DIR)]
    (if path
      (if (is-absolute-path? path) path (str storm-home file-path-separator path))
      (str storm-home file-path-separator "storm-local"))))

(defn master-local-dir
  [conf]
  (let [ret (str (absolute-storm-local-dir conf) file-path-separator "nimbus")]
    (FileUtils/forceMkdir (File. ret))
    ret))

(defn master-stormdist-root
  ([conf]
   (str (master-local-dir conf) file-path-separator "stormdist"))
  ([conf storm-id]
   (str (master-stormdist-root conf) file-path-separator storm-id)))

(defn master-stormjar-path
  [stormroot]
  (str stormroot file-path-separator "stormjar.jar"))

(defn master-stormcode-path
  [stormroot]
  (str stormroot file-path-separator "stormcode.ser"))

(defn master-stormconf-path
  [stormroot]
  (str stormroot file-path-separator "stormconf.ser"))

(defn master-inbox
  [conf]
  (let [ret (str (master-local-dir conf) file-path-separator "inbox")]
    (FileUtils/forceMkdir (File. ret))
    ret ))

(defn master-inimbus-dir
  [conf]
  (str (master-local-dir conf) file-path-separator "inimbus"))

(defn supervisor-local-dir
  [conf]
  (let [ret (str (absolute-storm-local-dir conf) file-path-separator "supervisor")]
    (FileUtils/forceMkdir (File. ret))
    ret))

(defn supervisor-isupervisor-dir
  [conf]
  (str (supervisor-local-dir conf) file-path-separator "isupervisor"))

(defn supervisor-stormdist-root
  ([conf]
   (str (supervisor-local-dir conf) file-path-separator "stormdist"))
  ([conf storm-id]
   (str (supervisor-stormdist-root conf) file-path-separator (url-encode storm-id))))

(defn supervisor-stormjar-path
  [stormroot]
  (str stormroot file-path-separator "stormjar.jar"))

(defn supervisor-stormcode-path
  [stormroot]
  (str stormroot file-path-separator "stormcode.ser"))

(defn supervisor-stormconf-path
  [stormroot]
  (str stormroot file-path-separator "stormconf.ser"))

(defn supervisor-tmp-dir
  [conf]
  (let [ret (str (supervisor-local-dir conf) file-path-separator "tmp")]
    (FileUtils/forceMkdir (File. ret))
    ret ))

(defn supervisor-storm-resources-path
  [stormroot]
  (str stormroot file-path-separator RESOURCES-SUBDIR))

(defn ^LocalState supervisor-state
  [conf]
  (LocalState. (str (supervisor-local-dir conf) file-path-separator "localstate")))

(defn read-supervisor-storm-conf
  [conf storm-id]
  (let [stormroot (supervisor-stormdist-root conf storm-id)
        conf-path (supervisor-stormconf-path stormroot)
        topology-path (supervisor-stormcode-path stormroot)]
    (merge conf (clojurify-structure (Utils/fromCompressedJsonConf (FileUtils/readFileToByteArray (File. conf-path)))))))

(defn read-supervisor-topology
  [conf storm-id]
  (let [stormroot (supervisor-stormdist-root conf storm-id)
        topology-path (supervisor-stormcode-path stormroot)]
    (Utils/deserialize (FileUtils/readFileToByteArray (File. topology-path)) StormTopology)
    ))

(defn worker-user-root [conf]
  (str (absolute-storm-local-dir conf) "/workers-users"))

(defn worker-user-file [conf worker-id]
  (str (worker-user-root conf) "/" worker-id))

(defn get-worker-user [conf worker-id]
  (log-message "GET worker-user " worker-id)
  (try
    (str/trim (slurp (worker-user-file conf worker-id)))
  (catch IOException e
    (log-warn-error e "Failed to get worker user for " worker-id ".")
    nil
    )))

  
(defn set-worker-user! [conf worker-id user]
  (log-message "SET worker-user " worker-id " " user)
  (let [file (worker-user-file conf worker-id)]
    (.mkdirs (.getParentFile (File. file)))
    (spit (worker-user-file conf worker-id) user)))

(defn remove-worker-user! [conf worker-id]
  (log-message "REMOVE worker-user " worker-id)
  (.delete (File. (worker-user-file conf worker-id))))

(defn worker-root
  ([conf]
   (str (absolute-storm-local-dir conf) file-path-separator "workers"))
  ([conf id]
   (str (worker-root conf) file-path-separator id)))

(defn worker-pids-root
  [conf id]
  (str (worker-root conf id) file-path-separator "pids"))

(defn worker-pid-path
  [conf id pid]
  (str (worker-pids-root conf id) file-path-separator pid))

(defn worker-heartbeats-root
  [conf id]
  (str (worker-root conf id) file-path-separator "heartbeats"))

;; workers heartbeat here with pid and timestamp
;; if supervisor stops receiving heartbeat, it kills and restarts the process
;; in local mode, keep a global map of ids to threads for simulating process management
(defn ^LocalState worker-state
  [conf id]
  (LocalState. (worker-heartbeats-root conf id)))

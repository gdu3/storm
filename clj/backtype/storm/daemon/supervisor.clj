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
(ns backtype.storm.daemon.supervisor
  (:import [java.io OutputStreamWriter BufferedWriter IOException])
  (:import [backtype.storm.scheduler ISupervisor]
           [backtype.storm.utils LocalState Time Utils]
           [backtype.storm.daemon Shutdownable]
           [backtype.storm Constants]
           [java.net JarURLConnection]
           [java.net URI]
           [org.apache.commons.io FileUtils]
           [java.io File])
  (:use [backtype.storm config util log timer local-state])
  (:import [backtype.storm.utils VersionInfo])
  (:use [backtype.storm.daemon common])
  (:require [backtype.storm.daemon [worker :as worker]]
            [backtype.storm [process-simulator :as psim] [cluster :as cluster] [event :as event]]
            [clojure.set :as set])
  (:import [org.apache.zookeeper data.ACL ZooDefs$Ids ZooDefs$Perms])
  (:import [org.yaml.snakeyaml Yaml]
           [org.yaml.snakeyaml.constructor SafeConstructor])
  (:gen-class
    :methods [^{:static true} [launch [backtype.storm.scheduler.ISupervisor] void]]))

(defmulti download-storm-code cluster-mode)
(defmulti launch-worker (fn [supervisor & _] (cluster-mode (:conf supervisor))))

(defprotocol SupervisorDaemon
  (get-id [this])
  (get-conf [this])
  (shutdown-all-workers [this])
  )

(defn- assignments-snapshot [storm-cluster-state callback assignment-versions]
  (let [storm-ids (.assignments storm-cluster-state callback)]
    (let [new-assignments
          (->>
           (dofor [sid storm-ids]
                  (let [recorded-version (:version (get assignment-versions sid))]
                    (if-let [assignment-version (.assignment-version storm-cluster-state sid callback)]
                      (if (= assignment-version recorded-version)
                        {sid (get assignment-versions sid)}
                        {sid (.assignment-info-with-version storm-cluster-state sid callback)})
                      {sid nil})))
           (apply merge)
           (filter-val not-nil?))]

      {:assignments (into {} (for [[k v] new-assignments] [k (:data v)]))
       :versions new-assignments})))

(defn- read-my-executors [assignments-snapshot storm-id assignment-id]
  (let [assignment (get assignments-snapshot storm-id)
        my-executors (filter (fn [[_ [node _]]] (= node assignment-id))
                           (:executor->node+port assignment))
        port-executors (apply merge-with
                          concat
                          (for [[executor [_ port]] my-executors]
                            {port [executor]}
                            ))]
    (into {} (for [[port executors] port-executors]
               ;; need to cast to int b/c it might be a long (due to how yaml parses things)
               ;; doall is to avoid serialization/deserialization problems with lazy seqs
               [(Integer. port) (mk-local-assignment storm-id (doall executors))]
               ))))

(defn- read-assignments
  "Returns map from port to struct containing :storm-id and :executors"
  ([assignments-snapshot assignment-id]
     (->> (dofor [sid (keys assignments-snapshot)] (read-my-executors assignments-snapshot sid assignment-id))
          (apply merge-with (fn [& ignored] (throw-runtime "Should not have multiple topologies assigned to one port")))))
  ([assignments-snapshot assignment-id existing-assignment retries]
     (try (let [assignments (read-assignments assignments-snapshot assignment-id)]
            (reset! retries 0)
            assignments)
          (catch RuntimeException e
            (if (> @retries 2) (throw e) (swap! retries inc))
            (log-warn (.getMessage e) ": retrying " @retries " of 3")
            existing-assignment))))

(defn- read-storm-code-locations
  [assignments-snapshot]
  (map-val :master-code-dir assignments-snapshot))

(defn- read-downloaded-storm-ids [conf]
  (map #(url-decode %) (read-dir-contents (supervisor-stormdist-root conf)))
  )

(defn read-worker-heartbeat [conf id]
  (let [local-state (worker-state conf id)]
    (try
      (ls-worker-heartbeat local-state)
      (catch Exception e
        (log-warn e "Failed to read local heartbeat for workerId : " id ",Ignoring exception.")
        nil))))


(defn my-worker-ids [conf]
  (read-dir-contents (worker-root conf)))

(defn read-worker-heartbeats
  "Returns map from worker id to heartbeat"
  [conf]
  (let [ids (my-worker-ids conf)]
    (into {}
      (dofor [id ids]
        [id (read-worker-heartbeat conf id)]))
    ))


(defn matches-an-assignment? [worker-heartbeat assigned-executors]
  (let [local-assignment (assigned-executors (:port worker-heartbeat))]
    (and local-assignment
         (= (:storm-id worker-heartbeat) (:storm-id local-assignment))
         (= (disj (set (:executors worker-heartbeat)) Constants/SYSTEM_EXECUTOR_ID)
            (set (:executors local-assignment))))))

(let [dead-workers (atom #{})]
  (defn get-dead-workers []
    @dead-workers)
  (defn add-dead-worker [worker]
    (swap! dead-workers conj worker))
  (defn remove-dead-worker [worker]
    (swap! dead-workers disj worker)))

(defn is-worker-hb-timed-out? [now hb conf]
  (> (- now (:time-secs hb))
     (conf SUPERVISOR-WORKER-TIMEOUT-SECS)))

(defn read-allocated-workers
  "Returns map from worker id to worker heartbeat. if the heartbeat is nil, then the worker is dead (timed out or never wrote heartbeat)"
  [supervisor assigned-executors now]
  (let [conf (:conf supervisor)
        ^LocalState local-state (:local-state supervisor)
        id->heartbeat (read-worker-heartbeats conf)
        approved-ids (set (keys (ls-approved-workers local-state)))]
    (into
     {}
     (dofor [[id hb] id->heartbeat]
            (let [state (cond
                         (not hb)
                           :not-started
                         (or (not (contains? approved-ids id))
                             (not (matches-an-assignment? hb assigned-executors)))
                           :disallowed
                         (or
                          (when (get (get-dead-workers) id)
                            (log-message "Worker Process " id " has died!")
                            true)
                          (is-worker-hb-timed-out? now hb conf))
                           :timed-out
                         true
                           :valid)]
              (log-debug "Worker " id " is " state ": " (pr-str hb) " at supervisor time-secs " now)
              [id [state hb]]
              ))
     )))

(defn- wait-for-worker-launch [conf id start-time]
  (let [state (worker-state conf id)]
    (loop []
      (let [hb (ls-worker-heartbeat state)]
        (when (and
               (not hb)
               (<
                (- (current-time-secs) start-time)
                (conf SUPERVISOR-WORKER-START-TIMEOUT-SECS)
                ))
          (log-message id " still hasn't started")
          (Time/sleep 500)
          (recur)
          )))
    (when-not (ls-worker-heartbeat state)
      (log-message "Worker " id " failed to start")
      )))

(defn- wait-for-workers-launch [conf ids]
  (let [start-time (current-time-secs)]
    (doseq [id ids]
      (wait-for-worker-launch conf id start-time))
    ))

(defn generate-supervisor-id []
  (uuid))

(defnk worker-launcher [conf user args :environment {} :log-prefix nil :exit-code-callback nil :directory nil]
  (let [_ (when (clojure.string/blank? user)
            (throw (java.lang.IllegalArgumentException.
                     "User cannot be blank when calling worker-launcher.")))
        wl-initial (conf SUPERVISOR-WORKER-LAUNCHER)
        storm-home (System/getProperty "storm.home")
        wl (if wl-initial wl-initial (str storm-home "/bin/worker-launcher"))
        command (concat [wl user] args)]
    (log-message "Running as user:" user " command:" (pr-str command))
    (launch-process command :environment environment :log-prefix log-prefix :exit-code-callback exit-code-callback :directory directory)
  ))

(defnk worker-launcher-and-wait [conf user args :environment {} :log-prefix nil]
  (let [process (worker-launcher conf user args :environment environment)]
    (if log-prefix
      (read-and-log-stream log-prefix (.getInputStream process)))
      (try
        (.waitFor process)
      (catch InterruptedException e
        (log-message log-prefix " interrupted.")))
      (.exitValue process)))

(defn- rmr-as-user
  "Launches a process owned by the given user that deletes the given path
  recursively.  Throws RuntimeException if the directory is not removed."
  [conf id user path]
  (worker-launcher-and-wait conf
                            user
                            ["rmr" path]
                            :log-prefix (str "rmr " id))
  (if (exists-file? path)
    (throw (RuntimeException. (str path " was not deleted")))))

(defn try-cleanup-worker [conf id user]
  (try
    (if (.exists (File. (worker-root conf id)))
      (do
        (if (conf SUPERVISOR-RUN-WORKER-AS-USER)
          (rmr-as-user conf id user (worker-root conf id))
          (do
            (rmr (worker-heartbeats-root conf id))
            ;; this avoids a race condition with worker or subprocess writing pid around same time
            (rmpath (worker-pids-root conf id))
            (rmpath (worker-root conf id))))
        (remove-worker-user! conf id)
        (remove-dead-worker id)
      ))
  (catch IOException e
    (log-warn-error e "Failed to cleanup worker " id ". Will retry later"))
  (catch RuntimeException e
    (log-warn-error e "Failed to cleanup worker " id ". Will retry later")
    )
  (catch java.io.FileNotFoundException e (log-message (.getMessage e)))
    ))

(defn shutdown-worker [supervisor id]
  (log-message "Shutting down " (:supervisor-id supervisor) ":" id)
  (let [conf (:conf supervisor)
        pids (read-dir-contents (worker-pids-root conf id))
        thread-pid (@(:worker-thread-pids-atom supervisor) id)
        shutdown-sleep-secs (conf SUPERVISOR-WORKER-SHUTDOWN-SLEEP-SECS)
        as-user (conf SUPERVISOR-RUN-WORKER-AS-USER)
        user (get-worker-user conf id)]
    (when thread-pid
      (psim/kill-process thread-pid))
    (doseq [pid pids]
      (if as-user
        (worker-launcher-and-wait conf user ["signal" pid "15"] :log-prefix (str "kill -15 " pid))
        (kill-process-with-sig-term pid)))
    (when-not (empty? pids)  
      (log-message "Sleep " shutdown-sleep-secs " seconds for execution of cleanup threads on worker.")
      (sleep-secs shutdown-sleep-secs))
    (doseq [pid pids]
      (if as-user
        (worker-launcher-and-wait conf user ["signal" pid "9"] :log-prefix (str "kill -9 " pid))
        (force-kill-process pid))
      (if as-user
        (rmr-as-user conf id user (worker-pid-path conf id pid))
        (try
          (rmpath (worker-pid-path conf id pid))
          (catch Exception e)))) ;; on windows, the supervisor may still holds the lock on the worker directory
    (try-cleanup-worker conf id user))
  (log-message "Shut down " (:supervisor-id supervisor) ":" id))

(def SUPERVISOR-ZK-ACLS
  [(first ZooDefs$Ids/CREATOR_ALL_ACL)
   (ACL. (bit-or ZooDefs$Perms/READ ZooDefs$Perms/CREATE) ZooDefs$Ids/ANYONE_ID_UNSAFE)])

(defn supervisor-data [conf shared-context ^ISupervisor isupervisor]
  {:conf conf
   :shared-context shared-context
   :isupervisor isupervisor
   :active (atom true)
   :uptime (uptime-computer)
   :version (str (VersionInfo/getVersion))
   :worker-thread-pids-atom (atom {})
   :storm-cluster-state (cluster/mk-storm-cluster-state conf :acls (when
                                                                     (Utils/isZkAuthenticationConfiguredStormServer
                                                                       conf)
                                                                     SUPERVISOR-ZK-ACLS))
   :local-state (supervisor-state conf)
   :supervisor-id (.getSupervisorId isupervisor)
   :assignment-id (.getAssignmentId isupervisor)
   :my-hostname (hostname conf)
   :curr-assignment (atom nil) ;; used for reporting used ports when heartbeating
   :heartbeat-timer (mk-timer :kill-fn (fn [t]
                               (log-error t "Error when processing event")
                               (exit-process! 20 "Error when processing an event")
                               ))
   :event-timer (mk-timer :kill-fn (fn [t]
                                         (log-error t "Error when processing event")
                                         (exit-process! 20 "Error when processing an event")
                                         ))
   :assignment-versions (atom {})
   :sync-retry (atom 0)
   :download-lock (Object.)
   })

(defn sync-processes [supervisor]
  (let [conf (:conf supervisor)
        download-lock (:download-lock supervisor)
        ^LocalState local-state (:local-state supervisor)
        storm-cluster-state (:storm-cluster-state supervisor)
        assigned-executors (defaulted (ls-local-assignments local-state) {})
        now (current-time-secs)
        allocated (read-allocated-workers supervisor assigned-executors now)
        keepers (filter-val
                 (fn [[state _]] (= state :valid))
                 allocated)
        keep-ports (set (for [[id [_ hb]] keepers] (:port hb)))
        reassign-executors (select-keys-pred (complement keep-ports) assigned-executors)
        new-worker-ids (into
                        {}
                        (for [port (keys reassign-executors)]
                          [port (uuid)]))
        ]
    ;; 1. to kill are those in allocated that are dead or disallowed
    ;; 2. kill the ones that should be dead
    ;;     - read pids, kill -9 and individually remove file
    ;;     - rmr heartbeat dir, rmdir pid dir, rmdir id dir (catch exception and log)
    ;; 3. of the rest, figure out what assignments aren't yet satisfied
    ;; 4. generate new worker ids, write new "approved workers" to LS
    ;; 5. create local dir for worker id
    ;; 5. launch new workers (give worker-id, port, and supervisor-id)
    ;; 6. wait for workers launch

    (log-debug "Syncing processes")
    (log-debug "Assigned executors: " assigned-executors)
    (log-debug "Allocated: " allocated)
    (doseq [[id [state heartbeat]] allocated]
      (when (not= :valid state)
        (log-message
         "Shutting down and clearing state for id " id
         ". Current supervisor time: " now
         ". State: " state
         ", Heartbeat: " (pr-str heartbeat))
        (shutdown-worker supervisor id)
        ))
    (doseq [id (vals new-worker-ids)]
      (local-mkdirs (worker-pids-root conf id))
      (local-mkdirs (worker-heartbeats-root conf id)))
    (ls-approved-workers! local-state
          (merge
           (select-keys (ls-approved-workers local-state)
                        (keys keepers))
           (zipmap (vals new-worker-ids) (keys new-worker-ids))
           ))

    ;; check storm topology code dir exists before launching workers
    (doseq [[port assignment] reassign-executors]
      (let [downloaded-storm-ids (set (read-downloaded-storm-ids conf))
            storm-id (:storm-id assignment)
            cached-assignment-info @(:assignment-versions supervisor)
            assignment-info (if (and (not-nil? cached-assignment-info) (contains? cached-assignment-info storm-id ))
                              (get cached-assignment-info storm-id)
                              (.assignment-info-with-version storm-cluster-state storm-id nil))
	    storm-code-map (read-storm-code-locations assignment-info)
            master-code-dir (if (contains? storm-code-map :data) (storm-code-map :data))
            stormroot (supervisor-stormdist-root conf storm-id)]
        (if-not (or (contains? downloaded-storm-ids storm-id) (.exists (File. stormroot)) (nil? master-code-dir))
          (download-storm-code conf storm-id master-code-dir download-lock))
        ))

    (wait-for-workers-launch
     conf
     (dofor [[port assignment] reassign-executors]
            (let [id (new-worker-ids port)]
              (try
                (log-message "Launching worker with assignment "
                             (pr-str assignment)
                             " for this supervisor "
                             (:supervisor-id supervisor)
                             " on port "
                             port
                             " with id "
                             id
                             )
                (launch-worker supervisor
                               (:storm-id assignment)
                               port
                               id)
                (catch java.io.FileNotFoundException e
                  (log-message "Unable to launch worker due to "
                               (.getMessage e)))
                (catch java.io.IOException e
                  (log-message "Unable to launch worker due to "
                               (.getMessage e))))
         id)))
    ))

(defn assigned-storm-ids-from-port-assignments [assignment]
  (->> assignment
       vals
       (map :storm-id)
       set))

(defn shutdown-disallowed-workers [supervisor]
  (let [conf (:conf supervisor)
        ^LocalState local-state (:local-state supervisor)
        assigned-executors (defaulted (ls-local-assignments local-state) {})
        now (current-time-secs)
        allocated (read-allocated-workers supervisor assigned-executors now)
        disallowed (keys (filter-val
                                  (fn [[state _]] (= state :disallowed))
                                  allocated))]
    (log-debug "Allocated workers " allocated)
    (log-debug "Disallowed workers " disallowed)
    (doseq [id disallowed]
      (shutdown-worker supervisor id))
    ))

(defn mk-synchronize-supervisor [supervisor sync-processes event-manager processes-event-manager]
  (fn this []
    (let [conf (:conf supervisor)
          download-lock (:download-lock supervisor)
          storm-cluster-state (:storm-cluster-state supervisor)
          ^ISupervisor isupervisor (:isupervisor supervisor)
          ^LocalState local-state (:local-state supervisor)
          sync-callback (fn [& ignored] (.add event-manager this))
          assignment-versions @(:assignment-versions supervisor)
          {assignments-snapshot :assignments versions :versions}  (assignments-snapshot
                                                                   storm-cluster-state sync-callback
                                                                  assignment-versions)
          storm-code-map (read-storm-code-locations assignments-snapshot)
          downloaded-storm-ids (set (read-downloaded-storm-ids conf))
          existing-assignment (ls-local-assignments local-state)
          all-assignment (read-assignments assignments-snapshot
                                           (:assignment-id supervisor)
                                           existing-assignment
                                           (:sync-retry supervisor))
          new-assignment (->> all-assignment
                              (filter-key #(.confirmAssigned isupervisor %)))
          assigned-storm-ids (assigned-storm-ids-from-port-assignments new-assignment)
          ]
      (log-debug "Synchronizing supervisor")
      (log-debug "Storm code map: " storm-code-map)
      (log-debug "Downloaded storm ids: " downloaded-storm-ids)
      (log-debug "All assignment: " all-assignment)
      (log-debug "New assignment: " new-assignment)

      ;; download code first
      ;; This might take awhile
      ;;   - should this be done separately from usual monitoring?
      ;; should we only download when topology is assigned to this supervisor?
      (doseq [[storm-id master-code-dir] storm-code-map]
        (when (and (not (downloaded-storm-ids storm-id))
                   (assigned-storm-ids storm-id))
          (download-storm-code conf storm-id master-code-dir download-lock)))

      (log-debug "Writing new assignment "
                 (pr-str new-assignment))
      (doseq [p (set/difference (set (keys existing-assignment))
                                (set (keys new-assignment)))]
        (.killedWorker isupervisor (int p)))
      (.assigned isupervisor (keys new-assignment))
      (ls-local-assignments! local-state
            new-assignment)
      (reset! (:assignment-versions supervisor) versions)
      (reset! (:curr-assignment supervisor) new-assignment)
      ;; remove any downloaded code that's no longer assigned or active
      ;; important that this happens after setting the local assignment so that
      ;; synchronize-supervisor doesn't try to launch workers for which the
      ;; resources don't exist
      (if on-windows? (shutdown-disallowed-workers supervisor))
      (doseq [storm-id downloaded-storm-ids]
        (when-not (assigned-storm-ids storm-id)
          (log-message "Removing code for storm id "
                       storm-id)
          (try
            (rmr (supervisor-stormdist-root conf storm-id))
            (catch Exception e (log-message (.getMessage e))))
          ))
      (.add processes-event-manager sync-processes)
      )))

;; in local state, supervisor stores who its current assignments are
;; another thread launches events to restart any dead processes if necessary
(defserverfn mk-supervisor [conf shared-context ^ISupervisor isupervisor]
  (log-message "Starting Supervisor with conf " conf)
  (.prepare isupervisor conf (supervisor-isupervisor-dir conf))
  (FileUtils/cleanDirectory (File. (supervisor-tmp-dir conf)))
  (let [supervisor (supervisor-data conf shared-context isupervisor)
        [event-manager processes-event-manager :as managers] [(event/event-manager false) (event/event-manager false)]
        sync-processes (partial sync-processes supervisor)
        synchronize-supervisor (mk-synchronize-supervisor supervisor sync-processes event-manager processes-event-manager)
        heartbeat-fn (fn [] (.supervisor-heartbeat!
                               (:storm-cluster-state supervisor)
                               (:supervisor-id supervisor)
                               (->SupervisorInfo (current-time-secs)
                                                 (:my-hostname supervisor)
                                                 (:assignment-id supervisor)
                                                 (keys @(:curr-assignment supervisor))
                                                 ;; used ports
                                                 (.getMetadata isupervisor)
                                                 (conf SUPERVISOR-SCHEDULER-META)
                                                 ((:uptime supervisor))
                                                 (:version supervisor))))]
    (heartbeat-fn)

    ;; should synchronize supervisor so it doesn't launch anything after being down (optimization)
    (schedule-recurring (:heartbeat-timer supervisor)
                        0
                        (conf SUPERVISOR-HEARTBEAT-FREQUENCY-SECS)
                        heartbeat-fn)
    (when (conf SUPERVISOR-ENABLE)
      ;; This isn't strictly necessary, but it doesn't hurt and ensures that the machine stays up
      ;; to date even if callbacks don't all work exactly right
      (schedule-recurring (:event-timer supervisor) 0 10 (fn [] (.add event-manager synchronize-supervisor)))
      (schedule-recurring (:event-timer supervisor)
                          0
                          (conf SUPERVISOR-MONITOR-FREQUENCY-SECS)
                          (fn [] (.add processes-event-manager sync-processes))))
    (log-message "Starting supervisor with id " (:supervisor-id supervisor) " at host " (:my-hostname supervisor))
    (reify
     Shutdownable
     (shutdown [this]
               (log-message "Shutting down supervisor " (:supervisor-id supervisor))
               (reset! (:active supervisor) false)
               (cancel-timer (:heartbeat-timer supervisor))
               (cancel-timer (:event-timer supervisor))
               (.shutdown event-manager)
               (.shutdown processes-event-manager)
               (.disconnect (:storm-cluster-state supervisor)))
     SupervisorDaemon
     (get-conf [this]
       conf)
     (get-id [this]
       (:supervisor-id supervisor))
     (shutdown-all-workers [this]
       (let [ids (my-worker-ids conf)]
         (doseq [id ids]
           (shutdown-worker supervisor id)
           )))
     DaemonCommon
     (waiting? [this]
       (or (not @(:active supervisor))
           (and
            (timer-waiting? (:heartbeat-timer supervisor))
            (timer-waiting? (:event-timer supervisor))
            (every? (memfn waiting?) managers)))
           ))))

(defn kill-supervisor [supervisor]
  (.shutdown supervisor)
  )

(defn setup-storm-code-dir [conf storm-conf dir]
 (if (conf SUPERVISOR-RUN-WORKER-AS-USER)
  (worker-launcher-and-wait conf (storm-conf TOPOLOGY-SUBMITTER-USER) ["code-dir" dir] :log-prefix (str "setup conf for " dir))))

;; distributed implementation
(defmethod download-storm-code
    :distributed [conf storm-id master-code-dir download-lock]
    ;; Downloading to permanent location is atomic
    (let [tmproot (str (supervisor-tmp-dir conf) file-path-separator (uuid))
          stormroot (supervisor-stormdist-root conf storm-id)]
      (locking download-lock
            (log-message "Downloading code for storm id "
                         storm-id
                         " from "
                         master-code-dir)
            (FileUtils/forceMkdir (File. tmproot))

            (Utils/downloadFromMaster conf (master-stormjar-path master-code-dir) (supervisor-stormjar-path tmproot))
            (Utils/downloadFromMaster conf (master-stormcode-path master-code-dir) (supervisor-stormcode-path tmproot))
            (Utils/downloadFromMaster conf (master-stormconf-path master-code-dir) (supervisor-stormconf-path tmproot))
            (extract-dir-from-jar (supervisor-stormjar-path tmproot) RESOURCES-SUBDIR tmproot)
            (if-not (.exists (File. stormroot))
              (FileUtils/moveDirectory (File. tmproot) (File. stormroot))
              (FileUtils/deleteDirectory (File. tmproot)))
            (setup-storm-code-dir conf (read-supervisor-storm-conf conf storm-id) stormroot)
            (log-message "Finished downloading code for storm id "
                         storm-id
                         " from "
                         master-code-dir))
      ))

(defn write-log-metadata-to-yaml-file! [storm-id port data conf]
  (let [file (get-log-metadata-file storm-id port)]
    ;;run worker as user needs the directory to have special permissions
    ;; or it is insecure
    (when (not (.exists (.getParentFile file)))
      (if (conf SUPERVISOR-RUN-WORKER-AS-USER)
        (do (FileUtils/forceMkdir (.getParentFile file))
            (setup-storm-code-dir conf (read-supervisor-storm-conf conf storm-id) (.getCanonicalPath (.getParentFile file))))
        (.mkdirs (.getParentFile file))))
    (let [writer (java.io.FileWriter. file)
        yaml (Yaml.)]
      (try
        (.dump yaml data writer)
        (finally
          (.close writer))))))

(defn write-log-metadata! [storm-conf user worker-id storm-id port conf]
  (let [data {TOPOLOGY-SUBMITTER-USER user
              "worker-id" worker-id
              LOGS-GROUPS (sort (distinct (remove nil?
                                           (concat
                                             (storm-conf LOGS-GROUPS)
                                             (storm-conf TOPOLOGY-GROUPS)))))
              LOGS-USERS (sort (distinct (remove nil?
                                           (concat
                                             (storm-conf LOGS-USERS)
                                             (storm-conf TOPOLOGY-USERS)))))}]
    (write-log-metadata-to-yaml-file! storm-id port data conf)))

(defn jlp [stormroot conf]
  (let [resource-root (str stormroot File/separator RESOURCES-SUBDIR)
        os (clojure.string/replace (System/getProperty "os.name") #"\s+" "_")
        arch (System/getProperty "os.arch")
        arch-resource-root (str resource-root File/separator os "-" arch)]
    (str arch-resource-root File/pathSeparator resource-root File/pathSeparator (conf JAVA-LIBRARY-PATH))))

(defn substitute-childopts
  "Generates runtime childopts by replacing keys with topology-id, worker-id, port"
  [value worker-id topology-id port]
  (let [replacement-map {"%ID%"          (str port)
                         "%WORKER-ID%"   (str worker-id)
                         "%TOPOLOGY-ID%"    (str topology-id)
                         "%WORKER-PORT%" (str port)}
        sub-fn #(reduce (fn [string entry]
                          (apply clojure.string/replace string entry))
                        %
                        replacement-map)]
    (cond
      (nil? value) nil
      (sequential? value) (vec (map sub-fn value))
      :else (-> value sub-fn (clojure.string/split #"\s+")))))

(defn java-cmd []
  (let [java-home (.get (System/getenv) "JAVA_HOME")]
    (if (nil? java-home)
      "java"
      (str java-home file-path-separator "bin" file-path-separator "java")
      )))


(defmethod launch-worker
    :distributed [supervisor storm-id port worker-id]
    (let [conf (:conf supervisor)
          run-worker-as-user (conf SUPERVISOR-RUN-WORKER-AS-USER)
          storm-home (System/getProperty "storm.home")
          storm-options (System/getProperty "storm.options")
          storm-conf-file (System/getProperty "storm.conf.file")
          storm-log-dir (or (System/getProperty "storm.log.dir") (str storm-home file-path-separator "logs"))
          storm-conf (read-storm-config)
          storm-log-conf-dir (storm-conf "storm.logback.conf.dir")
          storm-logback-conf-dir (or storm-log-conf-dir (str storm-home file-path-separator "log4j2"))
          stormroot (supervisor-stormdist-root conf storm-id)
          jlp (jlp stormroot conf)
          stormjar (supervisor-stormjar-path stormroot)
          storm-conf (read-supervisor-storm-conf conf storm-id)
          topo-classpath (if-let [cp (storm-conf TOPOLOGY-CLASSPATH)]
                           [cp]
                           [])
          classpath (-> (worker-classpath)
                        (add-to-classpath [stormjar])
                        (add-to-classpath topo-classpath))
          top-gc-opts (storm-conf TOPOLOGY-WORKER-GC-CHILDOPTS)
          gc-opts (substitute-childopts (if top-gc-opts top-gc-opts (conf WORKER-GC-CHILDOPTS)) worker-id storm-id port)
          user (storm-conf TOPOLOGY-SUBMITTER-USER)
          logging-sensitivity (storm-conf TOPOLOGY-LOGGING-SENSITIVITY "S3")
          logfilename (logs-filename storm-id port)
          worker-childopts (when-let [s (conf WORKER-CHILDOPTS)]
                             (substitute-childopts s worker-id storm-id port))
          topo-worker-childopts (when-let [s (storm-conf TOPOLOGY-WORKER-CHILDOPTS)]
                                  (substitute-childopts s worker-id storm-id port))
          topology-worker-environment (if-let [env (storm-conf TOPOLOGY-ENVIRONMENT)]
                                        (merge env {"LD_LIBRARY_PATH" jlp})
                                        {"LD_LIBRARY_PATH" jlp})
          command (concat
                    [(java-cmd) "-cp" classpath 
                     (str "-Dlogfile.name=" logfilename)
                     (str "-Dstorm.home=" storm-home)
                     (str "-Dstorm.id=" storm-id)
                     (str "-Dworker.id=" worker-id)
                     (str "-Dworker.port=" port)
                     (str "-Dstorm.log.dir=" storm-log-dir)
                     (str "-Dlog4j.configurationFile=" storm-logback-conf-dir file-path-separator "worker.xml")
                     "backtype.storm.LogWriter"]
                    [(java-cmd) "-server"]
                    worker-childopts
                    topo-worker-childopts
                    gc-opts
                    [(str "-Djava.library.path=" jlp)
                     (str "-Dlogfile.name=" logfilename)
                     (str "-Dstorm.home=" storm-home)
                     (str "-Dstorm.conf.file=" storm-conf-file)
                     (str "-Dstorm.options=" storm-options)
                     (str "-Dstorm.log.dir=" storm-log-dir)
                     (str "-Dlogging.sensitivity=" logging-sensitivity)
                     (str "-Dlog4j.configurationFile=" storm-logback-conf-dir file-path-separator "worker.xml")
                     (str "-Dstorm.id=" storm-id)
                     (str "-Dworker.id=" worker-id)
                     (str "-Dworker.port=" port)
                     "-cp" classpath
                     "backtype.storm.daemon.worker"
                     storm-id
                     (:assignment-id supervisor)
                     port
                     worker-id])
          command (->> command (map str) (filter (complement empty?)))]
      (log-message "Launching worker with command: " (shell-cmd command))
      (write-log-metadata! storm-conf user worker-id storm-id port conf)
      (set-worker-user! conf worker-id user)
      (let [log-prefix (str "Worker Process " worker-id)
            callback (fn [exit-code]
                       (log-message log-prefix " exited with code: " exit-code)
                       (add-dead-worker worker-id))
            worker-dir (worker-root conf worker-id)]
        (remove-dead-worker worker-id)
        (if run-worker-as-user
          (worker-launcher conf user ["worker" worker-dir (write-script worker-dir command :environment topology-worker-environment)] :log-prefix log-prefix :exit-code-callback callback :directory (File. worker-dir))
          (launch-process command :environment topology-worker-environment :log-prefix log-prefix :exit-code-callback callback :directory (File. worker-dir)))
        )))

;; local implementation

(defn resources-jar []
  (->> (.split (current-classpath) File/pathSeparator)
       (filter #(.endsWith  % ".jar"))
       (filter #(zip-contains-dir? % RESOURCES-SUBDIR))
       first ))

(defmethod download-storm-code
    :local [conf storm-id master-code-dir download-lock]
    (let [stormroot (supervisor-stormdist-root conf storm-id)]
      (locking download-lock
            (FileUtils/copyDirectory (File. master-code-dir) (File. stormroot))
            (let [classloader (.getContextClassLoader (Thread/currentThread))
                  resources-jar (resources-jar)
                  url (.getResource classloader RESOURCES-SUBDIR)
                  target-dir (str stormroot file-path-separator RESOURCES-SUBDIR)]
              (cond
               resources-jar
               (do
                 (log-message "Extracting resources from jar at " resources-jar " to " target-dir)
                 (extract-dir-from-jar resources-jar RESOURCES-SUBDIR stormroot))
               url
               (do
                 (log-message "Copying resources at " (URI. (str url)) " to " target-dir)
                 (if (= (.getProtocol url) "jar" )
                   (extract-dir-from-jar (.getFile (.getJarFileURL (.openConnection url))) RESOURCES-SUBDIR stormroot)
                   (FileUtils/copyDirectory (File. (.getPath (URI. (str url)))) (File. target-dir)))
                 )
               )
              )
            )))

(defmethod launch-worker
    :local [supervisor storm-id port worker-id]
    (let [conf (:conf supervisor)
          pid (uuid)
          worker (worker/mk-worker conf
                                   (:shared-context supervisor)
                                   storm-id
                                   (:assignment-id supervisor)
                                   port
                                   worker-id)]
      (set-worker-user! conf worker-id "")
      (psim/register-process pid worker)
      (swap! (:worker-thread-pids-atom supervisor) assoc worker-id pid)
      ))

(defn -launch [supervisor]
  (let [conf (read-storm-config)]
    (validate-distributed-mode! conf)
    (let [supervisor (mk-supervisor conf nil supervisor)]
      (add-shutdown-hook-with-force-kill-in-1-sec #(.shutdown supervisor)))))

(defn standalone-supervisor []
  (let [conf-atom (atom nil)
        id-atom (atom nil)]
    (reify ISupervisor
      (prepare [this conf local-dir]
        (reset! conf-atom conf)
        (let [state (LocalState. local-dir)
              curr-id (if-let [id (ls-supervisor-id state)]
                        id
                        (generate-supervisor-id))]
          (ls-supervisor-id! state curr-id)
          (reset! id-atom curr-id))
        )
      (confirmAssigned [this port]
        true)
      (getMetadata [this]
        (doall (map int (get @conf-atom SUPERVISOR-SLOTS-PORTS))))
      (getSupervisorId [this]
        @id-atom)
      (getAssignmentId [this]
        @id-atom)
      (killedWorker [this port]
        )
      (assigned [this ports]
        ))))

(defn -main []
  (setup-default-uncaught-exception-handler)
  (-launch (standalone-supervisor)))

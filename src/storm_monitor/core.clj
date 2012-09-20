;;
;;A storm monitor to check if storm is running well.
;;Author: boyan (boyan@taobao.com)
;;        zhouchen (zhouchen.zm@taobao.com)
;;
(ns storm-monitor.core
  (:use [backtype.storm cluster config log]
        [backtype.storm.zookeeper :only [mk-client]]
        [storm-monitor.alarm :only [alarm]])
  (:require [backtype.storm [zookeeper :as zk]]
            [storm-monitor [conf :as mc]])
  (:import (java.net Socket))
  (:require tron)
  (:use [clojure.contrib.string :only [join substring?]]))


(def SUPS "supervisors")
(def TOPS "topologies")
(def CHECK-NIMBUS "nimbus.check")

;;Check if supervisors are all alive
(defn- check-supervisors [sconf mconf zkc]
  (try
    (let [sups   (zk/get-children
                  zkc
                  SUPERVISORS-SUBTREE
                  false)
          expect (mconf SUPS)
          count  (count sups)]
      (if (not= count expect)
        (str (- expect count) " Supervisors are down!")))
    (catch Throwable e
      (log-error e "Failed to check supervisors"))))

;;Check if all topologies are all alive
(defn- check-tops [sconf mconf zkc]
  (try
    (let [tops         (mconf TOPS)
          children     (zk/get-children
                        zkc
                        STORMS-SUBTREE
                        false)
          deads        (filter
                        (fn [top]
                          (not-any? #(substring? top %) children))
                        tops)]
      (if (not (empty? deads))
        (str "Topologies [" (join "," deads) "] have been killed")))
    (catch Throwable e
      (log-error e "Failed to check topologies."))))

(defn- get-top-name [tops task]
  (first (filter #(substring? % task) tops)))

;;Check if all topologies have active tasks
(defn- check-tasks [sconf mconf zkc]
  (try
    (let [tops             (mconf TOPS)
          children         (filter
                            (fn [child]
                              (some #(substring? % child) tops))
                            (zk/get-children
                             zkc
                             TASKBEATS-ROOT
                             false))
          child-count-map  (reduce
                            #(assoc %1
                               (get-top-name tops %2)
                               (count
                                (zk/get-children
                                 zkc
                                 (str TASKBEATS-ROOT "/" %2)
                                 false)))
                           {}
                           children)
          top-count-map    (merge
                            (apply hash-map
                                   (interleave tops (repeat 0)))
                            child-count-map)
          empty-task-tops  (keys
                            (filter #(= (second %) 0) top-count-map))]
      (if (not-empty empty-task-tops)
        (str "Topologies ["
             (join "," empty-task-tops)
             "] have no active tasks")))
    (catch Throwable e
      (log-error e "Failed to check tasks"))))

(defn- check-nimbus [sconf mconf]
  (if (mconf CHECK-NIMBUS)
    ;;try to connect to nimbus
    (let [nimbus-host (sconf NIMBUS-HOST)
          nimbus-port (int (sconf NIMBUS-THRIFT-PORT))
          error (str "Could not connect to nimbus "
                     nimbus-host ":" nimbus-port)]
      (try
        (let [socket (Socket. nimbus-host nimbus-port)]
          (try
            (if (not (.isConnected socket))
              error)
            (finally
             (.close socket))))
        (catch Throwable e
          (do (log-error e "Failed to check nimbus")
              (str error ",exception:"
                   (class e)  ",msg:"
                   (.getMessage e)
                   )))))))

;;Check if the storm is running fine
(defn- check-storm []
  (let
      [sconf (read-storm-config)
       mconf (mc/read-config)
       zkc   (zk/mk-client
              sconf
              (sconf STORM-ZOOKEEPER-SERVERS)
              (sconf STORM-ZOOKEEPER-PORT)
              :auth-conf sconf
              :root (sconf STORM-ZOOKEEPER-ROOT))
       msgs  (filter #(not (nil? %))
                    [(check-supervisors sconf mconf zkc)
                     (check-tops sconf mconf zkc)
                     (check-tasks sconf mconf zkc)
                     (check-nimbus sconf mconf)])]
    (if (not-empty msgs)
      (dorun (map  #(alarm mconf %) msgs))
      (log-message "Checking is done at " (java.util.Date.)))
     (.close zkc)))

;;check the health of storm every minutes
(tron/periodically :check check-storm 60000)

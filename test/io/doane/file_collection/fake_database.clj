(ns io.doane.file-collection.fake-database
  (:require
    [clojure.edn]
    [clojure.java.io :as io]
    [clojure.string :as str]
    [clojure.test :refer :all]
    [io.doane.file-collection.data :as data])
  (:import (java.io RandomAccessFile)))

;;; This should probably be an environment variable or something.
(def testing-path "/tmp/fc-test")
(def testing-dir-file (io/file testing-path))

;;; Tests can't run if the testing dir doesn't exist since everything reads from and writes to disk.
(when (not (.exists testing-dir-file))
  (.mkdir testing-dir-file))

(def random-first-names (clojure.edn/read-string (slurp "test-data/random-first-names.edn")))
(def random-last-names (clojure.edn/read-string (slurp "test-data/random-last-names.edn")))

(def random-users
  (doall
    (for [first-name random-first-names
          last-name random-last-names]
      {:user/first-name first-name
       :user/last-name last-name
       :user/email (str (str/lower-case first-name) "." (str/lower-case last-name) "@fake.email.com.fk")})))

(defn smith-predicate
  [data]
  (= (:user/last-name data) "Smith"))

(defn create-user-data!
  [^RandomAccessFile data-raf]
  (data/write-collection! data-raf random-users))

(comment

  (require '[taoensso.nippy :as nippy])

  (time (def rval (nippy/freeze random-users)))

  (time (def rval2 (nippy/thaw rval)))

  )
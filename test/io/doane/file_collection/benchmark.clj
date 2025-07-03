(ns io.doane.file-collection.benchmark
  (:import
    [java.io RandomAccessFile])
  (:require
    [clojure.java.io :as io]
    [criterium.core :as c]
    [io.doane.file-collection.fake-database :as fake-database]
    [io.doane.file-collection.data :as data]
    [io.doane.file-collection.index :as index]
    [io.doane.file-collection.utils :refer [create-random-access-file]]))

(defn create-users-and-clear!
  [^RandomAccessFile data-raf]
  (.setLength data-raf 0)
  (fake-database/create-user-data! data-raf)
  (.setLength data-raf 0))

(defn create-smith-index-and-clear!
  [^RandomAccessFile data-raf ^RandomAccessFile index-raf]
  (.setLength index-raf 0)
  (index/advance-index! data-raf index-raf fake-database/smith-predicate)
  (.setLength index-raf 0))

(defn filter-on-smith-predicate
  [^RandomAccessFile user-raf]
  (doall (filter fake-database/smith-predicate (data/to-collection user-raf))))

(defn index-pull-smith-predicate
  [^RandomAccessFile user-raf ^RandomAccessFile index-raf]
  (doall (->> (index/to-raw-index-collection index-raf)
              (index/indexed-collection->data-collection user-raf))))

(defn find-by-email
  [^RandomAccessFile user-raf ^RandomAccessFile email-index-raf email-address]
  (->> (index/to-raw-index-collection email-index-raf)
       (filter #(= (:indexed-value %) email-address))
       (index/indexed-collection->data-collection user-raf)
       (doall)))

(defn index-pull-last-name
  [^RandomAccessFile user-raf ^RandomAccessFile last-index-raf last-name]
  (->> (index/to-raw-index-collection last-index-raf)
       (filter #(= (:indexed-value %) last-name))
       (index/indexed-collection->data-collection user-raf)
       (doall)))

(defn run-benchmark!
  [data-path file-sync?]
  (.mkdirs (io/file data-path))
  (let [user-raf        (create-random-access-file (str data-path "/users.fcd") file-sync?)
        smith-index-raf (create-random-access-file (str data-path "/users.smith-index.fci") file-sync?)
        last-index-raf  (create-random-access-file (str data-path "/users.lastname-index.fci") file-sync?)
        email-index-raf (create-random-access-file (str data-path "/users.email-index.fci") file-sync?)]
    ;;; Clear everything.
    (.setLength user-raf 0)
    (.setLength smith-index-raf 0)
    (.setLength email-index-raf 0)
    (.setLength last-index-raf 0)

    (println "Writing 10,000 maps with 3 attributes.")
    (c/bench (create-users-and-clear! user-raf))

    ;;; Once again create the fake user data.
    (fake-database/create-user-data! user-raf)
    (println "Filtering all of the 'Smith' last name users from the RAF.")
    (c/bench (filter-on-smith-predicate user-raf))

    (println "Create an index from the 'Smith' predicate.")
    (c/bench (create-smith-index-and-clear! user-raf smith-index-raf))

    (index/advance-index! user-raf smith-index-raf fake-database/smith-predicate)
    (println "Pull all the 'Smith' users via the Smith index.")
    (c/bench (index-pull-smith-predicate user-raf smith-index-raf))

    (index/advance-index! user-raf last-index-raf :user/last-name)
    (println "Pull all the 'Smith' users via the last name index.")
    (c/bench (index-pull-last-name user-raf last-index-raf "Smith"))

    (index/advance-index! user-raf email-index-raf :user/email)
    (println "Find a particular user by email via an index.")
    (c/bench (find-by-email user-raf email-index-raf "tina.turner@fake.email.com.fk"))))

(comment

  (c/quick-benchmark (+ 1 2 3 4 5) {})

  (run-benchmark! "/tmp/fc-benchmark" false)

  )
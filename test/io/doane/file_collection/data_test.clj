(ns io.doane.file-collection.data-test
  (:require
    [clojure.string :as str]
    [clojure.test :refer :all]
    [clojure.java.io :as io]
    [io.doane.file-collection.data :as data]))

(def random-first-names (clojure.edn/read-string (slurp "test-data/random-first-names.edn")))
(def random-last-names (clojure.edn/read-string (slurp "test-data/random-last-names.edn")))

;;; This should probably be an environment variable or something.
(def testing-path "/tmp/fc-test")
(def testing-dir-file (io/file testing-path))

;;; Tests can't run if the testing dir doesn't exist since everything reads from and writes to disk.
(when (not (.exists testing-dir-file))
  (.mkdir testing-dir-file))

(defn smith-predicate
  [data]
  (= (:user/last-name data) "Smith"))

(deftest basic-user-collection
  (let [user-raf (data/create-random-access-file (str testing-path "/users.fcd") true)
        copy-raf (data/create-random-access-file (str testing-path "/users-copy.fcd") true)]
    ;;; Before starting, let's erase the RAFs.
    (.setLength user-raf 0)
    (.setLength copy-raf 0)
    (testing "Can we add 10,000 users?"
      (doall
        (for [first-name random-first-names
              last-name  random-last-names]
          (data/write-data! user-raf {:user/first-name first-name
                                      :user/last-name  last-name
                                      :user/email      (str (str/lower-case first-name) "." (str/lower-case last-name) "@fake.email.com.fk")})))
      ;;; We just statefully added 10,000 users (100 first names * 100 last names.) To a file.
      (let [data (data/to-collection user-raf)]
        (is (= (count data) 10000) "Are there exactly 10,000 users?")

        (let [smith-users (filter smith-predicate data)]
          (is (= (count smith-users) 100) "Are there exactly 100 users with the last name 'Smith'?"))))
    (testing "Can we drop data?"
      ;;; Remove all the users with the last name Smith. There are 100 of them.
      (data/drop-data! user-raf smith-predicate)
      (let [data (data/to-collection user-raf)
            smith-users (filter smith-predicate data)]
        (is (= (count data) 9900) "There should be 100 less users, now that the Smith users were dropped.")
        (is (= (count smith-users) 0) "There should be no Smith users.")))

    (testing "Can we copy the altered data?"
      (data/copy! user-raf copy-raf)
      (let [src-data (data/to-collection user-raf)
            dest-data (data/to-collection copy-raf)]
        (is (= src-data dest-data) "The copied collection should be the same.")))))

(comment

  (time (basic-user-collection))

  (def user-raf (data/create-random-access-file (str testing-path "/users.fcd") true))

  (time (def rval (doall (data/to-collection user-raf))))

  )
(ns io.doane.file-collection.data-test
  (:require
    [clojure.test :refer :all]
    [io.doane.file-collection.data :as data]
    [io.doane.file-collection.fake-database :as fake-database]
    [io.doane.file-collection.utils :as utils]))

(deftest basic-user-collection
  (let [user-raf (utils/create-random-access-file (str fake-database/testing-path "/users.fcd") true)
        copy-raf (utils/create-random-access-file (str fake-database/testing-path "/users-copy.fcd") true)]
    ;;; Before starting, let's erase the RAFs.
    (.setLength user-raf 0)
    (.setLength copy-raf 0)
    (testing "Can we add 10,000 users?"
      (fake-database/create-user-data! user-raf)
      ;;; We just statefully added 10,000 users (100 first names * 100 last names.) To a file.
      (let [data (data/to-collection user-raf)]
        (is (= (count data) 10000) "Are there exactly 10,000 users?")

        (let [smith-users (filter fake-database/smith-predicate data)]
          (is (= (count smith-users) 100) "Are there exactly 100 users with the last name 'Smith'?"))))
    (testing "Can we drop data?"
      ;;; Remove all the users with the last name Smith. There are 100 of them.
      (data/drop-data! user-raf fake-database/smith-predicate)
      (let [data (data/to-collection user-raf)
            smith-users (filter fake-database/smith-predicate data)]
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
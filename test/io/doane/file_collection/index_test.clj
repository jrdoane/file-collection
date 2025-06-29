(ns io.doane.file-collection.index-test
  (:require [clojure.test :refer :all]
            [io.doane.file-collection.fake-database :as fake-database]
            [io.doane.file-collection.index :as index]
            [io.doane.file-collection.utils :as utils]))

(deftest smith-index
  (let [user-raf        (utils/create-random-access-file
                          (str fake-database/testing-path "/index-users.fcd")
                          true)
        smith-raf (utils/create-random-access-file
                          (str fake-database/testing-path "/index-users.smith-index.fci")
                          true)]
    ;;; Before starting, let's erase the RAFs.
    (.setLength user-raf 0)
    (.setLength smith-raf 0)
    ;;; Create fake users.
    (fake-database/create-user-data! user-raf)
    (testing "Can we index on the 'Smith' predicate?"
      (index/advance-index! user-raf smith-raf fake-database/smith-predicate)
      (let [data-collection (->> (index/to-raw-index-collection smith-raf)
                                 (index/indexed-collection->data-collection user-raf))]
        (is (= (count data-collection) 100) "There should be 100 Smiths.")
        (testing "Every Smith from the Smith predicate index should be a Smith."
          (doseq [{:user/keys [last-name]} data-collection]
            (is (= last-name "Smith") "The last name of data from the Smith index should have the last name of Smith.")))))))

(comment

  (time (smith-index))

  )
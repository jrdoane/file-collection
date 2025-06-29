(ns io.doane.file-collection.index
  (:require
    [io.doane.file-collection.data :as data]
    [taoensso.nippy :as nippy])
  (:import (java.io RandomAccessFile)))

(defn write-index-data-offset!
  [^RandomAccessFile index-raf next-data-offset]
  (locking index-raf
    (doto index-raf
      (.seek 0)
      (.writeLong next-data-offset)))
  nil)

(defn write-index!
  [^RandomAccessFile index-raf data-offset next-data-offset indexed-value]
  (let [frozen-bytes  (nippy/freeze indexed-value)
        frozen-length (count frozen-bytes)]
    (locking index-raf
      ;;; If we've never written to the index before, we need to skip the size
      ;;; of a Long, which is 64-bits/8-bytes.
      (when (= (.length index-raf) 0)
        (.setLength index-raf 8))
      (let [index-offset (.length index-raf)]
        ;;; Like data, we already write to the end of the file.
        (.seek index-raf (.length index-raf))
        ;;; Write the data offset from the data file.
        (.writeLong index-raf data-offset)
        ;;; Write the length of the predicate value's serialized bytes.
        (.writeLong index-raf frozen-length)
        ;;; Write the frozen predicate value.
        (.write index-raf frozen-bytes)
        ;;; Finally, we go back to the very beginning of the index and update
        ;;; the location of the next item that could be indexed.
        (write-index-data-offset! index-raf next-data-offset))))
  nil)

(defn read-index
  [^RandomAccessFile index-raf index-offset]
  (locking index-raf
    (.seek index-raf index-offset)
    (when-not (= (.length index-raf) (.getFilePointer index-raf))
      (let [data-offset   (.readLong index-raf)
            frozen-length (.readLong index-raf)
            frozen-bytes  (byte-array frozen-length)
            indexed-value (do (.read index-raf frozen-bytes)
                              (nippy/thaw frozen-bytes))]
        {:index-offset      index-offset
         :next-index-offset (.getFilePointer index-raf)
         :data-offset       data-offset
         :indexed-value     indexed-value}))))

(defn to-raw-index-collection
  ([index-raf]
   (to-raw-index-collection index-raf 8))
  ([index-raf index-offset]
   (lazy-seq
     (when-let [{:keys [next-index-offset] :as index-data} (read-index index-raf index-offset)]
       (cons index-data (to-raw-index-collection index-raf next-index-offset))))))

(defn resolve-indexed
  [^RandomAccessFile data-raf indexed]
  (:data (data/read-data data-raf (:data-offset indexed))))

(defn indexed-collection->data-collection
  [^RandomAccessFile data-raf indexed-collection]
  (->> indexed-collection
       (map (partial resolve-indexed data-raf))
       (remove #(= % ::data/dropped))))

(defn next-data-offset
  [^RandomAccessFile index-raf]
  (if (zero? (.length index-raf))
    0
    (locking index-raf
      (.seek index-raf 0)
      (.readLong index-raf))))

(defn advance-index!
  [^RandomAccessFile data-raf ^RandomAccessFile index-raf indexing-fn]
  (let [starting-data-offset (next-data-offset index-raf)
        raw-data-collection  (data/to-raw-collection data-raf starting-data-offset)]
    (doseq [{:keys [offset next-offset data]} raw-data-collection]
      (let [indexed-value (indexing-fn data)]
        (if indexed-value
          (write-index! index-raf offset next-offset indexed-value)
          (write-index-data-offset! index-raf next-offset))))))

(comment

  (require '[io.doane.file-collection.utils :refer [create-random-access-file]])

  (def data-raf (create-random-access-file "/tmp/fc-1" true))
  (def index-raf (create-random-access-file "/tmp/fc-1.index3.fci" true))

  (next-data-offset index-raf)

  (advance-index! data-raf index-raf :user/email)

  (->> (to-raw-index-collection index-raf)
       (indexed-collection->data-collection data-raf))

  )
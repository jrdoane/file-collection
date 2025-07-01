(ns io.doane.file-collection.data
  (:require
    [taoensso.nippy :as nippy])
  (:import (java.io RandomAccessFile)))

(defn write-data!
  [^RandomAccessFile raf data]
  (let [frozen-bytes  (nippy/freeze data)
        frozen-length (count frozen-bytes)]
    (locking raf
      (doto raf
        ;;; We always write to the end of the file unless a value is being dropped.
        (.seek (.length raf))
        ;;; This value has not been dropped from the collection. Can mutate.
        (.writeBoolean false)
        ;;; This is the size of the data for the next collection item.
        (.writeLong frozen-length)
        ;;; Write the frozen nippy content.
        (.write frozen-bytes))))
  nil)

(defn write-collection!
  [^RandomAccessFile raf data-collection]
  (doseq [data data-collection]
    (write-data! raf data)))

(defn read-data
  [^RandomAccessFile raf offset]
  (locking raf
    (.seek raf offset)
    (when-not (= (.length raf) (.getFilePointer raf))
      (let [dropped?      (.readBoolean raf)
            frozen-length (.readLong raf)
            frozen-bytes  (byte-array frozen-length)
            data          (if dropped?
                            (do
                              (.seek raf (+ (.getFilePointer raf) frozen-length))
                              ::dropped)
                            (do (.read raf frozen-bytes)
                                (nippy/thaw frozen-bytes)))]
        {:offset      offset
         :next-offset (.getFilePointer raf)
         :data       data}))))

(defn write-drop!
  [^RandomAccessFile raf offset]
  (locking raf
    (.seek raf offset)
    (.writeBoolean raf true)))

(defn drop-data!
  ([^RandomAccessFile raf pred]
   (drop-data! raf pred 0))
  ([^RandomAccessFile raf pred offset]
   (let [{:keys [data offset next-offset]} (read-data raf offset)]
     (when next-offset
       (when (and (not= ::dropped data) (pred data))
         (write-drop! raf offset))
       (recur raf pred next-offset)))))

(defn to-raw-collection
  ([^RandomAccessFile raf]
   (to-raw-collection raf 0))
  ([^RandomAccessFile raf offset]
   (lazy-seq
     (when-let [{:keys [next-offset data] :as data-map} (read-data raf offset)]
       (if (= data ::dropped)
         (to-raw-collection raf next-offset)
         (cons data-map (to-raw-collection raf next-offset)))))))

(defn to-collection
  ([^RandomAccessFile raf]
   (->> (to-raw-collection raf)
        (map :data))))

(defn copy!
  [^RandomAccessFile src-raf ^RandomAccessFile dest-raf]
  (->> (to-collection src-raf)
       (write-collection! dest-raf)))

(defn copy-when!
  [^RandomAccessFile src-raf ^RandomAccessFile dest-raf pred-fn]
  (->> (to-collection src-raf)
       (filter pred-fn)
       (write-collection! dest-raf)))

(comment

  (require '[io.doane.file-collection.utils :refer [create-random-access-file
                                                    create-file-data-input-stream]])

  (def raf (create-random-access-file "/tmp/fc-benchmark/users.fcd" false))

  (set! *warn-on-reflection* true)

  (time (def rval (doall (to-collection raf))))

  (.seek raf 0)

  (read-data raf)

  )
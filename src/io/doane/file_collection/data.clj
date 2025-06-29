(ns io.doane.file-collection.data
  (:require [clojure.java.io :as io]
            [taoensso.nippy :as nippy])
  (:import (java.io RandomAccessFile)))

(defn create-random-access-file
  [path sync?]
  (RandomAccessFile. (io/file path) (str "rw" (when sync? "s"))))

(defn write-data!
  [^RandomAccessFile raf data]
  (let [frozen-bytes  (nippy/freeze data)
        frozen-length (count frozen-bytes)]
    (locking raf
      ;;; We always write to the end of the file.
      (.seek raf (.length raf))
      ;;; This value has not been dropped from the collection.
      (.writeBoolean raf false)
      ;;; This is the size of the data for the next collection item.
      (.writeLong raf frozen-length)
      ;;; Write the frozen nippy content.
      (.write raf frozen-bytes)))
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

(defn drop-data!
  ([^RandomAccessFile raf pred]
   (drop-data! raf pred 0))
  ([^RandomAccessFile raf pred offset]
   (let [{:keys [data offset next-offset]} (read-data raf offset)]
     (when next-offset
       (when (and (not= ::dropped data) (pred data))
         (locking raf
           (.seek raf offset)
           (.writeBoolean raf true)))
       (recur raf pred next-offset)))))

(defn to-collection
  ([^RandomAccessFile raf]
   (to-collection raf 0))
  ([^RandomAccessFile raf offset]
   (lazy-seq
     (when-let [{:keys [next-offset data]} (read-data raf offset)]
       (if (= data ::dropped)
         (to-collection raf next-offset)
         (cons data (to-collection raf next-offset)))))))

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

  (def raf (create-random-access-file "/tmp/fc-1" true))
  (def raf2 (create-random-access-file "/tmp/fc-2" true))
  (def raf3 (create-random-access-file "/tmp/fc-3" true))

  (.getFilePointer raf)

  (write-data! raf {:a 1 :b 2 :c 3 :d 4 "e" 5.0})
  (write-data! raf {:user/email      "jrdoane@gmail.com"
                    :user/first-name "Jonathan"
                    :user/last-name  "Doane"})

  (drop-data! raf :a)

  (time (clean-copy! raf raf2))

  (time (to-collection raf2))

  (.seek raf 0)

  (read-data raf)

  )
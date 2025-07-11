(ns io.doane.file-collection.index
  (:require
    [io.doane.file-collection.data :as data]
    [taoensso.nippy :as nippy])
  (:import (java.io
             ByteArrayInputStream
             ByteArrayOutputStream
             DataInput
             DataInputStream
             DataOutput
             DataOutputStream
             EOFException
             RandomAccessFile)))

(defn next-data-offset
  "Given an instance of `java.io.RandomAccessFile`, either return the long value
  contained in the first 8 bytes of the RAF or zero if the file is empty. This
  value represents the last data location that was indexed. This function is
  thread-safe."
  [^RandomAccessFile index-raf]
  (if (zero? (.length index-raf))
    0
    (locking index-raf
      (.seek index-raf 0)
      (.readLong index-raf))))

(defn write-index-data-offset!
  "Given an instance of `java.io.RandomAccessFile`, and a long value for the next
  data offset to be processed (which may be the EOF of the data file,) write that
  next data offset to the first 8 bytes of the index RAF. This function is
  thread-safe."
  [^RandomAccessFile index-raf next-data-offset]
  (locking index-raf
    (doto index-raf
      (.seek 0)
      (.writeLong next-data-offset)))
  nil)

(defn write-index-data!
  "Given an instance of a `java.io.DataOutput`, a long data offset, and a byte
  array of frozen Nippy bytes, write the data offset, length of the frozen bytes,
  and the frozen bytes, in that order, to the DataOutput object. This function is
  thread-safe."
  [^DataOutput data-output ^long data-offset ^bytes frozen-bytes]
  (locking data-output
    (doto data-output
      ;;; Write the data offset from the data file.
      (.writeLong data-offset)
      ;;; Write the length of the predicate value's serialized bytes.
      (.writeLong (count frozen-bytes))
      ;;; Write the frozen predicate value.
      (.write frozen-bytes))))

(defn write-index-collection!
  "Given an instance of a `java.io.RandomAccessFile`, and index collection, and
  an optional batch options map, write all the items in the `index-collection` to
  disk in batches dictated by the `:batch-size` attribute of the batch options
  map. This function is thead-safe."
  ([^RandomAccessFile index-raf index-collection]
   (write-index-collection! index-raf index-collection {}))
  ([^RandomAccessFile index-raf index-collection {:keys [batch-size]
                                                  :or   {batch-size 100}}]
   (when (not-empty index-collection)
     (doseq [index-coll-part (partition-all batch-size index-collection)]
       (let [baos (ByteArrayOutputStream.)
             dos  (DataOutputStream. baos)]
         (doseq [{:keys [data-offset indexed-value]} index-coll-part]
           (when indexed-value
             (let [frozen-bytes (nippy/freeze indexed-value)]
               (write-index-data! dos data-offset frozen-bytes))))
         (let [next-offset   (reduce max 0 (map :next-data-offset index-coll-part))
               all-the-bytes (.toByteArray baos)]
           (locking index-raf
             (.seek index-raf (.length index-raf))
             (.write index-raf all-the-bytes)
             (write-index-data-offset! index-raf next-offset))))))))

(defn raw-data->indexed-data
  "Given an indexing function and raw data from `to-raw-collection` in the data
  namespace, create data related to indexing which includes the data offset,
  next data offset, and value of applying the indexing function to the stored
  data."
  [indexing-fn raw-data]
  {:data-offset      (:offset raw-data)
   :next-data-offset (:next-offset raw-data)
   :indexed-value    (when (not= ::data/dropped (:data raw-data))
                       (indexing-fn (:data raw-data)))})

(defn advance-index!
  "Given two instances of `java.io.RandomAccessFile`, the `data-raf`, the
  `index-raf`, and an indexing function. Bring the `index-raf` up to speed with
  the current state of the `data-raf`."
  [^RandomAccessFile data-raf ^RandomAccessFile index-raf indexing-fn]
  (let [starting-data-offset (next-data-offset index-raf)
        raw-data-collection  (data/to-raw-collection data-raf starting-data-offset)]
    (when (zero? (.length index-raf))
      (write-index-data-offset! index-raf 0))
    (->> raw-data-collection
         (map (partial raw-data->indexed-data indexing-fn))
         (write-index-collection! index-raf))))

(defn read-index-from-data-input
  [^DataInput data-input index-offset]
  (let [data-offset   (.readLong data-input)
        frozen-length (.readLong data-input)
        frozen-bytes  (byte-array frozen-length)
        indexed-value (do (.readFully data-input frozen-bytes 0 frozen-length)
                          (nippy/thaw frozen-bytes))]
    {:index-offset      index-offset
     :data-offset       data-offset
     :next-index-offset (+ index-offset 16 frozen-length)
     :indexed-value     indexed-value}))

(defn read-index
  [^RandomAccessFile index-raf index-offset]
  (locking index-raf
    (.seek index-raf index-offset)
    (when-not (= (.length index-raf) (.getFilePointer index-raf))
      (read-index-from-data-input index-raf index-offset))))

(defn read-index-batch
  ([^RandomAccessFile index-raf index-offset]
   (read-index-batch index-raf index-offset {}))
  ([^RandomAccessFile index-raf index-offset {:keys [byte-size]
                                              :or   {byte-size 262144}}]
   (let [batch-bytes (byte-array byte-size)
         _bytes-read (locking index-raf
                       (.seek index-raf index-offset)
                       (.read index-raf batch-bytes))
         bais        (ByteArrayInputStream. batch-bytes)
         dis         (DataInputStream. bais)]
     (loop [local-index-offset index-offset
            index-vec          []]
       (let [index-packet (try (read-index-from-data-input dis local-index-offset)
                               (catch EOFException ex
                                 nil))]
         (if (or (nil? index-packet)
                 (= (.length index-raf) (:next-index-offset index-packet)))
           (if (nil? index-packet)
             {:collection (seq index-vec)
              :next-offset local-index-offset}
             {:collection (seq (conj index-vec index-packet))
              :next-offset (:next-index-offset index-packet)})
           (recur (:next-index-offset index-packet)
                  (conj index-vec index-packet))))))))

(defn to-raw-index-collection
  ([index-raf]
   (to-raw-index-collection index-raf 8))
  ([index-raf index-offset]
   (to-raw-index-collection index-raf index-offset {}))
  ([index-raf index-offset batch-opts]
   (lazy-seq
     (when (not= (.length index-raf) index-offset)
       (let [{:keys [collection next-offset]} (read-index-batch index-raf index-offset batch-opts)]
         (lazy-cat collection (to-raw-index-collection index-raf next-offset batch-opts)))))))

(defn resolve-indexed
  [^RandomAccessFile data-raf indexed]
  (:data (data/read-data data-raf (:data-offset indexed))))

(defn indexed-collection->data-collection
  [^RandomAccessFile data-raf indexed-collection]
  (->> indexed-collection
       (map (partial resolve-indexed data-raf))
       (remove #(= % ::data/dropped))))

(comment

  (require '[io.doane.file-collection.utils :refer [create-random-access-file]])

  (def data-raf (create-random-access-file "/tmp/fc-benchmark/users.fcd" false))
  (def index-raf (create-random-access-file "/tmp/fc-1.index5.fci" false))
  (.setLength index-raf 0)

  (next-data-offset index-raf)

  (time (advance-index! data-raf index-raf :user/email))

  (time (def rval (doall (to-raw-index-collection index-raf))))
  (count rval)

  (->> (to-raw-index-collection index-raf)
       (indexed-collection->data-collection data-raf))

  )
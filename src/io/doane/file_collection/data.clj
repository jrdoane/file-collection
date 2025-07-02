(ns io.doane.file-collection.data
  (:require
    [taoensso.nippy :as nippy])
  (:import (java.io DataOutput
                    DataInput
                    DataOutputStream
                    DataInputStream
                    ByteArrayOutputStream
                    ByteArrayInputStream
                    EOFException
                    RandomAccessFile)))

(defn write-to-data-output!
  [^DataOutput data-output ^bytes frozen-bytes]
  (let [frozen-length (count frozen-bytes)]
    (locking data-output
      (doto data-output
        ;;; This value has not been dropped from the collection. Can mutate.
        (.writeBoolean false)
        ;;; This is the size of the data for the next collection item.
        (.writeLong frozen-length)
        ;;; Write the frozen nippy content.
        (.write frozen-bytes)))))

(defn write-data!
  [^RandomAccessFile raf data]
  (let [frozen-bytes (nippy/freeze data)]
    (locking raf
      ;;; We always write to the end of the file unless a value is being dropped.
      (.seek raf (.length raf))
      (write-to-data-output! raf frozen-bytes)))
  nil)

(defn write-collection!
  ([^RandomAccessFile raf data-collection]
   (write-collection! raf data-collection {}))
  ([^RandomAccessFile raf data-collection {:keys [batch-size] :as _opts
                                           :or   {batch-size 100}}]
   (doseq [data-coll-part (partition-all batch-size data-collection)]
     (let [baos (ByteArrayOutputStream.)
           dos  (DataOutputStream. baos)]
       (doseq [data data-coll-part]
         (let [frozen-bytes (nippy/freeze data)]
           (write-to-data-output! dos frozen-bytes)))
       (let [all-the-bytes (.toByteArray baos)]
         (locking raf
           (.seek raf (.length raf))
           (.write raf all-the-bytes)))))))

(defn read-from-data-input
  [^DataInput data-input offset]
  (let [dropped?      (.readBoolean data-input)
        frozen-length (.readLong data-input)]
    {:offset      offset
     :next-offset (+ offset 9 frozen-length)
     :data        (if dropped?
                    (do
                      (.skipBytes data-input frozen-length)
                      ::dropped)
                    (let [frozen-bytes (byte-array frozen-length)]
                      (.readFully data-input frozen-bytes 0 frozen-length)
                      (nippy/thaw frozen-bytes)))}))

(defn read-data
  [^RandomAccessFile raf offset]
  (locking raf
    (.seek raf offset)
    (when-not (= (.length raf) (.getFilePointer raf))
      (read-from-data-input raf offset))))

(defn read-batch
  ([^RandomAccessFile raf offset]
   (read-batch raf offset {}))
  ([^RandomAccessFile raf offset {:keys [byte-size]
                                  :or   {byte-size 262144}}]
   (let [batch-bytes (byte-array byte-size)
         _bytes-read (locking raf
                       (.seek raf offset)
                       (.read raf batch-bytes))
         bais        (ByteArrayInputStream. batch-bytes)
         dis         (DataInputStream. bais)]
     (loop [local-offset offset
            data-vec     []]
       (let [data-packet (try (read-from-data-input dis local-offset)
                              (catch EOFException ex
                                nil))]
         (if (or (nil? data-packet)
                 (= (.length raf) (:next-offset data-packet)))
           (if (nil? data-packet)
             {:collection  (seq data-vec)
              :next-offset local-offset}
             {:collection  (seq (conj data-vec data-packet))
              :next-offset (:next-offset data-packet)})
           (recur (:next-offset data-packet)
                  (if (= (:data data-packet) ::dropped)
                    data-vec
                    (conj data-vec data-packet)))))))))

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
   (to-raw-collection raf offset {}))
  ([^RandomAccessFile raf ^long offset batch-opts]
   (lazy-seq
     (when (not= (.length raf) offset)
       (let [{:keys [collection next-offset]} (read-batch raf offset batch-opts)]
         (lazy-cat collection (to-raw-collection raf next-offset)))))))

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

  (require '[io.doane.file-collection.utils :refer [create-random-access-file]])

  (def raf (create-random-access-file "/tmp/testing.fcd" false))

  (to-collection raf)

  (write-collection! raf [{:a 1 :b 2 :c "three" :d :four}])

  (def raf (create-random-access-file "/tmp/fc-benchmark/users.fcd" false))

  (time (def rval (read-batch raf 0)))
  (time (def rval2 (read-batch raf (:next-offset rval))))
  (time (def rval3 (read-batch raf (:next-offset rval2))))
  (time (def rval4 (read-batch raf (:next-offset rval3))))
  (time (def rval5 (read-batch raf (:next-offset rval4))))

  (count (:collection rval))

  (.length raf)

  (read-data raf 0)
  (read-data raf 1048312)

  (time (def rval (doall (read-all-via-batches raf))))
  (count rval)

  (set! *warn-on-reflection* true)

  (time (def rval (doall (to-collection raf))))

  (.seek raf 0)

  (read-data raf)

  )
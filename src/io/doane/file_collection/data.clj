(ns io.doane.file-collection.data
  (:require
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

(defn write-to-data-output!
  "Given an instance of a `java.io.DataOutput` object and a byte array of Nippy
  frozen bytes. Write a boolean to indicate if the item has been dropped, a long
  that represents the size of the frozen bytes, then all the frozen bytes.

  Grab monitor lock on `data-output` before writing so it should be thread safe.

  Returns the number of bytes written."
  [^DataOutput data-output ^bytes frozen-bytes]
  (let [frozen-length (count frozen-bytes)]
    (locking data-output
      (doto data-output
        ;;; This value has not been dropped from the collection. Can mutate.
        (.writeBoolean false)
        ;;; This is the size of the data for the next collection item.
        (.writeLong frozen-length)
        ;;; Write the frozen nippy content.
        (.write frozen-bytes)))
    (+ 9 frozen-length)))

(defn write-data!
  "Given an instance of a `java.io.RandomAccessFile` and arbitrary Clojure data,
  freeze the data with Nippy, seek to the end of the RAF, and write the singular
  frozen data entity via `write-to-data-output!`.

  Returns nil."
  [^RandomAccessFile raf data]
  (let [frozen-bytes (nippy/freeze data)]
    (locking raf
      ;;; We always write to the end of the file unless a value is being dropped.
      (.seek raf (.length raf))
      (write-to-data-output! raf frozen-bytes)))
  nil)

(defn write-collection!
  "Given an instance of a `java.io.RandomAccessFile` and a collection of arbitrary
  Clojure data, and an optional batch options map. Writes all of the provided collection
  to the RAF like `write-data!` except does it in batches into memory, then writes
  those batches to disk via the RAF.

  The batch options map can take a single argument:
  :batch-size which defaults to 100."
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
  "Given an instance of a `java.io.DataInput` and a data offset at which the data
  resides, reads 9 bytes for the dropped flag and length of the frozen data, followed
  by the length of the frozen data if the data hasn't been dropped. This is read
  out into a map with the attributes:
  `:offset` which is where the data packet starts.
  `:next-offset` where the next data is or will be if it's the end of the input.
  `:data` which is the unfrozen data from the on disk collection.
          The above may also be `:io.doane.file-collection.data/dropped` in which
          case we bypassed reading the data because it was marked as dropped.

  This function may throw IOException if something goes terribly wrong or EOFException
  if we've run out of data and expected more."
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
  "Given an instance of a `java.io.RandomAccessFile` read a single collection value
  from the RAF at the provided offset value. This function is thread-safe. Returns
  the output of `read-from-data-input` or `nil` if we're at the end of the file."
  [^RandomAccessFile raf offset]
  (locking raf
    (.seek raf offset)
    (when-not (= (.length raf) (.getFilePointer raf))
      (read-from-data-input raf offset))))

(defn read-batch
  "Given an instance of a `java.io.RandomAccessFile`, a data offset where we should
  start reading, and an optional batch options map. Returns a map of two attributes:
  `:collection` which is the slice of the collection we just read in a batch. These
                values are output from `read-from-data-input`.
  `:next-offset` which is the location to start reading from for the next item in
                 the collection if it exists yet. May point to the end of the file."
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
  "Given an instance of `java.io.RandomAccessFile` and an offset for data in that file,
  write a single byte (boolean value) to mark the data at that offset as dropped.

  Returns nil."
  [^RandomAccessFile raf offset]
  (locking raf
    (.seek raf offset)
    (.writeBoolean raf true)))

(defn to-raw-collection
  "Given an instance of a `java.io.RandomAccessFile` and optional offset and
  batch options map. Return a lazy sequence of raw values from the collection on
  disk with dropped items omitted. Data is read in batches and the size of those
  batches are controlled by the `:byte-size` attribute on the batch options map.
  This value may need to be increased for collections with very large items in.
  The default value is 256KB. The default offset is 0, the beginning of the file."
  ([^RandomAccessFile raf]
   (to-raw-collection raf 0))
  ([^RandomAccessFile raf offset]
   (to-raw-collection raf offset {}))
  ([^RandomAccessFile raf ^long offset batch-opts]
   (lazy-seq
     (when (not= (.length raf) offset)
       (let [{:keys [collection next-offset]} (read-batch raf offset batch-opts)]
         (lazy-cat collection (to-raw-collection raf next-offset batch-opts)))))))

(defn drop-data!
  "Given an instance of `java.io.RandomAccessFile` and a predicate function, lazily
  read the RAF via `to-raw-collection` and check every element for a truthy return
  value from the predicate function. Then write the 'drop byte/boolean' for each
  of those items.

  Returns nil"
  [^RandomAccessFile raf predicate-fn]
  (let [entities-to-drop (filter #(predicate-fn (:data %)) (to-raw-collection raf))]
    (doseq [{:keys [offset]} entities-to-drop]
      (write-drop! raf offset))))

(defn to-collection
  "Like `to-raw-collection` except mapped over the stored value. Should match the
  collection data initially written to disk."
  ([^RandomAccessFile raf]
   (->> (to-raw-collection raf)
        (map :data))))

(defn copy!
  "Given a source and destination `java.io.RandomAccessFile`, pull a lazy collection
  from the source and write to the destination with all those values."
  [^RandomAccessFile src-raf ^RandomAccessFile dest-raf]
  (->> (to-collection src-raf)
       (write-collection! dest-raf)))

(defn copy-when!
  "Like `copy!` except it takes a predicate function and only copies values for
  which the predicate function returns true."
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
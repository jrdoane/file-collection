(ns io.doane.file-collection.utils
  (:require [clojure.java.io :as io])
  (:import (java.io RandomAccessFile)))

(defn create-random-access-file
  "Given a path to a possibly non-existant file on disk and a `sync?` flag, return
  a readable/writeable `java.io.RandomAccessFile` to that path. If `sync?` is true,
  all writes to disk will occur synchronously."
  [path sync?]
  (RandomAccessFile. (io/file path) (str "rw" (when sync? "s"))))

(defn nil-culling
  "Given a map, return a new map with all the keys that map to nil removed. This
  is for applications that can tolerate missing keys for mappings to nil values."
  [a-map]
  (reduce
    (fn [a-map k]
      (if (nil? (a-map k))
        (dissoc a-map k)
        a-map))
    a-map
    (keys a-map)))

(comment

  (nil-culling {:a 1 :b 2 :c nil :d nil :e :foo})

  )
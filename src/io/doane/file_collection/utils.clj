(ns io.doane.file-collection.utils
  (:require [clojure.java.io :as io])
  (:import (java.io RandomAccessFile)))

(defn create-random-access-file
  [path sync?]
  (RandomAccessFile. (io/file path) (str "rw" (when sync? "s"))))
# File Collection
This is a Clojure library for writing arbitrary data to disk and returning it later as a lazy collection for processing.

## Usage
### Deps
Just pull it directly from GitHub for now.
```clojure
{:deps {io.github.jrdoane/file-collection {:git/tag "v0.1.2"}}}
```

### Writing Data
```clojure
(require '[io.doane.file-collection.data :as data])
(require '[io.doane.file-collection.utils :as utils])

;;; Create a Random Access File via the helper we've created.
;;; The first argument is fed to `clojure.java.io/file`.
;;; The second argument signals the RAF to write synchronously when true.
(def raf (utils/create-random-access-file "/tmp/my-first-file-collection" true))
(def raf2 (utils/create-random-access-file "/tmp/my-second-file-collection" true))

;;; Write some data to the newly created RAF. Data written can be any arbitrary
;;; data writable by Nippy.
(data/write-data! raf {:user/first-name "Jonathan"
                       :user/last-name  "Doane"
                       :user/email      "jrdoane@gmail.com"})

(data/write-data! raf {:user/first-name "Faker"
                       :user/last-name  "McFakerson"
                       :user/email      "fmfakerson@fakerson.fk"})

;;; "Remove" data from the on-disk collection. Doesn't reclaim disk space.
(data/drop-data! raf (fn [data] (= (:user/email data) "fmfakerson@fakerson.fk")))

;;; Get all the data as a lazy sequence.
;;; Please note that if the file is larger than memory, you'll blow the heap if
;;; you realize the entire thing.
(data/to-collection raf)

;;; Copy data from one random access file to another, removing dropped data.
;;; If the destination RAF already exists with data, the dest RAF will be
;;; concatenated to it.
(data/copy! raf raf2)
```

### Indexing Data
```clojure
;;; Including the code from above...

(require '[io.doane.file-collection.index :as index])

;;; We need a new random access file for the index we're going to create.
(def email-raf (utils/create-random-access-file "/tmp/my-first-index" true))

;;; Bring the newly created index up to speed.
(index/advance-index! raf email-raf :user/email)

;;; Get a lazy collection of everything with an email address.
(->> (index/to-raw-index-collection email-raf)
     (index/indexed-collection->data-collection raf))

;;; Use the Index to find a particular item.
(->> (index/to-raw-index-collection)
     (filter #(= (:indexed-value %) "jrdoane@gmail.com"))
     (index/indexed-collection->data-collection raf)
     (first))
```

### Running Tests
```shell
➜  file-collection git:(master) clj -X:test:run-tests

Running tests in #{"test"}

Testing io.doane.file-collection.data-test

Testing io.doane.file-collection.index-test

Ran 2 tests containing 106 assertions.
0 failures, 0 errors.
```

### Running the Benchmark
```shell
➜  file-collection git:(master) ✗ clj -X:test:run-benchmark
Writing 10,000 maps with 3 attributes.
Evaluation count : 4680 in 60 samples of 78 calls.
             Execution time mean : 13.744087 ms
    Execution time std-deviation : 1.154805 ms
   Execution time lower quantile : 12.904892 ms ( 2.5%)
   Execution time upper quantile : 17.200553 ms (97.5%)
                   Overhead used : 2.112887 ns

Found 4 outliers in 60 samples (6.6667 %)
	low-severe	 2 (3.3333 %)
	low-mild	 2 (3.3333 %)
 Variance from outliers : 61.8537 % Variance is severely inflated by outliers
Filtering all of the 'Smith' last name users from the RAF.
Evaluation count : 3000 in 60 samples of 50 calls.
             Execution time mean : 20.515648 ms
    Execution time std-deviation : 490.082099 µs
   Execution time lower quantile : 20.130916 ms ( 2.5%)
   Execution time upper quantile : 21.425524 ms (97.5%)
                   Overhead used : 2.112887 ns

Found 4 outliers in 60 samples (6.6667 %)
	low-severe	 2 (3.3333 %)
	low-mild	 2 (3.3333 %)
 Variance from outliers : 11.0455 % Variance is moderately inflated by outliers
Create an index from the 'Smith' predicate.
Evaluation count : 2520 in 60 samples of 42 calls.
             Execution time mean : 24.979500 ms
    Execution time std-deviation : 1.206162 ms
   Execution time lower quantile : 23.912795 ms ( 2.5%)
   Execution time upper quantile : 28.225404 ms (97.5%)
                   Overhead used : 2.112887 ns

Found 6 outliers in 60 samples (10.0000 %)
	low-severe	 5 (8.3333 %)
	low-mild	 1 (1.6667 %)
 Variance from outliers : 33.6318 % Variance is moderately inflated by outliers
Pull all the 'Smith' users via the Smith index.
Evaluation count : 84480 in 60 samples of 1408 calls.
             Execution time mean : 718.801073 µs
    Execution time std-deviation : 19.720036 µs
   Execution time lower quantile : 705.455846 µs ( 2.5%)
   Execution time upper quantile : 773.076617 µs (97.5%)
                   Overhead used : 2.112887 ns

Found 4 outliers in 60 samples (6.6667 %)
	low-severe	 4 (6.6667 %)
 Variance from outliers : 14.2215 % Variance is moderately inflated by outliers
Pull all the 'Smith' users via the last name index.
Evaluation count : 4440 in 60 samples of 74 calls.
             Execution time mean : 14.038325 ms
    Execution time std-deviation : 591.481559 µs
   Execution time lower quantile : 13.547565 ms ( 2.5%)
   Execution time upper quantile : 15.539167 ms (97.5%)
                   Overhead used : 2.112887 ns

Found 7 outliers in 60 samples (11.6667 %)
	low-severe	 5 (8.3333 %)
	low-mild	 2 (3.3333 %)
 Variance from outliers : 28.6797 % Variance is moderately inflated by outliers
Find a particular user by email via an index.
Evaluation count : 4500 in 60 samples of 75 calls.
             Execution time mean : 14.840389 ms
    Execution time std-deviation : 1.950922 ms
   Execution time lower quantile : 13.061196 ms ( 2.5%)
   Execution time upper quantile : 18.507917 ms (97.5%)
                   Overhead used : 2.112887 ns

Found 2 outliers in 60 samples (3.3333 %)
	low-severe	 1 (1.6667 %)
	low-mild	 1 (1.6667 %)
 Variance from outliers : 80.6617 % Variance is severely inflated by outliers
```

## License
Copyright &copy; 2025 [Jonathan Doane][].
Licensed under [EPL 1.0](license.md).

<!-- Some Links and stuff. -->
[Jonathan Doane]: mailto:jrdoane@gmail.com

# File Collection
This is a Clojure library for writing arbitrary data to disk and returning it later as a lazy collection for processing.

## Usage
### Deps
Just pull it directly from GitHub for now.
```clojure
{:deps {io.github.jrdoane/file-collection {:git/tag "v0.1.0"}}}
```

### Code Example
```clojure
(require '[io.doane.file-collection.data :as data])

;;; Create a Random Access File via the helper we've created.
;;; The first argument is fed to `clojure.java.io/file`.
;;; The second argument signals the RAF to write synchronously when true.
(def raf (data/create-random-access-file "/tmp/my-first-file-collection" true))
(def raf2 (data/create-random-access-file "/tmp/my-second-file-collection" true))

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

## License
Copyright &copy; 2025 [Jonathan Doane][].
Licensed under [EPL 1.0](license.md).

<!-- Some Links and stuff. -->
[Jonathan Doane]: mailto:jrdoane@gmail.com

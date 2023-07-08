(ns caulk
  "Just some statistics on the training data.."
  (:use clojure.data.csv
        clojure.math)
  (:require csv
            [tech.v3.dataset :as ds]
            [tech.v3.dataset.column-filters :as cf]))

(defn
  log2
  "Obvi `java.lang.Math` has base-e, base-10.. but not base-2.."
  [n]
  (/ (log n)
     (log 2)))

(def example (-> "example.csv"
                 ds/->dataset))

(def numeric-columns
  (into []
        (-> example
            (ds/descriptive-stats {:stat-names [:col-name
                                                :datatype
                                                :min
                                                :mean
                                                :max
                                                :standard-deviation]})
            (ds/filter-column :datatype #(= % :float64))
            (get :col-name))))
;; => ["Danceability"
;;     "Energy"
;;     "Key"
;;     "Loudness"
;;     "Speechiness"
;;     "Acousticness"
;;     "Instrumentalness"
;;     "Liveness"
;;     "Valence"
;;     "Tempo"
;;     "Duration_ms"
;;     "Views"
;;     "Likes"
;;     "Stream"
;;     "Comments"]

(defn
  add-index
  "Adds a INDEX column to keep track of the original row
  So that after a bunch of transformation and filtering
  you can return back to your original table and find the data"
  [dataset]
  (-> dataset
      (ds/add-column
        (->> dataset
             ds/row-count
             range
             (ds/new-column "INDEX")))))
#_
(-> example
    cf/numeric
    add-index)

(defn
  normalize-dataset
  "Make all the columns on the 0.0-1.0 range
  Column called INDEX is left untouched"
  [dataset]
  (let [col-mins (->> dataset
                      cf/numeric
                      ds/columns
                      (mapv (fn [column]
                              ;;       (println column)
                              (apply min
                                     (->> column                   
                                          (filter some?))))))
        col-maxs (->> dataset
                      cf/numeric
                      ds/columns
                      (mapv (fn [column]
                              ;;       (println column)
                              (apply max
                                     (->> column                   
                                          (filter some?))))))]
    (-> dataset
        cf/numeric
        (ds/row-map (fn [row]
                      (into {}
                            (mapv (fn [[elem-key
                                        elem-val]
                                       min-val
                                       max-val]
                                    (if (= elem-key
                                           "INDEX")
                                      [elem-key ;; don't normalize index :)
                                       elem-val]
                                      (if (nil? elem-val)
                                        [elem-key
                                         nil]
                                        [elem-key
                                         (/ (- elem-val
                                               min-val)
                                            max-val)])))
                                  row
                                  col-mins
                                  col-maxs)))))))
#_
(-> example
    add-index
    normalize-dataset)

(defn
  manhattan
  "Calculate the Manhattan distance (L1 norm)
  between two row values
  ..
  If either coordinate is `nil`
  then the coordinate is not considered
  ..
  Final result is normalized by the number of dimensionsd"
  [row1
   row2]
  (let [diff (->> (mapv (fn [[key1 val1]
                             [key2 val2]]
                          (if (or (nil? val1)
                                  (nil? val2)
                                  (= key1
                                     "INDEX")
                                  (= key2 ;; redundant check
                                     "INDEX"))
                            nil
                            (abs (- val1
                                    val2))))
                        row1
                        row2)
                  (filter some?))]
    (/ (->> diff
            (reduce +))
       (count diff))))
#_
(->> 10
     (range 1)
     (mapv #(let [data (-> example
                           cf/numeric
                           normalize-dataset
                           add-index
                           (ds/head 10))
                  row1 (-> data
                           (ds/row-at 0))
                  row2 (-> data
                           (ds/row-at %))]
              (manhattan row1
                         row2))))

(defn
  estimate-coord
  "Given a coordinate, specified by a `row-idx` and `col-name`
  Use an L1 Manhattan norm to equivalent value in
  the closest point/row which has a non-nil"
  [dataset
   normalized-data
   row-idx
   col-name]
  (let [#_#_normalized-data (-> dataset
                                cf/numeric
                                normalize-dataset
                                add-index)] ;; TODO: This is recomputed every time.. :S
    (let [reference-row  (-> normalized-data
                             (ds/row-at row-idx))
          candidate-data (-> normalized-data
                             add-index
                             (ds/drop-missing col-name))]
      ;; TODO: Throw some error if the candidate data is empty?
      (let [distances (->> candidate-data
                           count
                           range
                           (mapv #(let [candidate-row (-> candidate-data
                                                          (ds/row-at %))]
                                    (vector (get candidate-row
                                                 "INDEX")
                                            (manhattan reference-row
                                                       candidate-row)))))]
        (get (->> distances
                  (sort-by second)
                  first
                  first
                  (ds/row-at dataset))
             col-name)))))
#_
(estimate-coord example
                (-> example
                    cf/numeric
                    normalize-dataset
                    add-index)
                1
                "Loudness")

(defn
  manhattan-nullfill
  "Fill in null values with value from the closest
  neighbouring point that has a value for that column.
  Distance to points is calculates using the L1 Manhattan norm
  divided by the number of common non-nil dimensions."
  [dataset]
  (let [clean-ds        (-> dataset
                            cf/numeric
                            add-index)
        normalized-data (-> clean-ds
                            cf/numeric
                            normalize-dataset
                            add-index)]
    (-> clean-ds
        (ds/row-map (fn [row]
                      (->> row ;; run across each value in each row
                           (mapv (fn fix-if-nil
                                   [[key value]]
                                   (if (nil? value)
                                     [key
                                      (estimate-coord clean-ds ;; use original data
                                                      normalized-data
                                                      (get row
                                                           "INDEX")
                                                      key)]
                                     [key value])));; non-nil - no fix
                           (into {})))))))
(-> example
    cf/numeric
    add-index
    manhattan-nullfill
    (ds/write! "no-holes-numeric.csv"))

(let [hole-filled (-> example
                      cf/numeric
                      add-index
                      manhattan-nullfill)]
  (ds/write!
    (-> example
        (ds/add-or-update-column  (ds/column example
                                             "Energy" ))
        (ds/add-or-update-column  (ds/column example
                                             "Key" ))
        (ds/add-or-update-column  (ds/column example
                                             "Loudness" ))
        (ds/add-or-update-column  (ds/column example
                                             "Speechiness" ))
        (ds/add-or-update-column  (ds/column example
                                             "Acousticness" ))
        (ds/add-or-update-column  (ds/column example
                                             "Instrumentalness" ))
        (ds/add-or-update-column  (ds/column example
                                             "Liveness" ))
        (ds/add-or-update-column  (ds/column example
                                             "Valence" ))
        (ds/add-or-update-column  (ds/column example
                                             "Tempo" ))
        (ds/add-or-update-column  (ds/column example
                                             "Duration_ms" ))
        (ds/add-or-update-column  (ds/column example
                                             "Views" ))
        (ds/add-or-update-column  (ds/column example
                                             "Likes" ))
        (ds/add-or-update-column  (ds/column example
                                             "Stream" ))
        (ds/add-or-update-column  (ds/column example
                                             "id" ))
        (ds/add-or-update-column  (ds/column example
                                             "Comments" )))
    "no-holes-all.csv"))



(ns districts.core
  (:import (org.geotools.geojson.feature FeatureJSON)
           (org.geotools.geojson.geom GeometryJSON)
           (org.geotools.feature FeatureCollection)
           (org.opengis.feature.simple SimpleFeature)
           (java.util.zip GZIPInputStream)
           (java.text DecimalFormat))
  (:require (clojure.java [io :as io])
            (clojure.data [json :as json]
                          [csv :as csv])))

(def ^:private dec-format (DecimalFormat. "#.####"))

(def ^:private state-codes
  [1 2 4 5 6 8 9 10 12 13 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29
   30 31 32 33 34 35 36 37 38 39 40 41 42 44 45 46 47 48 49 50 51 53 54
   55 56])

(defn- truncate [v]
  (.parse dec-format (.format dec-format (double v))))

(defn- as-clj [^SimpleFeature v]
  (try {:state (Integer/parseInt (.getAttribute v 0))
        :district (Integer/parseInt (.getAttribute v 1))
        :congress (Integer/parseInt (.getAttribute v 5))
        :shape (.getAttribute v 12)}
       (catch Exception e nil)))

(defn- load-districts [file]
  (->> (GZIPInputStream. (io/input-stream file))
       (.readFeatureCollection (FeatureJSON.))
       (.toArray)
       (keep as-clj)
       (group-by :state)))

(defn- make-shape-scores [district-geos]
  (let [district-map (into {} (map #(vector (:district %) %)
                                   district-geos))
        districts (set (keys district-map))]
    (zipmap districts
            (for [district districts]
              (let [hull (.convexHull (:shape (district-map district)))
                    total-area (.getArea hull)]
                (->> (keep #(let [s (:shape (district-map %))]
                              (when (.intersects hull s)
                                (.getArea (.intersection hull s))))
                           (disj districts district))
                     (reduce +)
                     (- total-area)
                     (* (/ total-area))
                     (truncate)
                     (hash-map :shape_score)))))))

(defn- average [xs]
  (/ (reduce + xs) (count xs)))

(defn- shape-scores [state-districts]
  (zipmap (keys state-districts)
          (map (fn [code]
                 (let [scores (make-shape-scores (state-districts code))]
                   {:shape_score (->> (vals scores)
                                      (map :shape_score)
                                      (average)
                                      (truncate))
                    :districts scores}))
               (keys state-districts))))

(defn- url [state-code]
  (let [s (format "%02d" state-code)]
    (str "http://www2.census.gov/geo/relfiles/cdsld13/" s
         "/ur_cd_delim_" s ".txt")))

(defn- convert [row]
  [(Integer/parseInt (row 1))
   (let [pct (-> (apply str (drop-last (seq (row 5))))
                 (Double/parseDouble)
                 (/ 100))]
     {:urban_rural_score (truncate (* 2 (Math/abs (- 0.5 pct))))
      :urban_pct (truncate pct)})])

(defn- scrape-csv [code]
 (try (let [scores (->> (nnext (csv/read-csv (slurp (url code))))
                        (map convert)
                        (into {}))]
        [code {:urban_rural_score (->> (vals scores)
                                       (map :urban_rural_score)
                                       (average)
                                       (truncate))
               :districts scores}])
      (catch Exception e nil)))

(defn- spit-scores-113 []
  (->> (merge-with #(merge-with (fn [a b] (merge-with merge a b)) %1 %2)
                   (into {} (map scrape-csv state-codes))
                   (-> (load-districts "resources/districts-113-geo.json.gz")
                       (shape-scores)
                       (select-keys state-codes)))
       (hash-map :states)
       (json/json-str)
       (spit "resources/state_scores.json")))

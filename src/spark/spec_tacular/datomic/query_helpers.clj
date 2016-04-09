(ns spark.spec-tacular.datomic.query-helpers
  (:require [clj-time.coerce :as timec]
            [clojure.core.match :refer [match]]
            [clojure.string :refer [lower-case]]
            [clojure.tools.macro :as m]
            [datomic.api :as d]
            [spark.spec-tacular :refer :all]
            [spark.spec-tacular.datomic.util :refer [db-keyword]]
            [spark.spec-tacular.datomic.pull-helpers :refer [datomify-spec-pattern]])
  (:import (spark.spec_tacular.spec Spec
                                    EnumSpec)))

;; ===================================================================================================
;; static

(defn set-type! [tenv x t]
  (if-let [t- (get @tenv x)]
    (when-not (or (= t t-)
                  (if-let [elems (:elements (get-spec t))]
                    (contains? elems t-)))
      (throw (ex-info "retvars has two return types" {:type1 t :type2 t-})))
    (do (swap! tenv assoc x t) nil)))

(defn expand-ident [ident uenv tenv]
  "takes an ident and returns a map with a unique variable and its intended spec"
  (cond
    (keyword? ident)
    ,(let [uid (gensym (str "?" (lower-case (name ident))))]
       (expand-ident [uid ident] uenv tenv))
    (symbol? ident)
    ,(let [spec-name (get @tenv ident)]
       (when-not spec-name
         (throw (ex-info (str "could not infer type for " ident)
                         {:syntax ident :tenv @tenv})))
       (expand-ident [ident spec-name] uenv tenv))
    (vector? ident) 
    ,(let [[var spec-name] ident
           spec (get-spec spec-name)]
       (when-not spec
         (throw (ex-info (str "could not find spec for " spec-name)
                         {:syntax ident})))
       {:var var :spec spec})
    :else (throw (ex-info (str (type ident) " unsupported ident") {:syntax ident}))))

(declare expand-clause expand-map)
(defn expand-item [item db-kw rhs x uenv tenv]
  (let [{[arity sub-spec-name] :type} item
        mk-where-clause (fn [rhs] [`'~x db-kw rhs])]
    (cond
      (::patvar (meta rhs))
      ,(do (set-type! tenv rhs sub-spec-name)
           (vec (concat (if-let [spec (get-spec (get @tenv rhs))]
                          (when (instance? Spec spec)
                            [[`'~rhs :spec-tacular/spec (get @tenv rhs)]])
                          [])
                        [(mk-where-clause `'~rhs)])))
      (and (keyword? rhs)
           (get-spec rhs))
      ,(let [y (gensym "?tmp")]
         (if (instance? spark.spec_tacular.spec.EnumSpec (get-spec rhs))
           [(mk-where-clause `'~rhs)]
           [(mk-where-clause `'~y)
            [`'~y :spec-tacular/spec rhs]]))
      (map? rhs)
      ,(let [y (gensym "?tmp")
             sub-atmap rhs]
         `(conj ~(expand-map sub-atmap y (get-spec sub-spec-name) uenv tenv)
                ~(mk-where-clause `'~y)))
      (nil? rhs)
      ,[[(list 'list ''missing? ''$ `'~x db-kw)]]
      (vector? rhs)
      ,(let [y (gensym "?tmp"), k (gensym "?kw")
             [l r] rhs
             spec (get-spec sub-spec-name)
             spec (if (:elements spec)
                    (get-spec sub-spec-name (get @tenv l))
                    spec)]
         (cond
           (and (keyword? l)
                (get-spec l))
           `(conj ~(expand-clause [[y l] r] uenv tenv)
                  ~(mk-where-clause `'~y))
           (and (keyword? l)
                (primitive? l))
           ,(do (set-type! tenv r l)
                (expand-item item db-kw r x uenv tenv))
           (and (::patvar (meta l)) (:items spec))
           ,(do (set-type! tenv l sub-spec-name)
                `(conj ~(expand-clause [[l (:name spec)] r] uenv tenv)
                       ~(mk-where-clause `'~l)))
           (and (::patvar (meta l)) (::patvar (meta r)))
           ,(do (set-type! tenv l :keyword)
                (set-type! tenv r (:name spec))
                [[`'~x db-kw `'~r]
                 [`'~r :spec-tacular/spec `'~l]])
           :else ;; fall back to dynamic resolution
           (throw (ex-info "invalid item" {:syntax rhs}))))
      :else
      ;; We have a "thing" that eventually resolves into a value.
      ,(let [sub-spec (get-spec sub-spec-name)
             y (gensym "?tmp")
             z (gensym)]
         `(let [~z ~(if (= sub-spec-name :calendarday) `(timec/to-date ~rhs) rhs)
                rhs-spec# (get-spec ~z)]
            (when (nil? ~z)
              (throw (ex-info "Maps cannot have nil values at runtime"
                              {:field ~sub-spec-name :syntax '~rhs})))
            (if (and rhs-spec# (instance? Spec rhs-spec#)) ;; Spec
              (if-let [eid# (get-in ~z [:db-ref :eid])]
                [['~x ~db-kw eid#]]
                (let [item# (some #(if (:unique? %) %) (:items rhs-spec#))
                      val#  (and item# ((:name item#) ~z))]
                  (if (and item# (some? val#))
                    [['~x ~db-kw '~y]
                     ['~y (db-keyword rhs-spec# (:name item#)) val#]]
                    (throw (ex-info "Cannot uniquely describe the given value"
                                    {:syntax ~rhs :value ~z})))))
              ~[(mk-where-clause z)])))))) ;; EnumSpec; everything else

;; map = {:kw (ident | clause | map | value),+}
(defn expand-map [atmap x spec uenv tenv]
  (when-not (map? atmap)
    (throw (ex-info "invalid map" {:syntax atmap})))
  (let [{:keys [elements items]} spec]
    (if items
      (-> (fn [[kw rhs]]
            (let [item (get-item spec kw)
                  db-keyword (db-keyword spec kw)]
              (cond
                item
                `(concat [['~x :spec-tacular/spec ~(:name spec)]]
                         ~(expand-item item db-keyword rhs x uenv tenv))
                (= kw :spec-tacular/spec)
                (do (set-type! tenv rhs :keyword)
                    [[`'~x :spec-tacular/spec `'~rhs]])
                :else (throw (ex-info "could not find item" {:syntax atmap :field kw})))))
          (keep atmap) (doall) (conj `concat))
      (let [maybe-spec (:spec-tacular/spec atmap)]
        (if (and (keyword? maybe-spec) (contains? elements maybe-spec))
          `(conj ~(expand-map (dissoc atmap :spec-tacular/spec) x (get-spec maybe-spec) uenv tenv)
                 ['~x :spec-tacular/spec ~maybe-spec])
          (let [try-all (-> #(try {% (expand-map (dissoc atmap :spec-tacular/spec) x
                                                 (get-spec %) uenv tenv)}
                                  (catch clojure.lang.ExceptionInfo e {% e}))
                            (keep elements))
                try-map (into {} try-all)
                grouped (group-by
                         #(if (or (= (type (second %)) clojure.lang.PersistentList)
                                  (= (type (second %)) clojure.lang.Cons))
                            :syntax :error)
                         try-map)]
            (when (empty? (get grouped :syntax))
              (throw (ex-info "does not conform to any possible unioned spec"
                              {:syntax atmap :possible-specs elements
                               :errors (get grouped :error)})))
            (cond
              (::patvar (meta maybe-spec))
              ,(let [opts (into {} (get grouped :syntax))]
                 (set-type! tenv maybe-spec :keyword)
                 `(concat
                   [['~x :spec-tacular/spec '~maybe-spec]]
                   (let [opts# ~opts]
                     (if (every? empty? (vals opts#)) []
                         [(list ~''or ~@(map (fn [[kw stx]] `(~'cons ~''and (~'concat [[(~'list ~''ground ~kw) '~maybe-spec]] ~stx)))
                                             opts))]))))
              (symbol? maybe-spec)
              `(let [opts# ~(into {} (get grouped :syntax))
                     spec# ~maybe-spec]
                 (concat [['~x :spec-tacular/spec spec#]]
                         (or (get opts# spec#)
                             (throw (ex-info "does not conform to any possible unioned spec"
                                             {:computed-spec spec#
                                              :available-specs (keys opts#)})))))
              (and (nil? maybe-spec) (nil? (get grouped :error)))
              `(let [opts# ~(into {} (get grouped :syntax))]
                 [(cons ~''or (map (fn [[kw# stx#]] (cons ~''and stx#)) opts#))])
              :else (throw (ex-info "Query syntax not supported"
                                    {:syntax atmap :errors (get grouped :error)})))))))))

(defn expand-clause [clause uenv tenv]
  (cond
    (vector? clause)
    ,(cond
       (= (count clause) 1)
       ,[[(->> (rest (first clause))
               (map (fn [x] (if (::patvar (meta x))
                              `(with-meta '~x ~(merge (meta x) {:tag `'~(:type-symbol (get-type (get @tenv x)))}))
                              x)))
               (cons `'~(ffirst clause))
               (cons 'list))]]
       (= (count clause) 2)
       ,(let [[ident atmap] clause
              {:keys [var spec]} (expand-ident ident uenv tenv)]
          (cond
            (map? atmap) (expand-map atmap var spec uenv tenv)
            :else (throw (ex-info "Invalid clause rhs" {:syntax clause})))))
    (seq? clause)
    ,(let [[head & clauses] clause]
       (case head
         'not-join
         ,(let [[vars & clauses] clauses
                clauses (map #(expand-clause % uenv tenv) clauses)]
            `[(cons ~''not-join (cons '~vars (concat ~@clauses)))])
         :else (throw (ex-info "unsupported sequence head"
                               {:syntax clause}))))
    :else (throw (ex-info "invalid clause type, expecting vector or sequence"
                          {:type (type clause) :syntax clause}))))

(defn annotate-retvars! [rets uenv tenv]
  (let [n (atom 1) 
        mk-new #(with-meta (gensym (str "?" %)) {::patvar true})
        swap-keyword! (fn [spec]
                        (let [%   (symbol (str "%" @n))
                              new (mk-new (lower-case (name spec)))]
                          (set-type! tenv new spec)
                          (swap! n inc)
                          (swap! uenv assoc % new)
                          new))
        swap-symbol! (fn [old] 
                       (let [new (mk-new old)]
                         (swap! uenv assoc old new) new))]
    (letfn [(swap-retvar! [r]
              (cond 
                (keyword? r) (swap-keyword! r)
                (symbol? r)  (swap-symbol! r)
                (and (seq? r)
                     (= (first r) 'pull))
                (let [[pull ident pattern] r
                      r' (swap-retvar! ident)]
                  (list pull r' pattern))))]
      (doall (map swap-retvar! rets)))))

(defn annotate-patvars! [clauses uenv tenv]
  (let [mk-new #(with-meta (gensym (str "?" %)) {::patvar true})
        annotate-clause! (fn [[id atmap]]
                           (cond
                             (symbol? id)
                             ,(let [gs (mk-new id)]
                                (swap! uenv assoc id gs))
                             (vector? id)
                             ,(let [gs (mk-new (first id))]
                                (do (set-type! tenv gs (second id))
                                    (swap! uenv assoc (first id) gs)))))]
    (doall (map annotate-clause! clauses))))

(defn desugar-query [rets clauses uenv tenv]
  (let [rets     (annotate-retvars! rets uenv tenv)
        bindings (apply concat '[% %1] (vec @uenv))
        do-expr  (m/mexpand `(m/symbol-macrolet ~bindings ~clauses))
        _        (annotate-patvars! clauses uenv tenv)]
    {:rets rets :clauses (second do-expr)}))

(defn expand-query [f wc]
  (let [tenv (atom {}) 
        uenv (atom {})]
    (let [{:keys [rets clauses]} (desugar-query f wc uenv tenv)
          clauses (doall (map #(expand-clause % uenv tenv) ;; side effects
                              clauses))
          clauses `(concat ~@clauses)]
      (assert (= (count rets) (count f)) "internal error")
      {:args rets :env @tenv :clauses clauses})))

;; (q :find find-expr+ :in clojure-expr :where clause+)
;; find-expr = ident
;; ident     = spec-name
;;           | datomic-variable
;;             | [datomic-variable spec-name]
;; clause    = [ident map]
;; map       = % | %n | spec-name
;;           | {:kw (clause | map | ident | value),+}
(defn ^{:no-doc true} parse-query [stx]
  (let [keywords [:find :in :where]
        protected? #{'pull 'min 'max 'count 'count-distinct 'sum 'avg 'median 'variance 'stddev}
        partitions (partition-by (fn [stx] (some #(= stx %) keywords)) stx)]
    ;; (find-rel | find-coll | find-tuple | find-scalar)
    (match partitions ;; ((:find) (1 2 ....) (:in) (3) (:where) (4 5 ....))
      ([([:find] :seq) f ([:in] :seq) db ([:where] :seq) wc] :seq)
      (let [parsed-f (match f
                       ([elem '.] :seq)
                       {:f [elem] :type :scalar}
                       ([([elem '...] :seq)] :seq)
                       {:f [elem] :type :coll}
                       ([([& (elems :guard #(not (protected? (first %))))] :seq)] :seq)
                       {:f elems :type :tuple}
                       ([& elems] :seq)
                       {:f elems :type :relation}
                       :else (throw (ex-info "expecting find specification for relation, coll, tuple, or scalar"
                                             {:syntax f})))]
        (do (when-not (= (count db) 1)
              (throw (ex-info "expecting exactly one database expression" {:syntax db})))
            (when-not (every? #(or (vector? %) (list? %)) wc)
              (throw (ex-info "expecting sequence of vectors or lists" {:syntax wc})))
            (let [bad-elems (filter #(not (or (symbol? %)
                                              (keyword? %)
                                              (and (seq? %) (protected? (first %)))))
                                    (:f parsed-f))]
              (when-not (empty? bad-elems)
                (throw (ex-info "invalid find element" {:bad-elements bad-elems}))))
            (merge parsed-f {:db (first db) :wc wc})))
      :else
      (throw (ex-info "expecting keywords :find, :in, and :where followed by arguments"
                      {:syntax partitions})))))

;; ===================================================================================================
;; dynamic

(defn combine-where-clauses [& clauses]
  (let [clauses (apply concat clauses)]
    (if (not (empty? (rest clauses)))
      (cons 'and (distinct (mapcat #(case (first %) and (rest %) [%]) clauses)))
      (first clauses))))

(defn ^:no-doc datomify-where-clause [clause]
  (match clause
    ([(rator :guard #(case % (not or and) true false)) & clauses] :seq) ; not / or / and
    (cons rator (map datomify-where-clause clauses))
    ([(rator :guard #(case % (not-join or-join) true false)) ([& syms] :seq) & clauses] :seq) ; not-join / or-join
    (cons rator (cons syms (map datomify-where-clause clauses)))
    ([(lhs :guard seq?) rhs] :seq) ; fn-expr
    clause
    ([(lhs :guard symbol?) {:spec-tacular/spec spec-name}] :seq) ; spec-tacular rhs
    (let [spec (get-spec spec-name)
          rhs (second clause)]
      (combine-where-clauses
       [[lhs :spec-tacular/spec spec-name]]
       (for [[k v] (dissoc rhs :spec-tacular/spec)]
         (do (assert (not (seq? v)))
             [lhs (db-keyword spec k) v]))))
    ([([(rator :guard symbol?) & rands] :seq)] :seq) ; predicate expression
    clause
    :else (assert false)))

(def ^:const aggregate?
  #{'min 'max 'count 'count-distinct 'sum 'avg 'median 'variance 'stddev 'distinct 'rand 'sample})

(def ^:const protected?
  (conj aggregate? 'pull 'spec-pull 'instance))

(defn datomify-find-elem [find-elem]
  (let [identity-rebuild (fn [db result] result)]
    (match find-elem
      (['spec-pull (arg :guard symbol?) spec pattern] :seq)
      (let [spec (get-spec spec)
            {:keys [datomic-pattern rebuild]} (datomify-spec-pattern spec pattern)]
        {:datomic-find-elem (list 'pull arg datomic-pattern)
         :rebuild rebuild})
      (['instance spec-name (arg :guard symbol?)] :seq)
      {:datomic-find-elem arg
       :rebuild
       (fn [db result]
         (let [spec (get-spec spec-name)
               err #(throw (ex-info "possible spec mismatch"
                                    {:actual-type   (type result)
                                     :expected-type spec-name
                                     :query-result  result}))]
           (if (and (primitive? spec-name)
                    (not (instance? EnumSpec spec)))
             (if (= spec-name :calendarday)
               (if (instance? java.util.Date arg)
                 (timec/to-date-time result) (err))
               (if (instance? (get-spec-class spec-name) result)
                 result (err)))
             (if (instance? java.lang.Long result)
               (recursive-ctor spec-name (d/entity db result)) (err)))))}
      ([(rator :guard protected?) & args] :seq)
      {:datomic-find-elem (cons rator args)
       :rebuild identity-rebuild}
      ([(rator :guard #(ns-resolve *ns* %)) & args] :seq) ; custom aggregate
      {:datomic-find-elem (cons rator args)
       :rebuild identity-rebuild}
      (arg :guard symbol?)
      {:datomic-find-elem arg
       :rebuild identity-rebuild}
      :else (throw (ex-info "bad find element, expecting symbol, keyword, or sequence"
                            {:find-element find-elem})))))

(defn datomify-find-elems [find-elems]
  (match find-elems
    ([elem '.] :seq) ; scalar
    (let [{:keys [datomic-find-elem rebuild]} (datomify-find-elem elem)]
      {:datomic-find (list datomic-find-elem '.)
       :rebuild (fn [db result] (rebuild db result))})
    ([([elem '...] :seq)] :seq) ; coll
    (let [{:keys [datomic-find-elem rebuild]} (datomify-find-elem elem)]
      {:datomic-find (list [datomic-find-elem '...])
       :rebuild (fn [db result] (set (mapv rebuild (repeat db) result)))})
    ([([& (elems :guard #(not (protected? (first %))))] :seq)] :seq) ; tuple
    (let [rec (map datomify-find-elem elems)]
      {:datomic-find (list (mapv :datomic-find-elem rec))
       :rebuild (fn [db results] (mapv #(%1 db %2) (map :rebuild rec) results))})
    ([& elems] :seq) ; relation
    (let [rec (map datomify-find-elem elems)]
      {:datomic-find (map :datomic-find-elem rec)
       :rebuild (fn [db results]
                  (set (mapv (fn [results] (mapv #(%1 db %2) (map :rebuild rec) results)) results)))})
    :else (throw (ex-info "expecting find specification for relation, coll, tuple, or scalar"
                          {:syntax find-elems}))))

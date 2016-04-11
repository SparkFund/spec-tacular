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
                                    EnumSpec
                                    UnionSpec)
           (org.joda.time DateTime)))

(def ^:const aggregate?
  #{'min 'max 'count 'count-distinct 'sum 'avg 'median 'variance 'stddev 'distinct 'rand 'sample})

(def ^:const protected?
  (conj aggregate? 'pull 'spec-pull 'instance))

(def aggregate-types
  {'min (fn [t] t)
   'max (fn [t] t)
   'count (fn [_] `t/Integer)
   'count-distinct (fn [_] `t/Integer)
   'sum (fn [t] t)
   'avg (fn [t] `t/Num)})

;; ===================================================================================================
;; static

(defn set-type! [tenv x t]
  (if-let [t- (get @tenv x)]
    (when-not (or (= t t-)
                  (if-let [elems (:elements (get-spec t))]
                    (contains? elems t-)))
      (throw (ex-info "var has two incompatible return types" {:x x :type1 t :type2 t-})))
    (do (swap! tenv assoc x t) nil)))

(defn ^:no-doc expand-find-elem
  "Expands a single find element, returning an `:expanded-find-elem`
  suitable for `query` and a `:build-ret-type` that takes one
  argument, a type lookup function, and returns a suitable
  `core.typed` type."
  [tenv uenv find-elem]
  (letfn [(record-spec [spec-name]
            (let [spec (get-spec spec-name)
                  %n   (symbol (str "%" (inc (count @uenv))))
                  new  (gensym (str "?" (lower-case (name spec-name))))]
              (set-type! tenv new spec-name)
              (swap! uenv assoc %n new)
              new))]
    (match find-elem
      (arg :guard symbol?)
      {:expanded-find-elem `'~arg
       :build-ret-type (fn [lookup] (lookup arg))}
      (['instance (spec-name :guard keyword?) (arg :guard symbol?)] :seq)
      {:expanded-find-elem (list 'list ''instance spec-name `'~arg)
       :build-ret-type (fn [lookup] (lookup arg))}
      (spec-name :guard keyword?)
      (let [%n  (symbol (str "%" (inc (count @uenv))))
            new (gensym (str "?" (lower-case (name spec-name))))
            find-elem (list 'list ''instance spec-name `'~new)]
        (set-type! tenv new spec-name)
        (swap! uenv assoc %n new)
        {:expanded-find-elem (list 'list ''instance spec-name `'~new)
         :build-ret-type (fn [lookup] (lookup spec-name))})
      (['pull (spec-name :guard keyword?) pattern] :seq)
      ,(let [new (record-spec spec-name)]
         {:expanded-find-elem (list 'list ''spec-pull `'~new spec-name pattern)
          :build-ret-type (fn [lookup] `(t/Map t/Any t/Any))})
      ([(rator :guard protected?) & args] :seq) ; aggregate
      ,(let [arg (last args)
             new (if (symbol? arg) arg (record-spec arg))]
         {:expanded-find-elem (cons 'list (cons `'~rator (reverse (cons `'~new (rest (reverse args))))))
          :build-ret-type (fn [lookup] ((get aggregate-types rator) (lookup arg)))})
      ([(rator :guard #(ns-resolve *ns* %)) & args] :seq) ; custom aggregate
      ,(let [arg (last args)
             new (if (symbol? arg) arg (record-spec arg))]
         {:expanded-find-elem (cons 'list (cons `'~rator (reverse (cons `'~new (rest (reverse args))))))
          :build-ret-type (fn [lookup] `t/Any)})
      :else (throw (ex-info "bad find element, expecting symbol, keyword, or sequence"
                            {:find-element find-elem})))))

(defn ^:no-doc expand-find-elems
  "Expands an entire find expression, returning a `:find-expr`
  suitable for `query` and a `:build-ret-type` function that takes one
  argument, a type lookup function, and returns a suitable
  `core.typed` type."
  [tenv uenv find-elems]
  (match find-elems
    ([elem '.] :seq) ; scalar
    (let [{:keys [expanded-find-elem build-ret-type]} (expand-find-elem tenv uenv elem)]
      {:find-expr (list 'list expanded-find-elem ''.)
       :build-ret-type (fn [lookup] (build-ret-type lookup))})
    ([([elem '...] :seq)] :seq) ; coll
    (let [{:keys [expanded-find-elem build-ret-type]} (expand-find-elem tenv uenv elem)]
      {:find-expr (list 'list [expanded-find-elem ''...])
       :build-ret-type (fn [lookup] `(t/Set ~(build-ret-type lookup)))})
    ([([& (elems :guard #(not (protected? (first %))))] :seq)] :seq) ; tuple
    (let [rec (mapv (partial expand-find-elem tenv uenv) elems)]
      {:find-expr (cons 'list (list (mapv :expanded-find-elem rec)))
       :build-ret-type (fn [lookup]
                         (mapv (comp #(% lookup) :build-ret-type) rec))})
    ([& elems] :seq) ; relation
    (let [rec (mapv (partial expand-find-elem tenv uenv) elems)]
      {:find-expr (cons 'list (map :expanded-find-elem rec))
       :build-ret-type (fn [lookup]
                         (let [type-vec (mapv (comp #(% lookup) :build-ret-type) rec)]
                           `(t/Set (t/HVec ~type-vec))))})
    :else (throw (ex-info "expecting find specification for relation, coll, tuple, or scalar"
                          {:syntax find-elems}))))

(declare expand-spec-where-clause)

(defn expand-item [tenv sub-spec-name k v]
  (cond
    (map? v)
    ,(let [rec' (expand-spec-where-clause tenv [sub-spec-name v])]
       {:map-entry [[k (ffirst rec')]]
        :clause rec'})
    (vector? v)
    ,(expand-item tenv (first v) k (second v))
    (and (symbol? v) (= \? (first (str v))))
    ,(do (set-type! tenv v sub-spec-name)
         (let [t (get @tenv v)]
           (if (primitive? t)
             {:map-entry [[k `'~v]]}
             {:map-entry [[k `'~v]]
              :clause [[`'~v :spec-tacular/spec t]]})))
    (and (keyword? v)
         (not (instance? EnumSpec (get-spec sub-spec-name)))
         (not (= :keyword sub-spec-name)))
    (let [v' (gensym (str "?" (lower-case (name v))))]
      {:map-entry [[k `'~v']]
       :clause [[`'~v' :spec-tacular/spec v]]})
    :else
    {:map-entry [[k v]]}))

(defn expand-map
  "Expands a single map in the right-hand-side of a spec clause,
  merging all the sub-map-entries and clauses."
  [tenv spec m]
  (->> (for [[k v] m]
         (if-let [{[arity sub-spec-name] :type} (get-item spec k)]
           (expand-item tenv sub-spec-name k v)
           (throw (ex-info "keyword not in spec" {:k k :spec spec}))))
       (apply merge-with concat)))

(defn expand-spec-map-clause
  "Expands a spec map clause `[lhs rhs]`, where `lhs` is a symbol and
  `rhs` is a map."
  [tenv spec lhs rhs]
  (cond
    (instance? Spec spec)
    (let [rec (expand-map tenv spec rhs)]
      (vec (concat [[`'~lhs (into {:spec-tacular/spec (:name spec)} (:map-entry rec))]]
                   (:clause rec))))
    (instance? UnionSpec spec)
    (let [rec (->> (:elements spec)
                   (mapv (fn [spec-name]
                           (try {:syntax {spec-name (expand-spec-map-clause tenv (get-spec spec-name) lhs rhs)}}
                                (catch clojure.lang.ExceptionInfo e {:error {spec-name e}}))))
                   (apply merge-with merge))]
      (when (empty? (:syntax rec))
        (throw (ex-info "does not conform to any possible unioned spec"
                        {:syntax rhs :errors (:errors rec)})))
      (doseq [[spec-name e] (:error rec)]
        (case (.getMessage e) "var has two incompatible return types" (throw e) nil))
      [(cons 'list (cons ''or (map #(cons 'list (cons ''and %)) (vals (:syntax rec)))))])))

(defn expand-spec-where-clause
  "Expands a spec where clause, where `lhs` can still be either a
  symbol or a keyword.  Passes off to `expand-spec-map-clause`."
  [tenv clause]
  (match clause
    [(sym :guard symbol?) (rhs :guard map?)] ; no spec name
    (let [spec-name (get @tenv sym)
          spec (or (get-spec spec-name) (get-spec rhs))]
      (when-not spec
        (throw (ex-info "could not infer type" {:sym sym :syntax clause})))
      (expand-spec-map-clause tenv spec sym rhs))
    [(spec-name :guard keyword?) (rhs :guard map?)] ; with spec-name on lhs
    (let [spec (get-spec spec-name)
          x-gs (gensym (str "?" (lower-case (name spec-name))))]
      (expand-spec-map-clause tenv spec x-gs rhs))
    [(lhs :guard symbol?) [(spec-name :guard keyword?) (rhs :guard map?)]]
    (do (when-not (get-spec spec-name)
          (throw (ex-info "where clause matches spec-tacular syntax but keyword is not a spec-name"
                          {:keyword spec-name :clause clause})))
        (set-type! tenv lhs spec-name)
        (expand-spec-where-clause tenv [lhs rhs]))
    :else (throw (ex-info "invalid where clause" {:syntax clause}))))

(defn wrap-variable [x]
  (if (and (symbol? x) (= \? (first (str x))))
    `'~x x))

(defn expand-where-clause
  "Expands any where clause.  Leaves most as is, except for spec
  where-clauses, which are handled by `expand-spec-where-clause`."
  [tenv clause]
  (match clause
    [lhs (rhs :guard map?)]
    (expand-spec-where-clause tenv clause)
    [lhs [(spec-name :guard keyword?) (rhs :guard map?)]]
    (expand-spec-where-clause tenv clause)
    ([(rator :guard #(case % (not datomic-or datomic-and) true false)) & clauses] :seq) ; not / or / and
    (let [clauses' (mapcat (partial expand-where-clause tenv) clauses)]
      [(cons 'list (cons (case rator datomic-or ''or datomic-and ''and `'~rator) clauses'))])
    ([(rator :guard #(case % (not-join or-join) true false)) ([& syms] :seq) & clauses] :seq) ; not-join / or-join
    [(cons rator (cons syms (map expand-where-clause clauses)))]
    ([([rator & args] :seq) rhs] :seq) ; fn-expr
    [[(apply list 'list `'~rator (mapv wrap-variable args)) (wrap-variable rhs)]]
    ([([rator & args] :seq)] :seq) ; pred-expr
    [[(apply list 'list `'~rator (mapv wrap-variable args))]]
    ([(rator :guard symbol?) & (args :guard #(every? (complement seq?) %))] :seq) ; data pattern
    [(mapv wrap-variable args)]
    :else (throw (ex-info "invalid where clause" {:syntax clause}))))

;; (q :find find-expr+ :in clojure-expr :where clause+)
;; find-expr = ident
;; ident     = spec-name
;;           | datomic-variable
;;             | [datomic-variable spec-name];
; clause    = [ident map]
;; map       = % | %n | spec-name
;;           | {:kw (clause | map | ident | value),+}
(defn parse-query [stx]
  (let [keyword? #{:find :in :where}
        partitions (partition-by (fn [stx] (keyword? stx)) stx)]
    (match partitions ;; ((:find) (1 2 ....) (:in) (3) (:where) (4 5 ....))
      ([([:find] :seq) f ([:in] :seq) in ([:where] :seq) wc] :seq)
      (let [tenv (atom {}) 
            uenv (atom {})
            {:keys [find-expr build-ret-type]} (expand-find-elems tenv uenv f)
            ;; after uenv populated
            bindings (apply concat '[% %1] '[or datomic-or] '[and datomic-and] (vec @uenv))
            do-expr (m/mexpand `(m/symbol-macrolet ~bindings ~wc)) ; expands with a `do`
            clauses (second do-expr)
            where-expr (mapcat (partial expand-where-clause tenv) clauses)
            ;; after tenv populated
            lookup (fn [x] (or (some-> (if (symbol? x) (get @tenv x) x) get-type :type-symbol) `t/Any))
            query-ret-type (build-ret-type lookup)]
        {:find-expr find-expr
         :db-expr (first in)
         :in-expr (cons 'list (rest in))
         :where-expr (cons 'list where-expr)
         :query-ret-type query-ret-type})
      :else
      (throw (ex-info "expecting keywords :find, :in, and :where followed by arguments"
                      {:syntax partitions})))))

;; ===================================================================================================
;; dynamic

;; From http://docs.datomic.com/query.html
;; where-clauses     = ':where' clause+
;; clause            = (not-clause | not-join-clause | or-clause | or-join-clause | expression-clause)
;; not-clause        = [ src-var? 'not' clause+ ]
;; not-join-clause   = [ src-var? 'not-join' [variable+] clause+ ]
;; or-clause         = [ src-var? 'or' (clause | and-clause)+]
;; or-join-clause    = [ src-var? 'or-join' rule-vars (clause | and-clause)+ ]
;; and-clause        = [ 'and' clause+ ]
;; expression-clause = (data-pattern | pred-expr | fn-expr | rule-expr)
;; data-pattern      = [ src-var? (variable | constant | '_')+ ]
;; pred-expr         = [ [pred fn-arg+] ]
;; fn-expr           = [ [fn fn-arg+] binding ]
;; binding           = (bind-scalar | bind-tuple | bind-coll | bind-rel)
;; bind-scalar       = variable
;; bind-tuple        = [ (variable | '_')+]
;; bind-coll         = [variable '...']
;; bind-rel          = [ [(variable | '_')+] ]

(defn combine-where-clauses [& clauses]
  (if (not (empty? (rest clauses)))
    (cons 'and (distinct (mapcat #(case (first %) and (rest %) [%]) clauses)))
    (first clauses)))

(defn or-clause [& clauses]
  (if (empty? (rest clauses)) (first clauses) (cons 'or clauses)))

(defn datomify-spec-where-clause [spec-name lhs rhs]
  (let [spec (get-spec spec-name)]
    (cond
      (instance? Spec spec)
      (apply combine-where-clauses
             [lhs :spec-tacular/spec spec-name]
             (for [[k v] (dissoc rhs :spec-tacular/spec)
                   :let [{[arity sub-spec-name] :type :as item} (get-item spec k)
                         db-kw (db-keyword spec k)]]
               (cond (nil? v)
                     (throw (ex-info "can't have nil args in clause"
                                     {:syntax rhs}))
                     (symbol? v)
                     [lhs db-kw v]
                     (and (not (primitive? sub-spec-name))
                          (= (:name (get-spec v) sub-spec-name)))
                     (let [sub-spec (get-spec sub-spec-name)]
                       (if-let [eid (get-in v [:db-ref :eid])]
                         [lhs db-kw eid]
                         (when-let [unique-item (some #(when (and (:unique? %) (:identity? %)) %)
                                                      (:items sub-spec))]
                           (when-some [v' ((:name unique-item) v)]
                             (let [gs (gensym '?tmp)]
                               (combine-where-clauses
                                [lhs db-kw gs]
                                [gs (db-keyword sub-spec (:name unique-item)) v']))))))
                     (and (= sub-spec-name :calendarday)
                          (instance? DateTime v))
                     [lhs db-kw (timec/to-date v)]
                     :else
                     [lhs db-kw v])))
      (instance? UnionSpec spec)
      (apply or-clause
             (for [sub-spec-name (:elements spec)]
               (datomify-spec-where-clause sub-spec-name lhs rhs))))))

(defn datomify-where-clause [clause]
  (match clause
    ([(rator :guard #(case % (not or and) true false)) & clauses] :seq) ;; not / or / and
    (let [clauses (map datomify-where-clause clauses)]
      (case rator
        (or) (apply or-clause clauses)
        (and) (apply combine-where-clauses clauses)
        (not) (list 'not (apply combine-where-clauses clauses))))
    ([(rator :guard #(case % (not-join or-join) true false)) ([& syms] :seq) & clauses] :seq) ;; not-join / or-join
    (cons rator (cons syms (map datomify-where-clause clauses)))
    ([(lhs :guard seq?) rhs] :seq) ;; fn-expr
    clause
    ([(lhs :guard symbol?) {:spec-tacular/spec spec-name}] :seq) ;; spec-tacular rhs
    (datomify-spec-where-clause spec-name lhs (second clause))
    ([([(rator :guard symbol?) & rands] :seq)] :seq) ;; predicate expression
    clause
    ([& (args :guard #(every? (complement seq?) %))] :seq) ;; data pattern
    clause
    :else (throw (ex-info "invalid where clause" {:clause clause}))))

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
               err #(throw (ex-info "unexpected type returned from Datomic"
                                    {:actual-type   (type result)
                                     :expected-type spec-name
                                     :query-result  result}))]
           (if (and (primitive? spec-name)
                    (not (instance? EnumSpec spec)))
             (if (= spec-name :calendarday)
               (if (instance? java.util.Date result)
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

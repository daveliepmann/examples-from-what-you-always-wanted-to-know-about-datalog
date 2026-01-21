(ns analysis
  "Further analysis and exploration of the queries in `examples`"
  (:require
   [datomic.api :as d]
   [examples]))

;; Let's look at the query-stats for the queries in `examples`.

;;;; Database setup: same-same but different
;; To get query-stats we need a dev DB with a separately running transactor

(def db-uri "datomic:dev://localhost:4334/cousin")

(d/create-database db-uri)

(def conn (d/connect db-uri))

;; data-wise the DB is identical
(do (d/transact conn examples/cousins-schema)
    (d/transact conn (map #(hash-map :person/name %) examples/persons))
    (d/transact conn (map examples/parent+child->txd
                          ;; don't model a parent/child relation for fred
                          (filter (comp pos? dec count)
                                  examples/child+parent-rels))))

;;;; Query-stats of same-generation-cousins general case

;; adapt `q` to `query` with arg-map to get query-stats
(->> (d/query {:query '[:find ?x-name ?y-name
                        :in $ %
                        :where (sgc ?x ?y)
                        [?x :person/name ?x-name]
                        [?y :person/name ?y-name]]
               :args [(d/db conn) [examples/sgc-rule]]
               :query-stats true})
     ;; massage the data for easier viewing
     :query-stats
     :phases
     (map-indexed #(assoc %2 :phase %1)))

;; => 
'({:sched ;; Our query, as we gave it to the datalog engine
   (((sgc ?x ?y) [?x :person/name ?x-name] [?y :person/name ?y-name])),
   :clauses
   [{:clause (sgc ?x ?y),
     :rows-in 0,
     :rows-out 16, ; this looks like the final result set, bubbled up from the recursion
     :binds-in (),
     :binds-out [?x ?y],
     :expansion 16}
    {:clause [?x :person/name ?x-name],
     :rows-in 16,
     :rows-out 16,
     :binds-in [?x ?y],
     :binds-out [?x-name ?y]}
    {:clause [?y :person/name ?y-name],
     :rows-in 16,
     :rows-out 16,
     :binds-in [?x-name ?y],
     :binds-out [?x-name ?y-name]}],
   :phase 0}

  {:sched (((or-join [?x ?y]))), ; this appears to be part of sgc in the first phase?
   :clauses
   [{:clause (or-join [?x ?y]),
     :rows-in 0,
     :rows-out 16,
     :binds-in (),
     :binds-out [?x ?y],
     :expansion 16}],
   :phase 1}
  
  {:sched ;; The first `sgc` invokation
   (([?x :person/name _] [?y :person/name _] [(= ?x ?y)])
    ([?x1 :parent/of ?x] (sgc ?x1 ?y1) [?y1 :parent/of ?y])),
   :clauses
   [{:clause [?x :person/name _],
     :rows-in 0,
     :rows-out 8, ; there are 8 persons
     :binds-in (),
     :binds-out [?x],
     :expansion 8,
     :warnings {:unbound-vars #{?x}}}
    {:clause [?y :person/name _],
     :rows-in 8,
     :rows-out 8, ; there are 8 persons who can be both x and y (see predicate)
     :binds-in [?x],
     :binds-out [?x ?y],
     :preds ([(= ?x ?y)]), ; predicative clause gets pushed into the clause that provides its input
     :warnings {:unbound-vars #{?y}}}
    ;; or-join boundary --------------------------------------------------------
    ;; Despite being in the same phase, clauses above and below this point
    ;; belong to separate branches. This is why the bindings reset to ().
    {:clause [?x1 :parent/of ?x],
     :rows-in 0,
     :rows-out 6, ; there are six parent relations: g:d, g:e, d:b, d:a, h:a, e:c
     :binds-in (),
     :binds-out [?x ?x1],
     :expansion 6,
     :warnings {:unbound-vars #{?x ?x1}}}
    {:clause (sgc ?x1 ?y1),
     :rows-in 6,
     :rows-out 9, ; ??? this confuses me
     ;; I see 14 x1,x,y1,y combinations based on the six parent/child rows we have in hand:
     ;; x1 x,y1 y
     ;;  g d g  e
     ;;  g e g  d
     ;;  d b d  a
     ;;  d b h  a
     ;;  d b e  c
     ;;  d a d  b
     ;;  d a h  a
     ;;  d a e  c
     ;;  h a d  b
     ;;  h a d  a
     ;;  h a e  c
     ;;  e c d  b
     ;;  e c d  a
     ;;  e c h  a
     :binds-in [?x ?x1],
     :binds-out [?y1 ?x]}
    {:clause [?y1 :parent/of ?y],
     :rows-in 9,
     :rows-out 13, ; ???
     :binds-in [?y1 ?x],
     :binds-out [?x ?y],
     :expansion 4}],
   :phase 2}

  {:sched (((or-join [?x ?y]))), ; ??? not sure where we're coming from
   :clauses
   [{:clause (or-join [?x ?y]),
     :rows-in 4, ; ??? which 4 x bindings do we receive? abcf (leaves)?
     :rows-out 6, ; ??? not clear which rows-in this is piped to
     :binds-in (?x), ; ??? why is y not already bound?
     :binds-out [?x ?y],
     :expansion 2}],
   :phase 3}

  {:sched ; ??? not sure where this is coming from
   ;; The rows-in doesn't match previous phase.
   ;; Phase 13 is a likely candidate.
   (([?x :person/name _] [?y :person/name _] [(= ?x ?y)])
    ([?x1 :parent/of ?x] (sgc ?x1 ?y1) [?y1 :parent/of ?y])),
   :clauses
   [{:clause [?x :person/name _],
     :rows-in 4, 
     :rows-out 4,
     :binds-in (?x),
     :binds-out [?x]}
    {:clause [?y :person/name _],
     :rows-in 4,
     :rows-out 4,
     :binds-in [?x],
     :binds-out [?x ?y],
     :preds ([(= ?x ?y)]),
     :warnings {:unbound-vars #{?y}}}
    ;; or-join boundary --------------------------------------------------------
    {:clause [?x1 :parent/of ?x],
     :rows-in 4,
     :rows-out 2,
     :binds-in (?x),
     :binds-out [?x ?x1]}
    {:clause (sgc ?x1 ?y1),
     :rows-in 2,
     :rows-out 2,
     :binds-in [?x ?x1],
     :binds-out [?y1 ?x]}
    {:clause [?y1 :parent/of ?y],
     :rows-in 2,
     :rows-out 4,
     :binds-in [?y1 ?x],
     :binds-out [?x ?y],
     :expansion 2}],
   :phase 4}
  
  {:sched ; Why are we revisiting our original query clauses?
   ;; This phase is identical to phase 0.
   (((sgc ?x ?y) [?x :person/name ?x-name] [?y :person/name ?y-name])),
   :clauses
   [{:clause (sgc ?x ?y),
     :rows-in 0,
     :rows-out 16,
     :binds-in (),
     :binds-out [?x ?y],
     :expansion 16}
    {:clause [?x :person/name ?x-name],
     :rows-in 16,
     :rows-out 16,
     :binds-in [?x ?y],
     :binds-out [?x-name ?y]}
    {:clause [?y :person/name ?y-name],
     :rows-in 16,
     :rows-out 16,
     :binds-in [?x-name ?y],
     :binds-out [?x-name ?y-name]}],
   :phase 5}

  {:sched (((or-join [?x ?y]))), ;  Identical to phase 1
   :clauses
   [{:clause (or-join [?x ?y]),
     :rows-in 0,
     :rows-out 16,
     :binds-in (),
     :binds-out [?x ?y],
     :expansion 16}],
   :phase 6}
  
  {:sched ; identical to phase 2
   (([?x :person/name _] [?y :person/name _] [(= ?x ?y)])
    ([?x1 :parent/of ?x] (sgc ?x1 ?y1) [?y1 :parent/of ?y])),
   :clauses
   [{:clause [?x :person/name _],
     :rows-in 0,
     :rows-out 8,
     :binds-in (),
     :binds-out [?x],
     :expansion 8,
     :warnings {:unbound-vars #{?x}}}
    {:clause [?y :person/name _],
     :rows-in 8,
     :rows-out 8,
     :binds-in [?x],
     :binds-out [?x ?y],
     :preds ([(= ?x ?y)]),
     :warnings {:unbound-vars #{?y}}}
    ;; or-join boundary --------------------------------------------------------
    {:clause [?x1 :parent/of ?x],
     :rows-in 0,
     :rows-out 6,
     :binds-in (),
     :binds-out [?x ?x1],
     :expansion 6,
     :warnings {:unbound-vars #{?x ?x1}}}
    {:clause (sgc ?x1 ?y1),
     :rows-in 6,
     :rows-out 9,
     :binds-in [?x ?x1],
     :binds-out [?y1 ?x]}
    {:clause [?y1 :parent/of ?y],
     :rows-in 9,
     :rows-out 13,
     :binds-in [?y1 ?x],
     :binds-out [?x ?y],
     :expansion 4}],
   :phase 7}

  {:sched (((or-join [?x ?y]))), ; identical to phase 3
   :clauses
   [{:clause (or-join [?x ?y]),
     :rows-in 4,
     :rows-out 6,
     :binds-in (?x),
     :binds-out [?x ?y],
     :expansion 2}],
   :phase 8}
  
  {:sched ; identical to phase 4
   (([?x :person/name _] [?y :person/name _] [(= ?x ?y)])
    ([?x1 :parent/of ?x] (sgc ?x1 ?y1) [?y1 :parent/of ?y])),
   :clauses
   [{:clause [?x :person/name _],
     :rows-in 4,
     :rows-out 4,
     :binds-in (?x),
     :binds-out [?x]}
    {:clause [?y :person/name _],
     :rows-in 4,
     :rows-out 4,
     :binds-in [?x],
     :binds-out [?x ?y],
     :preds ([(= ?x ?y)]),
     :warnings {:unbound-vars #{?y}}}
    ;; or-join boundary --------------------------------------------------------
    {:clause [?x1 :parent/of ?x],
     :rows-in 4,
     :rows-out 2,
     :binds-in (?x),
     :binds-out [?x ?x1]}
    {:clause (sgc ?x1 ?y1),
     :rows-in 2,
     :rows-out 2,
     :binds-in [?x ?x1],
     :binds-out [?y1 ?x]}
    {:clause [?y1 :parent/of ?y],
     :rows-in 2,
     :rows-out 4,
     :binds-in [?y1 ?x],
     :binds-out [?x ?y],
     :expansion 2}],
   :phase 9}
  
  {:sched ; Like phase 0 and 5, but _not_ identical.
   (((sgc ?x ?y) [?x :person/name ?x-name] [?y :person/name ?y-name])),
   :clauses
   [{:clause (sgc ?x ?y),
     :rows-in 0,
     :rows-out 12, ; (was 16 in phases 0 & 5)
     :binds-in (), ; ??? how do we exclude 4 rows, given we lack inbound bindings? Perhaps the working-set database at this phase does not include the leaf persons a,b,c,f
     :binds-out [?x ?y],
     :expansion 12}
    {:clause [?x :person/name ?x-name],
     :rows-in 12,
     :rows-out 12,
     :binds-in [?x ?y],
     :binds-out [?x-name ?y]}
    {:clause [?y :person/name ?y-name],
     :rows-in 12,
     :rows-out 12,
     :binds-in [?x-name ?y],
     :binds-out [?x-name ?y-name]}],
   :phase 10}
  
  {:sched (((or-join [?x ?y]))), ; continuation of phase 10
   :clauses
   [{:clause (or-join [?x ?y]),
     :rows-in 0,
     :rows-out 12,
     :binds-in (),
     :binds-out [?x ?y],
     :expansion 12}],
   :phase 11}
  
  {:sched ; continuation of 10 & 11
   (([?x :person/name _] [?y :person/name _] [(= ?x ?y)])
    ([?x1 :parent/of ?x] (sgc ?x1 ?y1) [?y1 :parent/of ?y])),
   :clauses
   [{:clause [?x :person/name _],
     :rows-in 0,
     :rows-out 8,
     :binds-in (),
     :binds-out [?x],
     :expansion 8,
     :warnings {:unbound-vars #{?x}}}
    {:clause [?y :person/name _],
     :rows-in 8,
     :rows-out 8,
     :binds-in [?x],
     :binds-out [?x ?y],
     :preds ([(= ?x ?y)]),
     :warnings {:unbound-vars #{?y}}}
    ;; or-join boundary --------------------------------------------------------
    {:clause [?x1 :parent/of ?x],
     :rows-in 0,
     :rows-out 6,
     :binds-in (),
     :binds-out [?x ?x1],
     :expansion 6,
     :warnings {:unbound-vars #{?x ?x1}}}
    {:clause (sgc ?x1 ?y1),
     :rows-in 6,
     :rows-out 6, ; ??? 
     :binds-in [?x ?x1],
     :binds-out [?y1 ?x]}
    {:clause [?y1 :parent/of ?y],
     :rows-in 6,
     :rows-out 9,
     :binds-in [?y1 ?x],
     :binds-out [?x ?y],
     :expansion 3}],
   :phase 12}
  
  {:sched (((or-join [?x ?y]))), ; seems related to the following phase
   :clauses
   [{:clause (or-join [?x ?y]),
     :rows-in 4,
     :rows-out 4,
     :binds-in (?x),
     :binds-out [?x ?y]}],
   :phase 13}
  
  {:sched
   ;; Since x is already bound and (spoiler alert) the first/top/identity branch
   ;; of this or-join wins, this appears to be the phase where we receive the
   ;; parents (g,d,h,e) as x-binding candidates for same-generation-cousins (of
   ;; themselves). By confirming that we reach the base case of our recursive
   ;; stack.
   (([?x :person/name _] [?y :person/name _] [(= ?x ?y)])
    ([?x1 :parent/of ?x] (sgc ?x1 ?y1) [?y1 :parent/of ?y])),
   :clauses
   [{:clause [?x :person/name _],
     :rows-in 4,
     :rows-out 4,
     :binds-in (?x), ; ??? shouldn't y be bound too, as or-join/sgc invocation bind it?
     :binds-out [?x]}
    {:clause [?y :person/name _],
     :rows-in 4,
     :rows-out 4,
     :binds-in [?x],
     :binds-out [?x ?y],
     :preds ([(= ?x ?y)]),
     :warnings {:unbound-vars #{?y}}}
    ;; or-join boundary --------------------------------------------------------
    {:clause [?x1 :parent/of ?x],
     :rows-in 4, ; g,d,h,e each as x, I suppose?
     :rows-out 2, ; g:d, g:e (g has no parents)
     :binds-in (?x),
     :binds-out [?x ?x1]}
    {:clause (sgc ?x1 ?y1),
     :rows-in 2, ; g:d, g:e
     :rows-out 0, ; g has no parents (not sure how this works mechanically)
     :binds-in [?x ?x1],
     :binds-out [?y1 ?x]}
    {:clause [?y1 :parent/of ?y],
     :rows-in 0,
     :rows-out 0,
     :binds-in [?y1 ?x],
     :binds-out [?x ?y]}],
   :phase 14})

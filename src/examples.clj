(ns examples
  "Datomic-flavored Datalog examples from Ceri, Gottlob, & Tanca 1989
   - same-generation cousins
   - siblings"
  (:require [arrowic.core :as arrowic] ; for diagram
            [datomic.client.api :as d]))

;;;; Database setup

(def client (d/client {:server-type :datomic-local
                       :system "datomic-samples"}))

(d/create-database client {:db-name "cousins"})

(def conn (d/connect client {:db-name "cousins"}))

(comment ; for data emergencies
  (d/delete-database client {:db-name "cousins"})

  )


;;;; The data

;; "Consider a database consisting of two relations with respective schemes
;; PERSON(NAME) and PAR(CHILD, PARENT). The first contains the names of persons
;; and the second expresses a parent relationship between persons."

;; PERSON = { < ann >, < bertrand >, < charles >,
;;            < dorothy >, < evelyn >, < fred >,
;;            < george >, < hilary > }

;; PAR = { < dorothy, george >, < evelyn, george >,
;;         < bertrand, dorothy >, < ann, dorothy >,
;;         < ann, hilary >, < charles, evelyn >

;; We could model the paper's data like so:
(def cousins-schema
  [{:db/ident :person/name
    :db/valueType :db.type/string
    :db/unique :db.unique/identity
    :db/cardinality :db.cardinality/one}
   {:db/ident :parent/of
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/many}])

(d/transact conn {:tx-data cousins-schema})


(def persons ["ann" "bertrand" "charles"
              "dorothy" "evelyn" "fred"
              "george" "hilary"])

(d/transact conn {:tx-data (map #(hash-map :person/name %) persons)})

(def child+parent-rels
  [["fred"]
   ["dorothy" "george"]
   ["evelyn" "george"]
   ["bertrand" "dorothy"]
   ["ann" "dorothy"]
   ["ann" "hilary"]
   ["charles" "evelyn"]])


(comment ; I found a diagram helpful at this point
  (def family-tree
    (arrowic/graph-from-seqs (map reverse child+parent-rels)))

  (arrowic/create-viewer family-tree)

  (spit "family-tree.svg" (arrowic/as-svg family-tree))
  )


(defn parent+child->txd [[child parent]]
  [:db/add [:person/name parent] :parent/of [:person/name child]])

(d/transact conn {:tx-data (map parent+child->txd
                                ;; don't model a parent/child relation for fred
                                (filter (comp pos? dec count)
                                        child+parent-rels))})


;;;; Defining a same-generation cousins rule

;; "Let P1 be a Datalog program consisting of the following clauses:"
;;
;; r1: sgc(X, X) :- person(X).
;; r2: sgc(X, Y) :- par(X, X1), sgc(X1, Y1), par(Y, Y1)
;;
;; Rule r2 is recursive and states that two persons are same generation cousins
;; whenever they have parents which are in turn same generation cousins.

(def sgc-rule
  "Named query logic to find same-generation cousins (SGCs)."
  '[(sgc ?x ?y)
    (or-join [?x ?y]
             (and                       ; r1
               [?x :person/name _]
               [?y :person/name _]
               [(= ?x ?y)])
             (and                       ; r2
               [?x1 :parent/of ?x]
               (sgc ?x1 ?y1) ; recursive
               [?y1 :parent/of ?y]))])

;;; same-generation cousins: naively / all permutations
(d/q '[:find ?x-name ?y-name
       :in $ %
       :where (sgc ?x ?y)
       [?x :person/name ?x-name]
       [?y :person/name ?y-name]]
     (d/db conn)
     [sgc-rule])

;; same-generation cousins: all permutations (without single persons)
(->> (d/q '[:find ?x-name ?y-name
            :in $ %
            :where
            (sgc ?x ?y)
            [?x :person/name ?x-name]
            [?y :person/name ?y-name]]
          (d/db conn)
          [sgc-rule])
     (filter #(not= (first %) (second %))))

;;; same-generation cousins (without single persons or reversed duplicate pairs)
(->> (d/q '[:find ?x-name ?y-name
            :in $ %
            :where
            (sgc ?x ?y)
            [?x :person/name ?x-name]
            [?y :person/name ?y-name]]
          (d/db conn)
          [sgc-rule])
     (map set)
     (filter (comp pos? dec count))
     (into #{}))


;; same-generation cousins _of Ann_
(let [goal "ann"]                  ; see last paragraph of the paper's section B
  (map first (d/q '[:find ?x-name
                    :in $ % ?goal-name
                    :where
                    (sgc ?x ?y)
                    [?x :person/name ?x-name]
                    [?y :person/name ?goal-name]
                    [(not= ?x-name ?goal-name)]]
                  (d/db conn)
                  [sgc-rule]
                  goal)))


;;;; Siblings

;; P2: sibling(X,Y) :- par(Z,X), par(Z,Y), X != Y

(def sibling-rule
  "Named query logic to find siblings."
  '[(sibling ?x ?y)
    (or-join [?x ?y]
             (and [?z :parent/of ?x]
                  [?z :parent/of ?y]
                  [(not= ?x ?y)]))])

(distinct (map set (d/q '[:find ?x-name ?y-name
                      :in $ %
                      :where
                      (sibling ?x ?y)
                      [?x :person/name ?x-name]
                      [?y :person/name ?y-name]]
                    (d/db conn)
                    [sibling-rule])))

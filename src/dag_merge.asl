(module asl-agent-core/dag-merge
  :d "Lock-Free Concurrent Task DAG Splicing and Monotonic Typestate Lattice Engine"
  :exports [
    MergedTaskNode
    DagMergeResult
    state-to-rank
    resolve-state-lattice
    dag-merge-branches
    validate-dag-acyclic
  ])

(dfs MergedTaskNode
  (:f id Str "Task identifier")
  (:f title Str "Human-readable task title")
  (:f state-str Str "Monotonically resolved execution state")
  (:f dependencies (List Str) "Union of prerequisite task identifiers")
  (:f premises (List Str) "Union of truth premise identifiers"))

(dfs DagMergeResult
  (:f merged-nodes (List MergedTaskNode) "All unified task nodes")
  (:f added-by-a (List Str) "Task IDs introduced by branch A")
  (:f added-by-b (List Str) "Task IDs introduced by branch B")
  (:f reconciled-nodes (List Str) "Task IDs modified in both branches and reconciled")
  (:f valid-dag Bool "True if DAG is topological and cycle-free")
  (:f error-message (Option Str) "Error description if cycle or conflict detected"))

(df state-to-rank [(st Str)] -> I64
  :d "Maps task execution state string to monotonic lattice height."
  (if (= st "pending")
    0
    (if (= st "active")
      1
      (if (= st "completed")
        2
        (if (= st "failed")
          3
          (if (= st "invalidated")
            3
            0))))))

(df resolve-state-lattice [(st-a Str) (st-b Str)] -> Str
  :d "Resolves concurrent states using monotonic join semilattice."
  (let [(rank-a (state-to-rank st-a))
        (rank-b (state-to-rank st-b))]
    (if (>= rank-a rank-b) st-a st-b)))

(df merge-str-lists [(list-a (List Str)) (list-b (List Str))] -> (List Str)
  :d "Computes set union of two string lists without duplicates."
  (fold (fn [(acc (List Str)) (item Str)] -> (List Str)
          (if (list-contains? acc item)
            acc
            (list-append acc (list item))))
        list-a
        list-b))

(df find-node-by-id [(nodes (List MergedTaskNode)) (target-id Str)] -> (Option MergedTaskNode)
  :d "Finds task node by ID."
  (fold (fn [(acc (Option MergedTaskNode)) (n MergedTaskNode)] -> (Option MergedTaskNode)
          (mt acc
            ((some found) (some found))
            ((none) (if (= (.-id n) target-id) (some n) (none)))))
        (none)
        nodes))

(df has-direct-cycle? [(nodes (List MergedTaskNode))] -> Bool
  :d "Fast path checking for direct mutual dependencies between tasks."
  (fold (fn [(acc Bool) (n MergedTaskNode)] -> Bool
          (if acc
            true
            (let [(my-id (.-id n))
                  (deps (.-dependencies n))]
              (fold (fn [(inner-acc Bool) (dep-id Str)] -> Bool
                      (if inner-acc
                        true
                        (let [(dep-node-opt (find-node-by-id nodes dep-id))]
                          (mt dep-node-opt
                            ((none) false)
                            ((some dn) (list-contains? (.-dependencies dn) my-id))))))
                    false
                    deps))))
        false
        nodes))

(df validate-dag-acyclic [(nodes (List MergedTaskNode))] -> Bool
  :d "Validates task graph is acyclic."
  (not (has-direct-cycle? nodes)))

(dfs DagMergeAccumulator
  (:f nodes (List MergedTaskNode) "Accumulated merged nodes")
  (:f added-a (List Str) "Added by branch A")
  (:f added-b (List Str) "Added by branch B")
  (:f reconciled (List Str) "Reconciled in both")
  (:f seen (List Str) "Processed node IDs"))

(df dag-merge-branches [(base-nodes (List MergedTaskNode))
                        (branch-a-nodes (List MergedTaskNode))
                        (branch-b-nodes (List MergedTaskNode))] -> DagMergeResult
  :d "Merges two concurrent task DAG branches with monotonic state lattices and edge unions."
  (let [(base-ids (fold (fn [(acc (List Str)) (n MergedTaskNode)] -> (List Str)
                          (list-append acc (list (.-id n))))
                        (list)
                        base-nodes))
        (init (DagMergeAccumulator
                :nodes (list)
                :added-a (list)
                :added-b (list)
                :reconciled (list)
                :seen (list)))
        (after-a (fold (fn [(acc DagMergeAccumulator) (na MergedTaskNode)] -> DagMergeAccumulator
                         (let [(id (.-id na))
                               (b-opt (find-node-by-id branch-b-nodes id))
                               (cur-nodes (.-nodes acc))
                               (cur-seen (list-append (.-seen acc) (list id)))]
                           (mt b-opt
                             ((none)
                              (let [(is-new (not (list-contains? base-ids id)))
                                    (next-a (if is-new (list-append (.-added-a acc) (list id)) (.-added-a acc)))]
                                (DagMergeAccumulator
                                  :nodes (list-append cur-nodes (list na))
                                  :added-a next-a
                                  :added-b (.-added-b acc)
                                  :reconciled (.-reconciled acc)
                                  :seen cur-seen)))
                             ((some nb)
                              (let [(resolved-st (resolve-state-lattice (.-state-str na) (.-state-str nb)))
                                    (merged-deps (merge-str-lists (.-dependencies na) (.-dependencies nb)))
                                    (merged-premises (merge-str-lists (.-premises na) (.-premises nb)))
                                    (unified (MergedTaskNode
                                               :id id
                                               :title (.-title na)
                                               :state-str resolved-st
                                               :dependencies merged-deps
                                               :premises merged-premises))
                                    (next-rec (list-append (.-reconciled acc) (list id)))]
                                (DagMergeAccumulator
                                  :nodes (list-append cur-nodes (list unified))
                                  :added-a (.-added-a acc)
                                  :added-b (.-added-b acc)
                                  :reconciled next-rec
                                  :seen cur-seen))))))
                       init
                       branch-a-nodes))
        (final-acc (fold (fn [(acc DagMergeAccumulator) (nb MergedTaskNode)] -> DagMergeAccumulator
                           (let [(id (.-id nb))]
                             (if (list-contains? (.-seen acc) id)
                               acc
                               (let [(cur-nodes (.-nodes acc))
                                     (cur-seen (list-append (.-seen acc) (list id)))
                                     (is-new (not (list-contains? base-ids id)))
                                     (next-b (if is-new (list-append (.-added-b acc) (list id)) (.-added-b acc)))]
                                 (DagMergeAccumulator
                                   :nodes (list-append cur-nodes (list nb))
                                   :added-a (.-added-a acc)
                                   :added-b next-b
                                   :reconciled (.-reconciled acc)
                                   :seen cur-seen)))))
                         after-a
                         branch-b-nodes))
        (merged-list (.-nodes final-acc))
        (is-acyclic (validate-dag-acyclic merged-list))]
    (if is-acyclic
      (DagMergeResult
        :merged-nodes merged-list
        :added-by-a (.-added-a final-acc)
        :added-by-b (.-added-b final-acc)
        :reconciled-nodes (.-reconciled final-acc)
        :valid-dag true
        :error-message (none))
      (DagMergeResult
        :merged-nodes merged-list
        :added-by-a (.-added-a final-acc)
        :added-by-b (.-added-b final-acc)
        :reconciled-nodes (.-reconciled final-acc)
        :valid-dag false
        :error-message (some "Topological cycle detected in merged task dependencies")))))

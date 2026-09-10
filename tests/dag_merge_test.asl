(module asl-agent-core/tests/dag-merge-test
  :d "Falsifiable verification test suite for Lock-Free Concurrent Task DAG Splicing and Monotonic State Lattice"
  :x [run-tests
      TestStateLatticeOrdering
      TestDisjointSubtaskSplice
      TestReconciledTaskStateAndDependencyUnion
      TestCycleDetectionRejection]
  :i [(dag_merge :a dm)])

(df TestStateLatticeOrdering [] -> Bool
  :d "Verifies monotonic lattice height ordering across task lifecycle states."
  (assert (= (dm/resolve-state-lattice "pending" "active") "active") "Active must dominate pending")
  (assert (= (dm/resolve-state-lattice "active" "completed") "completed") "Completed must dominate active")
  (assert (= (dm/resolve-state-lattice "completed" "pending") "completed") "Completed must dominate pending")
  (assert (= (dm/resolve-state-lattice "completed" "failed") "failed") "Failed must dominate completed")
  true)

(df TestDisjointSubtaskSplice [] -> Bool
  :d "Verifies two agents adding disjoint subtasks splice cleanly into unified DAG."
  (let [(root (dm/MergedTaskNode :id "T1" :title "Root Task" :state-str "active" :dependencies (list) :premises (list)))
        (sub1 (dm/MergedTaskNode :id "T1.1" :title "Subtask 1" :state-str "pending" :dependencies (list "T1") :premises (list)))
        (sub2 (dm/MergedTaskNode :id "T1.2" :title "Subtask 2" :state-str "pending" :dependencies (list "T1") :premises (list)))
        (base (list root))
        (branch-a (list root sub1))
        (branch-b (list root sub2))
        (res (dm/dag-merge-branches base branch-a branch-b))]
    (assert (.-valid-dag res) "Spliced DAG must be valid and acyclic")
    (assert (= (list-length (.-merged-nodes res)) 3) "Merged DAG must contain 3 nodes")
    (assert (list-contains? (.-added-by-a res) "T1.1") "Must record T1.1 from branch A")
    (assert (list-contains? (.-added-by-b res) "T1.2") "Must record T1.2 from branch B")
    (assert (= (list-length (.-reconciled-nodes res)) 1) "Root node T1 must be reconciled")
    true))

(df TestReconciledTaskStateAndDependencyUnion [] -> Bool
  :d "Verifies concurrent modifications to same task reconcile monotonically with dependency union."
  (let [(t0 (dm/MergedTaskNode :id "T2" :title "Build Task" :state-str "active" :dependencies (list "dep1") :premises (list "premise-a")))
        (t-branch-a (dm/MergedTaskNode :id "T2" :title "Build Task" :state-str "completed" :dependencies (list "dep1" "dep2") :premises (list "premise-a")))
        (t-branch-b (dm/MergedTaskNode :id "T2" :title "Build Task" :state-str "active" :dependencies (list "dep1" "dep3") :premises (list "premise-a" "premise-b")))
        (base (list t0))
        (branch-a (list t-branch-a))
        (branch-b (list t-branch-b))
        (res (dm/dag-merge-branches base branch-a branch-b))]
    (assert (.-valid-dag res) "Merged DAG must be valid")
    (assert (= (list-length (.-merged-nodes res)) 1) "Must contain exactly 1 merged node")
    (let [(merged-node (option-or (list-get (.-merged-nodes res) 0) (dm/MergedTaskNode :id "" :title "" :state-str "" :dependencies (list) :premises (list))))]
      (assert (= (.-state-str merged-node) "completed") "State must resolve monotonically to completed")
      (assert (= (list-length (.-dependencies merged-node)) 3) "Dependencies must union to 3 items")
      (assert (list-contains? (.-dependencies merged-node) "dep1") "Must include dep1")
      (assert (list-contains? (.-dependencies merged-node) "dep2") "Must include dep2")
      (assert (list-contains? (.-dependencies merged-node) "dep3") "Must include dep3")
      (assert (= (list-length (.-premises merged-node)) 2) "Premises must union to 2 items")
      (assert (list-contains? (.-premises merged-node) "premise-b") "Must include premise-b"))
    true))

(df TestCycleDetectionRejection [] -> Bool
  :d "Verifies mutually conflicting dependency additions trigger cycle detection."
  (let [(t1-a (dm/MergedTaskNode :id "TA" :title "A" :state-str "pending" :dependencies (list "TB") :premises (list)))
        (t2-b (dm/MergedTaskNode :id "TB" :title "B" :state-str "pending" :dependencies (list "TA") :premises (list)))
        (base (list))
        (branch-a (list t1-a))
        (branch-b (list t2-b))
        (res (dm/dag-merge-branches base branch-a branch-b))]
    (assert (not (.-valid-dag res)) "Direct mutual cycle must be flagged invalid")
    (mt (.-error-message res)
      ((none) (assert false "Cycle error message must be set"))
      ((some msg) (assert (string-contains? msg "cycle") "Error message must mention cycle")))
    true))

(df run-tests [] -> Bool
  :d "Executes all Lock-Free Concurrent Task DAG Splicing test suites."
  (and (TestStateLatticeOrdering)
       (and (TestDisjointSubtaskSplice)
            (and (TestReconciledTaskStateAndDependencyUnion)
                 (TestCycleDetectionRejection)))))

(module asl-core/dag
  :d "Blackboard Task-Premise DAG G=(V,E,P,H) and Optimistic Concurrency Control engine."
  :x [TaskState
      TaskNode
      TaskDag
      dag-create
      dag-add-node
      dag-find-node
      dag-is-falsified?
      dag-falsify-premise
      dag-can-activate?
      dag-cas-version]
  :i [(premise :a p)])

(dfe TaskState
  (:c task-pending [] "Task is pending execution")
  (:c task-active [] "Task is actively running")
  (:c task-completed [] "Task completed successfully")
  (:c task-failed [] "Task execution failed")
  (:c task-invalidated [] "Task invalidated due to refuted premise")
  (:c task-suspended [] "Task suspended awaiting intervention"))

(dfs TaskNode
  (:f id Str "Unique task identifier")
  (:f title Str "Human-readable task title")
  (:f state TaskState "Current execution state")
  (:f dependencies (List Str) "Identifiers of prerequisite tasks")
  (:f premises (List Str) "Identifiers of truth premises this task depends on"))

(dfs TaskDag
  (:f nodes (List TaskNode) "Collection of task nodes in the DAG")
  (:f version I64 "Monotonically increasing version counter for OCC")
  (:f falsified-premises (List Str) "Append-only registry of refuted premise IDs"))

(df dag-create [] -> TaskDag
  :d "Constructs an empty TaskDag at initial version 0."
  (TaskDag
    :nodes (list)
    :version 0
    :falsified-premises (list)))

(df dag-add-node [(dag TaskDag) (node TaskNode)] -> TaskDag
  :d "Appends a new task node to the DAG."
  (TaskDag
    :nodes (list-append (.-nodes dag) (list node))
    :version (.-version dag)
    :falsified-premises (.-falsified-premises dag)))

(df dag-find-node [(dag TaskDag) (node-id Str)] -> (Option TaskNode)
  :d "Finds a task node in the DAG by its identifier."
  (let [(matches (filter (fn [(n TaskNode)] -> Bool (= (.-id n) node-id)) (.-nodes dag)))]
    (list-head matches)))

(df dag-is-falsified? [(dag TaskDag) (premise-id Str)] -> Bool
  :d "Returns true if premise-id is present in falsified-premises."
  (list-contains? (.-falsified-premises dag) premise-id))

(df dag-falsify-premise [(dag TaskDag) (premise-id Str)] -> TaskDag
  :d "Records a falsified premise and cascades invalidation to all dependent task nodes, incrementing version."
  (let [(new-falsified (if (list-contains? (.-falsified-premises dag) premise-id)
                         (.-falsified-premises dag)
                         (list-append (.-falsified-premises dag) (list premise-id))))
        (new-nodes (map (fn [(node TaskNode)] -> TaskNode
                          (if (list-contains? (.-premises node) premise-id)
                            (TaskNode
                              :id (.-id node)
                              :title (.-title node)
                              :state (task-invalidated)
                              :dependencies (.-dependencies node)
                              :premises (.-premises node))
                            node))
                        (.-nodes dag)))]
    (TaskDag
      :nodes new-nodes
      :version (+ (.-version dag) 1)
      :falsified-premises new-falsified)))

(df is-node-completed? [(dag TaskDag) (dep-id Str)] -> Bool
  :d "Internal helper verifying if a dependency node exists and is completed."
  (let [(opt-n (dag-find-node dag dep-id))]
    (mt opt-n
      ((some n)
       (mt (.-state n)
         ((task-completed) true)
         (_ false)))
      ((none) false))))

(df dag-can-activate? [(dag TaskDag) (node-id Str)] -> Bool
  :d "Evaluates if a node can activate: must be pending, all dependencies completed, and zero falsified premises."
  (let [(opt-n (dag-find-node dag node-id))]
    (mt opt-n
      ((none) false)
      ((some node)
       (let [(is-pending (mt (.-state node)
                           ((task-pending) true)
                           (_ false)))
             (deps-ok (fold (fn [(acc Bool) (dep-id Str)] -> Bool
                              (and acc (is-node-completed? dag dep-id)))
                            true
                            (.-dependencies node)))
             (premises-ok (fold (fn [(acc Bool) (premise-id Str)] -> Bool
                                  (and acc (not (dag-is-falsified? dag premise-id))))
                                true
                                (.-premises node)))]
         (and is-pending (and deps-ok premises-ok)))))))

(df dag-cas-version [(current-dag TaskDag) (expected-version I64) (new-dag TaskDag)] -> (Option TaskDag)
  :d "Performs Optimistic Concurrency Control CAS check against the expected version."
  (if (= (.-version current-dag) expected-version)
    (some new-dag)
    (none)))

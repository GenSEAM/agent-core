(module asl-core/dag-test
  :d "Unit tests for Blackboard Task-Premise DAG and Premise Registry."
  :x [run-tests
      test-dag-create
      test-dag-add-node
      test-dag-can-activate
      test-dag-falsify-cascading-invalidation
      test-dag-cas-occ]
  :i [(premise :a p) (dag :a d)])

"run: (run-tests)"

(df test-dag-create [] -> Bool
  :d "Verifies dag-create initializes empty TaskDag at version 0."
  (let [(dag (d/dag-create))]
    (and (= (list-length (.-nodes dag)) 0)
         (and (= (.-version dag) 0)
              (= (list-length (.-falsified-premises dag)) 0)))))

(df test-dag-add-node [] -> Bool
  :d "Verifies dag-add-node inserts node and dag-find-node retrieves it."
  (let [(dag0 (d/dag-create))
        (node1 (d/TaskNode
                 :id "task-1"
                 :title "Initialize schema"
                 :state (d/task-pending)
                 :dependencies (list)
                 :premises (list "prem-1")))
        (dag1 (d/dag-add-node dag0 node1))
        (found (d/dag-find-node dag1 "task-1"))
        (missing (d/dag-find-node dag1 "task-missing"))]
    (and (= (list-length (.-nodes dag1)) 1)
         (and (mt found
                ((some n) (and (= (.-id n) "task-1")
                               (= (.-title n) "Initialize schema")))
                ((none) false))
              (mt missing
                ((some _) false)
                ((none) true))))))

(df test-dag-can-activate [] -> Bool
  :d "Verifies dag-can-activate? checks pending state, completed dependencies, and active premises."
  (let [(dag0 (d/dag-create))
        (node1 (d/TaskNode :id "t1" :title "Task 1" :state (d/task-pending) :dependencies (list) :premises (list "prem-ok")))
        (node2 (d/TaskNode :id "t2" :title "Task 2" :state (d/task-pending) :dependencies (list "t1") :premises (list)))
        (node3 (d/TaskNode :id "t3" :title "Task 3" :state (d/task-completed) :dependencies (list) :premises (list)))
        (node4 (d/TaskNode :id "t4" :title "Task 4" :state (d/task-pending) :dependencies (list "t3") :premises (list)))
        (dag1 (d/dag-add-node (d/dag-add-node (d/dag-add-node (d/dag-add-node dag0 node1) node2) node3) node4))]
    (and
      (d/dag-can-activate? dag1 "t1")
      (and
        (not (d/dag-can-activate? dag1 "t2"))
        (and
          (d/dag-can-activate? dag1 "t4")
          (and
            (not (d/dag-can-activate? dag1 "t3"))
            (not (d/dag-can-activate? dag1 "t-nonexistent"))))))))

(df test-dag-falsify-cascading-invalidation [] -> Bool
  :d "Verifies dag-falsify-premise transitions dependent nodes to task-invalidated and blocks activation."
  (let [(prem-rec (p/make-premise "p-vector" "Vector indexing is sub-millisecond"))
        (falsified-rec (p/falsify-premise prem-rec))
        (dag0 (d/dag-create))
        (node-dep (d/TaskNode :id "t-search" :title "Vector Search" :state (d/task-pending) :dependencies (list) :premises (list "p-vector")))
        (node-other (d/TaskNode :id "t-keyword" :title "Keyword Search" :state (d/task-pending) :dependencies (list) :premises (list "p-text")))
        (dag1 (d/dag-add-node (d/dag-add-node dag0 node-dep) node-other))]
    (let [(can-act-before (d/dag-can-activate? dag1 "t-search"))
          (dag2 (d/dag-falsify-premise dag1 "p-vector"))
          (found-dep (d/dag-find-node dag2 "t-search"))
          (found-other (d/dag-find-node dag2 "t-keyword"))]
      (and
        (string-contains? (p/format-premise falsified-rec) "falsified true")
        (and
          can-act-before
          (and
            (= (.-version dag2) 1)
            (and
              (d/dag-is-falsified? dag2 "p-vector")
              (and
                (not (d/dag-is-falsified? dag2 "p-text"))
                (and
                  (mt found-dep
                    ((some n) (mt (.-state n)
                                ((task-invalidated) true)
                                (_ false)))
                    ((none) false))
                  (and
                    (mt found-other
                      ((some n) (mt (.-state n)
                                  ((task-pending) true)
                                  (_ false)))
                      ((none) false))
                    (not (d/dag-can-activate? dag2 "t-search"))))))))))))

(df test-dag-cas-occ [] -> Bool
  :d "Verifies dag-cas-version OCC guarantees: succeeds on matching version and rejects on conflict."
  (let [(dag0 (d/dag-create))
        (node (d/TaskNode :id "t1" :title "Task 1" :state (d/task-pending) :dependencies (list) :premises (list)))
        (dag1 (d/dag-add-node dag0 node))]
    (let [(cas-ok (d/dag-cas-version dag0 0 dag1))
          (cas-conflict (d/dag-cas-version dag0 99 dag1))
          (dag-bumped (d/dag-falsify-premise dag1 "prem-x"))]
      (and
        (mt cas-ok
          ((some updated) (= (list-length (.-nodes updated)) 1))
          ((none) false))
        (and
          (mt cas-conflict
            ((some _) false)
            ((none) true))
          (and
            (mt (d/dag-cas-version dag-bumped 1 dag0)
              ((some _) true)
              ((none) false))
            (mt (d/dag-cas-version dag-bumped 0 dag0)
              ((some _) false)
              ((none) true))))))))

(df run-tests [] -> Bool
  :d "Runs all Blackboard Task-Premise DAG unit tests."
  (and (test-dag-create)
       (and (test-dag-add-node)
            (and (test-dag-can-activate)
                 (and (test-dag-falsify-cascading-invalidation)
                      (test-dag-cas-occ))))))

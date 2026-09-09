(module agent-core/tests/taskstore-test
  :d "Unit verification test suite for TaskStore, StateMachine, and FIFO Scheduler."
  :x [test-state-machine
      test-taskstore-persistence-and-claims
      test-scheduler-mutual-exclusion-and-fairness
      test-stale-claim-reaper
      test-task-kinds-and-outcomes
      test-taskstore-in-flight-tracking-and-spawning
      run-tests]
  :i [(tasktypes :a tt) (statemachine :a sm) (taskstore :a ts) (scheduler :a sc)])

(df test-state-machine [] -> Bool
  :d "Verifies deterministic state machine transitions, terminal state checks, and stage skip rejection."
  (do
    (assert (valid-transition? (state-queued) (state-routing)) "state-queued -> state-routing must be valid")
    (assert (valid-transition? (state-routing) (state-ready)) "state-routing -> state-ready must be valid")
    (assert (valid-transition? (state-ready) (state-executing)) "state-ready -> state-executing must be valid")
    (assert (valid-transition? (state-executing) (state-verifying)) "state-executing -> state-verifying must be valid")
    (assert (valid-transition? (state-verifying) (state-done)) "state-verifying -> state-done must be valid")
    (assert (not (valid-transition? (state-done) (state-routing))) "Terminal state-done must not allow transition to state-routing")
    (assert (not (valid-transition? (state-queued) (state-done))) "Skipping execution stages from state-queued to state-done must be invalid")
    (assert (is-terminal-state? (state-done)) "state-done must be classified as terminal state")
    (assert (is-terminal-state? (state-failed)) "state-failed must be classified as terminal state")
    (assert (is-terminal-state? (state-cancelled)) "state-cancelled must be classified as terminal state")
    (assert (not (is-terminal-state? (state-executing))) "state-executing must not be classified as terminal state")
    true))

(df test-taskstore-persistence-and-claims [] -> Bool
  :d "Verifies TaskStore persistence, FIFO queue ordering, atomic claims, and illegal transitions."
  (let [(t1 (task-record-create "task-1" "lane-1" "/ws/proj-1" (priority-normal) 200 "payload-1"))
        (store0 (taskstore-create "/ws/.asl/mem/tasks"))]
    (assert (= (.-id t1) "task-1") "Task record id must match constructor argument")
    (assert (= (.-created-at t1) 200) "Task record created-at must match constructor argument")
    (let [(put-res1 (taskstore-put store0 t1))]
      (assert (is-ok? put-res1) "taskstore-put must succeed with ok")
      (let [(store1 (result-or put-res1 store0))
            (got-opt (taskstore-get store1 "task-1"))]
        (assert (is-some? got-opt) "taskstore-get must find persisted task-1")
        (let [(got-t1 (option-or got-opt t1))]
          (assert (= (.-lane got-t1) "lane-1") "Retrieved task lane must match")
          (assert (= (.-state got-t1) (state-queued)) "Initial task state must be state-queued")
          (let [(t2 (task-record-create "task-2" "lane-1" "/ws/proj-1" (priority-high) 100 "payload-2"))
                (t3 (task-record-create "task-3" "lane-1" "/ws/proj-1" (priority-low) 300 "payload-3"))
                (store2 (result-or (taskstore-put store1 t2) store1))
                (store3 (result-or (taskstore-put store2 t3) store2))
                (queued-list (taskstore-list-queued store3))]
            (assert (= (list-length queued-list) 3) "taskstore-list-queued must return all 3 queued tasks")
            (let [(first-q (option-or (list-head queued-list) t1))
                  (rest-q1 (option-or (list-tail queued-list) (list)))
                  (second-q (option-or (list-head rest-q1) t1))
                  (rest-q2 (option-or (list-tail rest-q1) (list)))
                  (third-q (option-or (list-head rest-q2) t1))]
              (assert (= (.-id first-q) "task-2") "First queued task must be task-2 with earliest created-at 100")
              (assert (= (.-id second-q) "task-1") "Second queued task must be task-1 with created-at 200")
              (assert (= (.-id third-q) "task-3") "Third queued task must be task-3 with created-at 300")
              (let [(claim1-res (taskstore-claim store3 "task-2"))]
                (assert (is-ok? claim1-res) "Atomic claim on task-2 must succeed with ok")
                (let [(store-claimed (result-or claim1-res store3))
                      (claimed-task (option-or (taskstore-get store-claimed "task-2") t2))]
                  (assert (= (.-state claimed-task) (state-routing)) "Claimed task state must transition to state-routing")
                  (let [(double-claim-res (taskstore-claim store-claimed "task-2"))]
                    (assert (is-err? double-claim-res) "Double claiming already claimed task-2 must return err")
                    (let [(trans-res (taskstore-transition store-claimed "task-2" (state-ready) 500))]
                      (assert (is-ok? trans-res) "Valid transition from state-routing to state-ready must succeed")
                      true)))))))))))

(df test-scheduler-mutual-exclusion-and-fairness [] -> Bool
  :d "Verifies strict per-project mutual exclusion, concurrency ceiling, lane fairness, and project lock release."
  (let [(t-a1 (task-record-create "task-a1" "lane-a" "/ws/proj-1" (priority-normal) 100 "cmd-1"))
        (t-a2 (task-record-create "task-a2" "lane-a" "/ws/proj-1" (priority-normal) 200 "cmd-2"))
        (store-m0 (taskstore-create "/ws/.asl/mem/tasks"))
        (store-m1 (result-or (taskstore-put store-m0 t-a1) store-m0))
        (store-m2 (result-or (taskstore-put store-m1 t-a2) store-m1))
        (cfg-m (SchedulerConfig :max-concurrent 5 :stale-timeout-ms 5000))
        (sched-m (scheduler-create cfg-m))
        (drain-res-m (result-or (scheduler-drain sched-m store-m2)
                                (DrainResult :scheduler sched-m :store store-m2 :claimed-tasks (list))))
        (claimed-m (.-claimed-tasks drain-res-m))]
    (assert (= (list-length claimed-m) 1) "Scheduler drain must claim at most 1 task per project path")
    (let [(first-claimed (option-or (list-head claimed-m) t-a1))]
      (assert (= (.-id first-claimed) "task-a1") "First task on proj-1 must be claimed")
      (let [(a2-in-store (option-or (taskstore-get (.-store drain-res-m) "task-a2") t-a1))]
        (assert (= (.-state a2-in-store) (state-queued)) "Conflicting task-a2 on same project must remain in state-queued")
        (let [(cfg-c (SchedulerConfig :max-concurrent 2 :stale-timeout-ms 5000))
              (sched-c (scheduler-create cfg-c))
              (t-p1 (task-record-create "task-p1" "lane-1" "/ws/proj-p1" (priority-normal) 100 "p1"))
              (t-p2 (task-record-create "task-p2" "lane-2" "/ws/proj-p2" (priority-normal) 200 "p2"))
              (t-p3 (task-record-create "task-p3" "lane-3" "/ws/proj-p3" (priority-normal) 300 "p3"))
              (store-c0 (taskstore-create "/ws/.asl/mem/tasks"))
              (store-c1 (result-or (taskstore-put store-c0 t-p1) store-c0))
              (store-c2 (result-or (taskstore-put store-c1 t-p2) store-c1))
              (store-c3 (result-or (taskstore-put store-c2 t-p3) store-c2))
              (drain-res-c (result-or (scheduler-drain sched-c store-c3)
                                      (DrainResult :scheduler sched-c :store store-c3 :claimed-tasks (list))))
              (claimed-c (.-claimed-tasks drain-res-c))]
          (assert (= (list-length claimed-c) 2) "Scheduler drain must respect global max-concurrent ceiling of 2")
          (let [(t-l1-1 (task-record-create "task-l1-1" "lane-a" "/ws/proj-a1" (priority-normal) 100 "l1-1"))
                (t-l1-2 (task-record-create "task-l1-2" "lane-a" "/ws/proj-a2" (priority-normal) 200 "l1-2"))
                (t-l2-1 (task-record-create "task-l2-1" "lane-b" "/ws/proj-b1" (priority-normal) 300 "l2-1"))
                (store-f0 (taskstore-create "/ws/.asl/mem/tasks"))
                (store-f1 (result-or (taskstore-put store-f0 t-l1-1) store-f0))
                (store-f2 (result-or (taskstore-put store-f1 t-l1-2) store-f1))
                (store-f3 (result-or (taskstore-put store-f2 t-l2-1) store-f2))
                (cfg-f (SchedulerConfig :max-concurrent 2 :stale-timeout-ms 5000))
                (sched-f (scheduler-create cfg-f))
                (drain-res-f (result-or (scheduler-drain sched-f store-f3)
                                        (DrainResult :scheduler sched-f :store store-f3 :claimed-tasks (list))))
                (claimed-f (.-claimed-tasks drain-res-f))]
            (assert (= (list-length claimed-f) 2) "Lane fairness drain must claim 2 tasks across lanes")
            (let [(f-task1 (option-or (list-head claimed-f) t-l1-1))
                  (f-rest (option-or (list-tail claimed-f) (list)))
                  (f-task2 (option-or (list-head f-rest) t-l1-1))]
              (assert (= (.-id f-task1) "task-l1-1") "First claimed task must be task-l1-1 from lane-a")
              (assert (= (.-id f-task2) "task-l2-1") "Second claimed task must be task-l2-1 from idle lane-b")
              (let [(l1-2-in-store (option-or (taskstore-get (.-store drain-res-f) "task-l1-2") t-l1-1))]
                (assert (= (.-state l1-2-in-store) (state-queued)) "Secondary task task-l1-2 in busy lane-a must remain state-queued")
                (let [(sched-rel (scheduler-release-project (.-scheduler drain-res-m) "/ws/proj-1"))]
                  (assert (= (list-length (.-active-projects sched-rel)) 0) "Releasing project lock must evict project from active-projects")
                  true)))))))))

(df test-stale-claim-reaper [] -> Bool
  :d "Verifies stale claim reaper resets abandoned routing and executing tasks and releases locks."
  (let [(t-sr (task-record-create "task-sr" "lane-sr" "/ws/proj-sr" (priority-normal) 1000 "sr"))
        (store-r0 (taskstore-create "/ws/.asl/mem/tasks"))
        (store-r1 (result-or (taskstore-put store-r0 t-sr) store-r0))
        (store-claimed (result-or (taskstore-claim store-r1 "task-sr") store-r1))
        (cfg-r (SchedulerConfig :max-concurrent 5 :stale-timeout-ms 5000))
        (sched-r0 (scheduler-create cfg-r))
        (sched-r1 (SchedulerState
                    :config cfg-r
                    :active-projects (list "/ws/proj-sr")
                    :active-lanes (map-set (map-empty) "lane-sr" 1)))
        (reap-res-r (result-or (scheduler-reap-stale sched-r1 store-claimed 7000)
                               (ReapResult :scheduler sched-r1 :store store-claimed :reaped-count 0)))]
    (assert (= (.-reaped-count reap-res-r) 1) "Abandoned routing task exceeding heartbeat threshold must be reaped")
    (let [(sr-reaped (option-or (taskstore-get (.-store reap-res-r) "task-sr") t-sr))]
      (assert (= (.-state sr-reaped) (state-queued)) "Reaped routing task must be reset to state-queued")
      (assert (not (list-contains? (.-active-projects (.-scheduler reap-res-r)) "/ws/proj-sr")) "Reaping routing task must release its project lock")
      (let [(t-se (task-record-create "task-se" "lane-se" "/ws/proj-se" (priority-normal) 1000 "se"))
            (store-e0 (result-or (taskstore-put (.-store reap-res-r) t-se) (.-store reap-res-r)))
            (store-e1 (result-or (taskstore-claim store-e0 "task-se") store-e0))
            (store-e2 (result-or (taskstore-transition store-e1 "task-se" (state-ready) 1100) store-e1))
            (store-e3 (result-or (taskstore-transition store-e2 "task-se" (state-executing) 1200) store-e2))
            (sched-e (SchedulerState
                       :config cfg-r
                       :active-projects (list "/ws/proj-se")
                       :active-lanes (map-set (map-empty) "lane-se" 1)))
            (reap-res-e (result-or (scheduler-reap-stale sched-e store-e3 7000)
                                   (ReapResult :scheduler sched-e :store store-e3 :reaped-count 0)))]
        (assert (= (.-reaped-count reap-res-e) 1) "Abandoned executing task exceeding heartbeat threshold must be reaped")
        (let [(se-reaped (option-or (taskstore-get (.-store reap-res-e) "task-se") t-se))]
          (assert (= (.-state se-reaped) (state-queued)) "Reaped executing task must be reset to state-queued")
          (let [(t-act (task-record-create "task-act" "lane-act" "/ws/proj-act" (priority-normal) 6000 "act"))
                (store-a0 (result-or (taskstore-put (.-store reap-res-e) t-act) (.-store reap-res-e)))
                (store-a1 (result-or (taskstore-claim store-a0 "task-act") store-a0))
                (sched-a (SchedulerState
                           :config cfg-r
                           :active-projects (list "/ws/proj-act")
                           :active-lanes (map-set (map-empty) "lane-act" 1)))
                (reap-res-a (result-or (scheduler-reap-stale sched-a store-a1 7000)
                                       (ReapResult :scheduler sched-a :store store-a1 :reaped-count 0)))]
            (assert (= (.-reaped-count reap-res-a) 0) "Active unexpired task within heartbeat threshold must not be reaped")
            (let [(act-task (option-or (taskstore-get (.-store reap-res-a) "task-act") t-act))]
              (assert (= (.-state act-task) (state-routing)) "Active unexpired task must remain in state-routing")
              true)))))))

(df test-task-kinds-and-outcomes [] -> Bool
  :d "Verifies TaskKind enum variants, TaskOutcome records, and in-flight state detection."
  (let [(k-mut (tt/kind-code-mutation))
        (k-spw (tt/kind-task-spawn))
        (k-aud (tt/kind-audit-verdict))
        (outcome (tt/make-task-outcome k-mut (list "src/a.asl") (list "c-1") "doc.md" "exit 0"))]
    (assert (= (list-length (.-mutated-paths outcome)) 1) "Outcome must have 1 mutated path")
    (assert (= (list-length (.-spawned-task-ids outcome)) 1) "Outcome must have 1 spawned task id")
    (assert (= (.-artifact-path outcome) "doc.md") "Outcome artifact path must match")
    (assert (= (.-receipt outcome) "exit 0") "Outcome receipt must match")
    (assert (tt/task-state-in-flight? (tt/state-routing)) "state-routing must be in-flight")
    (assert (tt/task-state-in-flight? (tt/state-ready)) "state-ready must be in-flight")
    (assert (tt/task-state-in-flight? (tt/state-executing)) "state-executing must be in-flight")
    (assert (tt/task-state-in-flight? (tt/state-verifying)) "state-verifying must be in-flight")
    (assert (not (tt/task-state-in-flight? (tt/state-queued))) "state-queued must not be in-flight")
    (assert (not (tt/task-state-in-flight? (tt/state-done))) "state-done must not be in-flight")
    true))

(df test-taskstore-in-flight-tracking-and-spawning [] -> Bool
  :d "Verifies in-flight task filtering, state counting, and dynamic child task spawning."
  (let [(store0 (ts/taskstore-create "/ws/.asl/mem/tasks"))
        (t1 (tt/task-record-create "task-m1" "lane-1" "/ws" (tt/priority-normal) 100 "cmd-1"))
        (t2 (tt/task-record-create "task-m2" "lane-1" "/ws" (tt/priority-normal) 200 "cmd-2"))
        (store1 (result-or (ts/taskstore-put store0 t1) store0))
        (store2 (result-or (ts/taskstore-put store1 t2) store1))
        (store-claimed (result-or (ts/taskstore-claim store2 "task-m1") store2))]
    (assert (= (ts/taskstore-count-by-state store-claimed (tt/state-queued)) 1) "Queued count must be 1")
    (assert (= (ts/taskstore-count-by-state store-claimed (tt/state-routing)) 1) "Routing count must be 1")
    (let [(in-flight (ts/taskstore-list-in-flight store-claimed))]
      (assert (= (list-length in-flight) 1) "In-flight task list count must be 1")
      (assert (= (.-id (option-or (list-head in-flight) t1)) "task-m1") "In-flight task must be task-m1")
      (let [(parent t2)
            (children-payloads (list "child payload 1" "child payload 2"))
            (store-spawned (ts/taskstore-spawn-children store-claimed parent children-payloads 300))]
        (assert (= (ts/taskstore-count-by-state store-spawned (tt/state-queued)) 3) "Queued count must be 3 after spawning 2 children")
        (let [(c1-opt (ts/taskstore-get store-spawned "task-m2-child-1"))
              (c2-opt (ts/taskstore-get store-spawned "task-m2-child-2"))]
          (assert (is-some? c1-opt) "Child 1 must be present in store")
          (assert (is-some? c2-opt) "Child 2 must be present in store")
          (let [(c1 (option-or c1-opt t1))]
            (assert (= (.-payload c1) "child payload 1") "Child 1 payload must match")
            (assert (= (.-state c1) (tt/state-queued)) "Child 1 state must be state-queued")
            true))))))

(df run-tests [] -> Bool
  :d "Executes complete test suite for phase 321 and holistic task extensions."
  (do
    (assert (test-state-machine) "test-state-machine suite must pass")
    (assert (test-taskstore-persistence-and-claims) "test-taskstore-persistence-and-claims suite must pass")
    (assert (test-scheduler-mutual-exclusion-and-fairness) "test-scheduler-mutual-exclusion-and-fairness suite must pass")
    (assert (test-stale-claim-reaper) "test-stale-claim-reaper suite must pass")
    (assert (test-task-kinds-and-outcomes) "test-task-kinds-and-outcomes suite must pass")
    (assert (test-taskstore-in-flight-tracking-and-spawning) "test-taskstore-in-flight-tracking-and-spawning suite must pass")
    true))

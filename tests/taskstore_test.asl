(module asl-agent-core/taskstore-test
  :d "Unit tests for TaskStore, StateMachine, and FIFO Scheduler in agent-core."
  :x [test-statemachine
      test-taskstore
      test-scheduler
      test-reaper
      run-tests]
  :i [(statemachine :a sm)
      (taskstore :a ts)
      (scheduler :a sc)])

(df test-statemachine [] -> Bool
  :d "Verifies state machine transitions, invalid transition rejections, and terminal predicates."
  (assert (sm/valid-transition? (ts/state-queued) (ts/state-routing)) "state-queued to state-routing must be valid")
  (assert (sm/valid-transition? (ts/state-routing) (ts/state-ready)) "state-routing to state-ready must be valid")
  (assert (not (sm/valid-transition? (ts/state-done) (ts/state-routing))) "state-done to state-routing must be invalid")
  (assert (not (sm/valid-transition? (ts/state-queued) (ts/state-done))) "state-queued to state-done must be invalid")
  (assert (sm/is-terminal-state? (ts/state-done)) "state-done must be terminal")
  (assert (and (sm/is-terminal-state? (ts/state-failed)) (sm/is-terminal-state? (ts/state-cancelled))) "failed and cancelled must be terminal")
  (assert (not (sm/is-terminal-state? (ts/state-executing))) "state-executing must not be terminal")
  true)

(df test-taskstore [] -> Bool
  :d "Verifies persistent task store creation, ordering, retrieval, and claim transitions."
  (let [(s0 (ts/taskstore-create "/tmp/tasks"))
        (t1 (ts/task-record-create "t1" "lane-a" "/repo/1" (ts/priority-normal) 1000 "payload-1"))
        (t2 (ts/task-record-create "t2" "lane-b" "/repo/2" (ts/priority-high) 2000 "payload-2"))
        (t3 (ts/task-record-create "t3" "lane-a" "/repo/1" (ts/priority-low) 3000 "payload-3"))
        (put1 (ts/taskstore-put s0 t1))
        (s1 (mt put1 ((ok st) st) ((err _) s0)))
        (put2 (ts/taskstore-put s1 t2))
        (s2 (mt put2 ((ok st) st) ((err _) s1)))
        (put3 (ts/taskstore-put s2 t3))
        (s3 (mt put3 ((ok st) st) ((err _) s2)))]
    (assert (is-some (ts/taskstore-get s3 "t1")) "t1 must be retrievable from store")
    (assert (= (list-len (ts/taskstore-list-queued s3)) 3) "store must have 3 queued tasks")
    (assert (= (.-id (option-or (list-head (ts/taskstore-list-queued s3)) t3)) "t1") "queued tasks must be in FIFO order")
    (let [(c1 (ts/taskstore-claim s3 "t1"))]
      (assert (is-ok c1) "claiming queued task t1 must succeed")
      (let [(s-claimed (mt c1 ((ok st) st) ((err _) s3)))
            (rec1 (option-or (ts/taskstore-get s-claimed "t1") t1))]
        (assert (mt (.-state rec1) ((state-routing) true) (_ false)) "claimed task must be in state-routing")
        (let [(c2 (ts/taskstore-claim s-claimed "t1"))]
          (assert (is-err c2) "claiming already claimed task must fail")
          (assert (= (list-len (ts/taskstore-list-queued s-claimed)) 2) "queued count must decrease after claim")
          true)))))

(df test-scheduler [] -> Bool
  :d "Verifies scheduler drain with mutual exclusion, concurrency bounds, and lane fairness."
  (let [(s0 (ts/taskstore-create "/tmp/tasks"))
        (t1 (ts/task-record-create "t1" "lane-a" "/repo/1" (ts/priority-normal) 1000 "payload-1"))
        (t2 (ts/task-record-create "t2" "lane-b" "/repo/2" (ts/priority-high) 2000 "payload-2"))
        (t3 (ts/task-record-create "t3" "lane-a" "/repo/1" (ts/priority-low) 3000 "payload-3"))
        (put1 (ts/taskstore-put s0 t1))
        (s1 (mt put1 ((ok st) st) ((err _) s0)))
        (put2 (ts/taskstore-put s1 t2))
        (s2 (mt put2 ((ok st) st) ((err _) s1)))
        (put3 (ts/taskstore-put s2 t3))
        (s3 (mt put3 ((ok st) st) ((err _) s2)))
        (cfg (sc/SchedulerConfig :max-concurrent 2 :stale-timeout-ms 5000))
        (sched0 (sc/scheduler-create cfg))
        (drain-res (sc/scheduler-drain sched0 s3))]
    (assert (is-ok drain-res) "scheduler drain must succeed")
    (let [(dr (mt drain-res ((ok d) d) ((err _) (sc/DrainResult :scheduler sched0 :store s3 :claimed-tasks (list)))))
          (claimed-tasks (.-claimed-tasks dr))
          (sched1 (.-scheduler dr))
          (s-drained (.-store dr))]
      (assert (= (list-len claimed-tasks) 2) "scheduler must claim exactly 2 concurrent tasks")
      (assert (= (list-len (.-active-projects sched1)) 2) "scheduler must track 2 active project paths")
      (let [(rec3 (option-or (ts/taskstore-get s-drained "t3") t3))]
        (assert (mt (.-state rec3) ((state-queued) true) (_ false)) "conflicting project task t3 must remain in state-queued")
        (assert (= (map-get-or (.-active-lanes sched1) "lane-a" 0) 1) "lane-a must have 1 active task")
        (assert (= (map-get-or (.-active-lanes sched1) "lane-b" 0) 1) "lane-b must have 1 active task")
        (let [(sched2 (sc/scheduler-release-project sched1 "/repo/1"))]
          (assert (= (list-len (.-active-projects sched2)) 1) "releasing project lock must decrease active projects")
          true)))))

(df test-reaper [] -> Bool
  :d "Verifies stale claim reaper resets expired tasks and releases project locks."
  (let [(s0 (ts/taskstore-create "/tmp/tasks"))
        (task-stale-routing (ts/TaskRecord
                              :id "stale-1"
                              :lane "lane-c"
                              :project-path "/repo/stale"
                              :state (ts/state-routing)
                              :priority (ts/priority-normal)
                              :created-at 1000
                              :updated-at 1000
                              :payload ""))
        (task-stale-exec (ts/TaskRecord
                           :id "stale-2"
                           :lane "lane-d"
                           :project-path "/repo/exec"
                           :state (ts/state-executing)
                           :priority (ts/priority-normal)
                           :created-at 1000
                           :updated-at 1000
                           :payload ""))
        (task-fresh (ts/TaskRecord
                      :id "fresh-1"
                      :lane "lane-e"
                      :project-path "/repo/fresh"
                      :state (ts/state-executing)
                      :priority (ts/priority-normal)
                      :created-at 1000
                      :updated-at 9500
                      :payload ""))
        (put1 (ts/taskstore-put s0 task-stale-routing))
        (s1 (mt put1 ((ok st) st) ((err _) s0)))
        (put2 (ts/taskstore-put s1 task-stale-exec))
        (s2 (mt put2 ((ok st) st) ((err _) s1)))
        (put3 (ts/taskstore-put s2 task-fresh))
        (store-reap (mt put3 ((ok st) st) ((err _) s2)))
        (cfg (sc/SchedulerConfig :max-concurrent 4 :stale-timeout-ms 5000))
        (sched-reap (sc/SchedulerState
                      :config cfg
                      :active-projects (list "/repo/stale" "/repo/exec" "/repo/fresh")
                      :active-lanes (map-empty)))
        (reap-res (sc/scheduler-reap-stale sched-reap store-reap 10000))]
    (assert (is-ok reap-res) "reaping stale tasks must succeed")
    (let [(rr (mt reap-res ((ok r) r) ((err _) (sc/ReapResult :scheduler sched-reap :store store-reap :reaped-count 0))))]
      (assert (= (.-reaped-count rr) 2) "reaper must reap exactly 2 expired tasks")
      (let [(reaped-t1 (option-or (ts/taskstore-get (.-store rr) "stale-1") task-stale-routing))]
        (assert (mt (.-state reaped-t1) ((state-queued) true) (_ false)) "stale routing task must be reset to state-queued")
        (let [(reaped-t2 (option-or (ts/taskstore-get (.-store rr) "stale-2") task-stale-exec))]
          (assert (mt (.-state reaped-t2) ((state-queued) true) (_ false)) "stale executing task must be reset to state-queued")
          (let [(fresh-rec (option-or (ts/taskstore-get (.-store rr) "fresh-1") task-fresh))]
            (assert (mt (.-state fresh-rec) ((state-executing) true) (_ false)) "fresh executing task must not be reaped")
            (assert (= (list-len (.-active-projects (.-scheduler rr))) 1) "reaped tasks must release project locks")
            (assert (= (option-or (list-head (.-active-projects (.-scheduler rr))) "") "/repo/fresh") "only fresh project must retain lock")
            true))))))

(df run-tests [] -> Bool
  :d "Executes all unit tests for agent-core taskstore, statemachine, and scheduler."
  (let [(_t1 (test-statemachine))
        (_t2 (test-taskstore))
        (_t3 (test-scheduler))
        (_t4 (test-reaper))]
    true))

(module asl-core/scheduler
  :d "FIFO lane scheduler with per-project mutual exclusion and stale claim reaper."
  :x [SchedulerConfig
      SchedulerState
      DrainResult
      ReapResult
      DrainAccumulator
      scheduler-create
      scheduler-drain
      scheduler-reap-stale
      scheduler-release-project]
  :i [(tasktypes :a tt) (taskstore :a ts)])

(dfs SchedulerConfig
  (:f max-concurrent I64 "Global concurrency ceiling")
  (:f stale-timeout-ms I64 "Heartbeat timeout threshold in milliseconds"))

(dfs SchedulerState
  (:f config SchedulerConfig "Scheduler configuration")
  (:f active-projects (List Str) "Project paths currently running tasks")
  (:f active-lanes (Map Str I64) "Active task count per conversation lane"))

(dfs DrainResult
  (:f scheduler SchedulerState "Updated scheduler state")
  (:f store ts/TaskStore "Updated task store with claimed tasks")
  (:f claimed-tasks (List tt/TaskRecord) "List of tasks claimed in this drain cycle"))

(dfs ReapResult
  (:f scheduler SchedulerState "Updated scheduler state with locks released")
  (:f store ts/TaskStore "Updated task store with expired tasks reset to state-queued")
  (:f reaped-count I64 "Number of stale tasks reaped"))

(dfs DrainAccumulator
  (:f sched SchedulerState "Accumulated scheduler state")
  (:f store ts/TaskStore "Accumulated task store")
  (:f claimed (List tt/TaskRecord) "Accumulated claimed tasks"))

(df scheduler-create [(config SchedulerConfig)] -> SchedulerState
  :d "Instantiates SchedulerState with designated config and empty active tracking structures."
  (SchedulerState
    :config config
    :active-projects (list)
    :active-lanes (map-empty)))

(df remove-str [(items (List Str)) (target Str)] -> (List Str)
  :d "Filters out all occurrences of target string from list."
  (if (list-empty? items)
    (list)
    (let [(h (option-or (list-head items) ""))
          (tl (option-or (list-tail items) (list)))]
      (if (= h target)
        (remove-str tl target)
        (list-cons h (remove-str tl target))))))

(df scheduler-release-project [(sched SchedulerState) (project-path Str)] -> SchedulerState
  :d "Evicts completed task project path from active-projects list."
  (SchedulerState
    :config (.-config sched)
    :active-projects (remove-str (.-active-projects sched) project-path)
    :active-lanes (.-active-lanes sched)))

(df get-lane-count [(lanes (Map Str I64)) (lane Str)] -> I64
  :d "Returns active task count for a lane, defaulting to 0."
  (option-or (map-get lanes lane) 0))

(df inc-lane-count [(lanes (Map Str I64)) (lane Str)] -> (Map Str I64)
  :d "Increments active task count for specified lane."
  (let [(curr (get-lane-count lanes lane))]
    (map-set lanes lane (+ curr 1))))

(df dec-lane-count [(lanes (Map Str I64)) (lane Str)] -> (Map Str I64)
  :d "Decrements active task count for specified lane without dropping below zero."
  (let [(curr (get-lane-count lanes lane))]
    (if (> curr 0)
      (map-set lanes lane (- curr 1))
      lanes)))

(df task-in-list? [(tasks (List tt/TaskRecord)) (id Str)] -> Bool
  :d "Checks if a task with the given ID exists in the task list."
  (if (list-empty? tasks)
    false
    (let [(h (option-or (list-head tasks) (tt/task-record-create "" "" "" (tt/priority-normal) 0 "")))
          (tl (option-or (list-tail tasks) (list)))]
      (if (= (.-id h) id)
        true
        (task-in-list? tl id)))))

(df drain-pass-idle [(tasks (List tt/TaskRecord)) (acc DrainAccumulator)] -> DrainAccumulator
  :d "First pass: claims earliest tasks from idle lanes where active lane count is zero."
  (if (list-empty? tasks)
    acc
    (let [(sched (.-sched acc))
          (store (.-store acc))
          (claimed (.-claimed acc))
          (task (option-or (list-head tasks) (tt/task-record-create "" "" "" (tt/priority-normal) 0 "")))
          (rest-tasks (option-or (list-tail tasks) (list)))
          (max-conc (.-max-concurrent (.-config sched)))
          (curr-active (list-length (.-active-projects sched)))
          (lane-count (get-lane-count (.-active-lanes sched) (.-lane task)))
          (proj (.-project-path task))
          (proj-busy (list-contains? (.-active-projects sched) proj))]
      (if (>= curr-active max-conc)
        acc
        (if (or proj-busy (> lane-count 0))
          (drain-pass-idle rest-tasks acc)
          (let [(claim-res (ts/taskstore-claim store (.-id task)))]
            (mt claim-res
              ((ok next-store)
               (let [(next-projects (list-cons proj (.-active-projects sched)))
                     (next-lanes (inc-lane-count (.-active-lanes sched) (.-lane task)))
                     (next-sched (SchedulerState
                                   :config (.-config sched)
                                   :active-projects next-projects
                                   :active-lanes next-lanes))
                     (claimed-task (tt/TaskRecord
                                     :id (.-id task)
                                     :lane (.-lane task)
                                     :project-path proj
                                     :state (tt/state-routing)
                                     :priority (.-priority task)
                                     :created-at (.-created-at task)
                                     :updated-at (.-updated-at task)
                                     :payload (.-payload task)))
                     (next-claimed (list-concat claimed (list claimed-task)))
                     (next-acc (DrainAccumulator
                                 :sched next-sched
                                 :store next-store
                                 :claimed next-claimed))]
                 (drain-pass-idle rest-tasks next-acc)))
              ((err _)
               (drain-pass-idle rest-tasks acc)))))))))

(df drain-pass-busy [(tasks (List tt/TaskRecord)) (acc DrainAccumulator)] -> DrainAccumulator
  :d "Second pass: claims remaining tasks in FIFO order up to concurrency ceiling."
  (if (list-empty? tasks)
    acc
    (let [(sched (.-sched acc))
          (store (.-store acc))
          (claimed (.-claimed acc))
          (task (option-or (list-head tasks) (tt/task-record-create "" "" "" (tt/priority-normal) 0 "")))
          (rest-tasks (option-or (list-tail tasks) (list)))
          (max-conc (.-max-concurrent (.-config sched)))
          (curr-active (list-length (.-active-projects sched)))
          (proj (.-project-path task))
          (proj-busy (list-contains? (.-active-projects sched) proj))
          (already-claimed (task-in-list? claimed (.-id task)))]
      (if (>= curr-active max-conc)
        acc
        (if (or proj-busy already-claimed)
          (drain-pass-busy rest-tasks acc)
          (let [(claim-res (ts/taskstore-claim store (.-id task)))]
            (mt claim-res
              ((ok next-store)
               (let [(next-projects (list-cons proj (.-active-projects sched)))
                     (next-lanes (inc-lane-count (.-active-lanes sched) (.-lane task)))
                     (next-sched (SchedulerState
                                   :config (.-config sched)
                                   :active-projects next-projects
                                   :active-lanes next-lanes))
                     (claimed-task (tt/TaskRecord
                                     :id (.-id task)
                                     :lane (.-lane task)
                                     :project-path proj
                                     :state (tt/state-routing)
                                     :priority (.-priority task)
                                     :created-at (.-created-at task)
                                     :updated-at (.-updated-at task)
                                     :payload (.-payload task)))
                     (next-claimed (list-concat claimed (list claimed-task)))
                     (next-acc (DrainAccumulator
                                 :sched next-sched
                                 :store next-store
                                 :claimed next-claimed))]
                 (drain-pass-busy rest-tasks next-acc)))
              ((err _)
               (drain-pass-busy rest-tasks acc)))))))))

(df scheduler-drain [(sched SchedulerState) (store ts/TaskStore)] -> (Result DrainResult Str)
  :d "Drains queued tasks enforcing project mutual exclusion, concurrency ceiling, and lane fairness."
  (let [(queued (ts/taskstore-list-queued store))
        (init-acc (DrainAccumulator
                    :sched sched
                    :store store
                    :claimed (list)))
        (acc-idle (drain-pass-idle queued init-acc))
        (final-acc (drain-pass-busy queued acc-idle))]
    (ok (DrainResult
          :scheduler (.-sched final-acc)
          :store (.-store final-acc)
          :claimed-tasks (.-claimed final-acc)))))

(df is-reapable-state? [(state tt/TaskState)] -> Bool
  :d "Checks if task state is eligible for stale claim reaping."
  (mt state
    ((state-routing) true)
    ((state-executing) true)
    (_ false)))

(df reap-tasks [(tasks (List tt/TaskRecord)) (sched SchedulerState) (store ts/TaskStore) (now-ms I64) (reaped-count I64)] -> ReapResult
  :d "Recursively inspects tasks in store, resetting expired routing or executing tasks."
  (if (list-empty? tasks)
    (ReapResult
      :scheduler sched
      :store store
      :reaped-count reaped-count)
    (let [(task (option-or (list-head tasks) (tt/task-record-create "" "" "" (tt/priority-normal) 0 "")))
          (rest-tasks (option-or (list-tail tasks) (list)))
          (stale-timeout (.-stale-timeout-ms (.-config sched)))
          (elapsed (- now-ms (.-updated-at task)))
          (is-stale (and (is-reapable-state? (.-state task)) (> elapsed stale-timeout)))]
      (if is-stale
        (let [(trans-res (ts/taskstore-transition store (.-id task) (tt/state-queued) now-ms))]
          (mt trans-res
            ((ok next-store)
             (let [(next-projects (remove-str (.-active-projects sched) (.-project-path task)))
                   (next-lanes (dec-lane-count (.-active-lanes sched) (.-lane task)))
                   (next-sched (SchedulerState
                                 :config (.-config sched)
                                 :active-projects next-projects
                                 :active-lanes next-lanes))]
               (reap-tasks rest-tasks next-sched next-store now-ms (+ reaped-count 1))))
            ((err _)
             (reap-tasks rest-tasks sched store now-ms reaped-count))))
        (reap-tasks rest-tasks sched store now-ms reaped-count)))))

(df scheduler-reap-stale [(sched SchedulerState) (store ts/TaskStore) (current-time-ms I64)] -> (Result ReapResult Str)
  :d "Detects abandoned tasks in state-routing or state-executing exceeding heartbeat threshold and resets them."
  (let [(all-tasks (map-values (.-tasks store)))
        (result (reap-tasks all-tasks sched store current-time-ms 0))]
    (ok result)))

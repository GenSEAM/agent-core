(module asl-core/scheduler
  :d "FIFO lane scheduler with per-project mutual exclusion and stale claim reaper."
  :x [TaskState
      TaskPriority
      TaskRecord
      TaskStore
      SchedulerConfig
      SchedulerState
      DrainResult
      ReapResult
      DrainAccumulator
      scheduler-create
      scheduler-drain
      scheduler-reap-stale
      scheduler-release-project]
  :i [])

(dfe TaskState
  (:c state-queued [] "Task born on disk, awaiting scheduler drain")
  (:c state-routing [] "Claimed by scheduler, determining route and spec")
  (:c state-clarification [] "Awaiting external spec clarification")
  (:c state-ready [] "Spec confirmed and queued for harness execution")
  (:c state-executing [] "Active execution in harness loop")
  (:c state-verifying [] "Verification gate inspection")
  (:c state-done [] "Verified terminal success")
  (:c state-failed [] "Terminal failure")
  (:c state-cancelled [] "Cancelled by user or precondition"))

(dfe TaskPriority
  (:c priority-low [] "Low priority background task")
  (:c priority-normal [] "Standard execution priority")
  (:c priority-high [] "Elevated interactive priority")
  (:c priority-urgent [] "Urgent priority bypassing standard queue delay"))

(dfs TaskRecord
  (:f id Str "Unique task identifier")
  (:f lane Str "Conversation or thread lane identifier")
  (:f project-path Str "Target repository or workspace directory")
  (:f state TaskState "Current lifecycle state")
  (:f priority TaskPriority "Scheduling priority")
  (:f created-at I64 "Epoch millisecond timestamp of task creation")
  (:f updated-at I64 "Epoch millisecond timestamp of latest state update")
  (:f payload Str "Serialized task specification or instruction payload"))

(dfs TaskStore
  (:f base-path Str "Root directory for task records")
  (:f tasks (Map Str TaskRecord) "In-memory indexed cache of tasks"))

(df task-record-create [(id Str) (lane Str) (project-path Str) (priority TaskPriority) (created-at I64) (payload Str)] -> TaskRecord
  :d "Constructs a new TaskRecord born in state-queued with updated-at equal to created-at."
  (TaskRecord
    :id id
    :lane lane
    :project-path project-path
    :state (state-queued)
    :priority priority
    :created-at created-at
    :updated-at created-at
    :payload payload))

(df taskstore-create [(base-path Str)] -> TaskStore
  :d "Instantiates TaskStore with designated base path and empty task index."
  (TaskStore
    :base-path base-path
    :tasks (map-empty)))

(df taskstore-put [(store TaskStore) (task TaskRecord)] -> (Result TaskStore Str)
  :d "Persists task record into task store index."
  (ok (TaskStore
        :base-path (.-base-path store)
        :tasks (map-set (.-tasks store) (.-id task) task))))

(df taskstore-get [(store TaskStore) (id Str)] -> (Option TaskRecord)
  :d "Retrieves task record by identifier."
  (map-get (.-tasks store) id))

(df is-task-queued? [(task TaskRecord)] -> Bool
  :d "Checks if task state is state-queued."
  (mt (.-state task)
    ((state-queued) true)
    (_ false)))

(df filter-queued [(tasks (List TaskRecord))] -> (List TaskRecord)
  :d "Filters list of tasks retaining only those in state-queued."
  (if (list-empty? tasks)
    (list)
    (let [(h (option-or (list-head tasks) (task-record-create "" "" "" (priority-normal) 0 "")))
          (tail-list (option-or (list-tail tasks) (list)))]
      (if (is-task-queued? h)
        (list-cons h (filter-queued tail-list))
        (filter-queued tail-list)))))

(df insert-task [(t TaskRecord) (sorted (List TaskRecord))] -> (List TaskRecord)
  :d "Inserts task into sorted list by created-at ascending."
  (if (list-empty? sorted)
    (list t)
    (let [(h (option-or (list-head sorted) t))
          (tail-list (option-or (list-tail sorted) (list)))]
      (if (<= (.-created-at t) (.-created-at h))
        (list-cons t sorted)
        (list-cons h (insert-task t tail-list))))))

(df sort-tasks [(tasks (List TaskRecord))] -> (List TaskRecord)
  :d "Sorts task records by created-at ascending."
  (if (list-empty? tasks)
    (list)
    (let [(h (option-or (list-head tasks) (task-record-create "" "" "" (priority-normal) 0 "")))
          (tail-list (option-or (list-tail tasks) (list)))]
      (insert-task h (sort-tasks tail-list)))))

(df taskstore-list-queued [(store TaskStore)] -> (List TaskRecord)
  :d "Returns all tasks currently in state-queued ordered by created-at ascending."
  (sort-tasks (filter-queued (map-values (.-tasks store)))))

(df valid-transition? [(from TaskState) (to TaskState)] -> Bool
  :d "Validates legal lifecycle state transitions including reaper recoveries and terminal state locks."
  (mt from
    ((state-queued)
     (mt to
       ((state-routing) true)
       ((state-cancelled) true)
       (_ false)))
    ((state-routing)
     (mt to
       ((state-clarification) true)
       ((state-ready) true)
       ((state-failed) true)
       ((state-cancelled) true)
       ((state-queued) true)
       (_ false)))
    ((state-clarification)
     (mt to
       ((state-routing) true)
       ((state-cancelled) true)
       (_ false)))
    ((state-ready)
     (mt to
       ((state-executing) true)
       ((state-cancelled) true)
       (_ false)))
    ((state-executing)
     (mt to
       ((state-verifying) true)
       ((state-failed) true)
       ((state-cancelled) true)
       ((state-queued) true)
       (_ false)))
    ((state-verifying)
     (mt to
       ((state-done) true)
       ((state-failed) true)
       ((state-executing) true)
       (_ false)))
    (_ false)))

(df taskstore-claim [(store TaskStore) (id Str)] -> (Result TaskStore Str)
  :d "Performs atomic claim transition from state-queued to state-routing."
  (mt (taskstore-get store id)
    ((none) (err "Task not found"))
    ((some t)
     (mt (.-state t)
       ((state-queued)
        (let [(claimed (TaskRecord
                         :id (.-id t)
                         :lane (.-lane t)
                         :project-path (.-project-path t)
                         :state (state-routing)
                         :priority (.-priority t)
                         :created-at (.-created-at t)
                         :updated-at (.-updated-at t)
                         :payload (.-payload t)))]
          (taskstore-put store claimed)))
       (_ (err "Task is not in state-queued"))))))

(df taskstore-transition [(store TaskStore) (id Str) (new-state TaskState) (now-ts I64)] -> (Result TaskStore Str)
  :d "Validates lifecycle state transition and updates task record state and updated-at."
  (mt (taskstore-get store id)
    ((none) (err "Task not found"))
    ((some t)
     (if (valid-transition? (.-state t) new-state)
       (let [(updated (TaskRecord
                        :id (.-id t)
                        :lane (.-lane t)
                        :project-path (.-project-path t)
                        :state new-state
                        :priority (.-priority t)
                        :created-at (.-created-at t)
                        :updated-at now-ts
                        :payload (.-payload t)))]
         (taskstore-put store updated))
       (err "Illegal state transition")))))

(dfs SchedulerConfig
  (:f max-concurrent I64 "Global concurrency ceiling")
  (:f stale-timeout-ms I64 "Heartbeat timeout threshold in milliseconds"))

(dfs SchedulerState
  (:f config SchedulerConfig "Scheduler configuration")
  (:f active-projects (List Str) "Project paths currently running tasks")
  (:f active-lanes (Map Str I64) "Active task count per conversation lane"))

(dfs DrainResult
  (:f scheduler SchedulerState "Updated scheduler state")
  (:f store TaskStore "Updated task store with claimed tasks")
  (:f claimed-tasks (List TaskRecord) "List of tasks claimed in this drain cycle"))

(dfs ReapResult
  (:f scheduler SchedulerState "Updated scheduler state with locks released")
  (:f store TaskStore "Updated task store with expired tasks reset to state-queued")
  (:f reaped-count I64 "Number of stale tasks reaped"))

(dfs DrainAccumulator
  (:f sched SchedulerState "Accumulated scheduler state")
  (:f store TaskStore "Accumulated task store")
  (:f claimed (List TaskRecord) "Accumulated claimed tasks"))

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

(df task-in-list? [(tasks (List TaskRecord)) (id Str)] -> Bool
  :d "Checks if a task with the given ID exists in the task list."
  (if (list-empty? tasks)
    false
    (let [(h (option-or (list-head tasks) (task-record-create "" "" "" (priority-normal) 0 "")))
          (tl (option-or (list-tail tasks) (list)))]
      (if (= (.-id h) id)
        true
        (task-in-list? tl id)))))

(df drain-pass-idle [(tasks (List TaskRecord)) (acc DrainAccumulator)] -> DrainAccumulator
  :d "First pass: claims earliest tasks from idle lanes where active lane count is zero."
  (if (list-empty? tasks)
    acc
    (let [(sched (.-sched acc))
          (store (.-store acc))
          (claimed (.-claimed acc))
          (task (option-or (list-head tasks) (task-record-create "" "" "" (priority-normal) 0 "")))
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
          (let [(claim-res (taskstore-claim store (.-id task)))]
            (mt claim-res
              ((ok next-store)
               (let [(next-projects (list-cons proj (.-active-projects sched)))
                     (next-lanes (inc-lane-count (.-active-lanes sched) (.-lane task)))
                     (next-sched (SchedulerState
                                   :config (.-config sched)
                                   :active-projects next-projects
                                   :active-lanes next-lanes))
                     (claimed-task (TaskRecord
                                     :id (.-id task)
                                     :lane (.-lane task)
                                     :project-path proj
                                     :state (state-routing)
                                     :priority (.-priority task)
                                     :created-at (.-created-at task)
                                     :updated-at (.-updated-at task)
                                     :payload (.-payload task)))
                     (next-claimed (list-append claimed (list claimed-task)))
                     (next-acc (DrainAccumulator
                                 :sched next-sched
                                 :store next-store
                                 :claimed next-claimed))]
                 (drain-pass-idle rest-tasks next-acc)))
              ((err _)
               (drain-pass-idle rest-tasks acc)))))))))

(df drain-pass-busy [(tasks (List TaskRecord)) (acc DrainAccumulator)] -> DrainAccumulator
  :d "Second pass: claims remaining tasks in FIFO order up to concurrency ceiling."
  (if (list-empty? tasks)
    acc
    (let [(sched (.-sched acc))
          (store (.-store acc))
          (claimed (.-claimed acc))
          (task (option-or (list-head tasks) (task-record-create "" "" "" (priority-normal) 0 "")))
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
          (let [(claim-res (taskstore-claim store (.-id task)))]
            (mt claim-res
              ((ok next-store)
               (let [(next-projects (list-cons proj (.-active-projects sched)))
                     (next-lanes (inc-lane-count (.-active-lanes sched) (.-lane task)))
                     (next-sched (SchedulerState
                                   :config (.-config sched)
                                   :active-projects next-projects
                                   :active-lanes next-lanes))
                     (claimed-task (TaskRecord
                                     :id (.-id task)
                                     :lane (.-lane task)
                                     :project-path proj
                                     :state (state-routing)
                                     :priority (.-priority task)
                                     :created-at (.-created-at task)
                                     :updated-at (.-updated-at task)
                                     :payload (.-payload task)))
                     (next-claimed (list-append claimed (list claimed-task)))
                     (next-acc (DrainAccumulator
                                 :sched next-sched
                                 :store next-store
                                 :claimed next-claimed))]
                 (drain-pass-busy rest-tasks next-acc)))
              ((err _)
               (drain-pass-busy rest-tasks acc)))))))))

(df scheduler-drain [(sched SchedulerState) (store TaskStore)] -> (Result DrainResult Str)
  :d "Drains queued tasks enforcing project mutual exclusion, concurrency ceiling, and lane fairness."
  (let [(queued (taskstore-list-queued store))
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

(df is-reapable-state? [(state TaskState)] -> Bool
  :d "Checks if task state is eligible for stale claim reaping."
  (mt state
    ((state-routing) true)
    ((state-executing) true)
    (_ false)))

(df reap-tasks [(tasks (List TaskRecord)) (sched SchedulerState) (store TaskStore) (now-ms I64) (reaped-count I64)] -> ReapResult
  :d "Recursively inspects tasks in store, resetting expired routing or executing tasks."
  (if (list-empty? tasks)
    (ReapResult
      :scheduler sched
      :store store
      :reaped-count reaped-count)
    (let [(task (option-or (list-head tasks) (task-record-create "" "" "" (priority-normal) 0 "")))
          (rest-tasks (option-or (list-tail tasks) (list)))
          (stale-timeout (.-stale-timeout-ms (.-config sched)))
          (elapsed (- now-ms (.-updated-at task)))
          (is-stale (and (is-reapable-state? (.-state task)) (> elapsed stale-timeout)))]
      (if is-stale
        (let [(trans-res (taskstore-transition store (.-id task) (state-queued) now-ms))]
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

(df scheduler-reap-stale [(sched SchedulerState) (store TaskStore) (current-time-ms I64)] -> (Result ReapResult Str)
  :d "Detects abandoned tasks in state-routing or state-executing exceeding heartbeat threshold and resets them."
  (let [(all-tasks (map-values (.-tasks store)))
        (result (reap-tasks all-tasks sched store current-time-ms 0))]
    (ok result)))

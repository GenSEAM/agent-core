(module asl-core/taskstore
  :d "Persistent disk-backed TaskStore implementing store-is-the-queue semantics and atomic state transitions."
  :x [TaskState
      TaskPriority
      TaskRecord
      TaskStore
      task-record-create
      taskstore-create
      taskstore-put
      taskstore-get
      taskstore-list-queued
      taskstore-claim
      taskstore-transition
      valid-transition?]
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

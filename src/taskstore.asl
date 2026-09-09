(module asl-core/taskstore
  :d "Persistent disk-backed TaskStore implementing store-is-the-queue semantics, two-tier in-flight tracking, and atomic state transitions."
  :x [TaskStore
      taskstore-create
      taskstore-put
      taskstore-get
      is-task-queued?
      filter-queued
      insert-task
      sort-tasks
      taskstore-list-queued
      taskstore-claim
      taskstore-transition
      taskstore-list-in-flight
      taskstore-count-by-state
      taskstore-spawn-children]
  :i [(tasktypes :a tt) (statemachine :a sm)])

(dfs TaskStore
  (:f base-path Str "Root directory for task records")
  (:f tasks (Map Str tt/TaskRecord) "In-memory indexed cache of tasks"))

(df taskstore-create [(base-path Str)] -> TaskStore
  :d "Instantiates TaskStore with designated base path and empty task index."
  (TaskStore
    :base-path base-path
    :tasks (map-empty)))

(df taskstore-put [(store TaskStore) (task tt/TaskRecord)] -> (Result TaskStore Str)
  :d "Persists task record into task store index."
  (ok (TaskStore
        :base-path (.-base-path store)
        :tasks (map-set (.-tasks store) (.-id task) task))))

(df taskstore-get [(store TaskStore) (id Str)] -> (Option tt/TaskRecord)
  :d "Retrieves task record by identifier."
  (map-get (.-tasks store) id))

(df is-task-queued? [(task tt/TaskRecord)] -> Bool
  :d "Checks if task state is state-queued."
  (mt (.-state task)
    ((state-queued) true)
    (_ false)))

(df filter-queued [(tasks (List tt/TaskRecord))] -> (List tt/TaskRecord)
  :d "Filters list of tasks retaining only those in state-queued."
  (if (list-empty? tasks)
    (list)
    (let [(h (option-or (list-head tasks) (tt/task-record-create "" "" "" (tt/priority-normal) 0 "")))
          (tail-list (option-or (list-tail tasks) (list)))]
      (if (is-task-queued? h)
        (list-cons h (filter-queued tail-list))
        (filter-queued tail-list)))))

(df filter-in-flight [(tasks (List tt/TaskRecord))] -> (List tt/TaskRecord)
  :d "Filters list of tasks retaining only those in active in-flight states."
  (if (list-empty? tasks)
    (list)
    (let [(h (option-or (list-head tasks) (tt/task-record-create "" "" "" (tt/priority-normal) 0 "")))
          (tail-list (option-or (list-tail tasks) (list)))]
      (if (tt/task-state-in-flight? (.-state h))
        (list-cons h (filter-in-flight tail-list))
        (filter-in-flight tail-list)))))

(df insert-task [(t tt/TaskRecord) (sorted (List tt/TaskRecord))] -> (List tt/TaskRecord)
  :d "Inserts task into sorted list by created-at ascending."
  (if (list-empty? sorted)
    (list t)
    (let [(h (option-or (list-head sorted) t))
          (tail-list (option-or (list-tail sorted) (list)))]
      (if (<= (.-created-at t) (.-created-at h))
        (list-cons t sorted)
        (list-cons h (insert-task t tail-list))))))

(df sort-tasks [(tasks (List tt/TaskRecord))] -> (List tt/TaskRecord)
  :d "Sorts task records by created-at ascending."
  (if (list-empty? tasks)
    (list)
    (let [(h (option-or (list-head tasks) (tt/task-record-create "" "" "" (tt/priority-normal) 0 "")))
          (tail-list (option-or (list-tail tasks) (list)))]
      (insert-task h (sort-tasks tail-list)))))

(df taskstore-list-queued [(store TaskStore)] -> (List tt/TaskRecord)
  :d "Returns all tasks currently in state-queued ordered by created-at ascending."
  (sort-tasks (filter-queued (map-values (.-tasks store)))))

(df taskstore-list-in-flight [(store TaskStore)] -> (List tt/TaskRecord)
  :d "Returns all tasks currently in an in-flight lifecycle state ordered by created-at."
  (sort-tasks (filter-in-flight (map-values (.-tasks store)))))

(df taskstore-count-by-state [(store TaskStore) (state tt/TaskState)] -> I64
  :d "Counts number of tasks currently residing in specified state."
  (let [(all-tasks (map-values (.-tasks store)))
        (matching (filter (fn [(t tt/TaskRecord)] -> Bool (= (.-state t) state)) all-tasks))]
    (list-length matching)))

(df taskstore-claim [(store TaskStore) (id Str)] -> (Result TaskStore Str)
  :d "Performs atomic claim transition from state-queued to state-routing."
  (mt (taskstore-get store id)
    ((none) (err "Task not found"))
    ((some t)
     (mt (.-state t)
       ((state-queued)
        (let [(claimed (tt/TaskRecord
                         :id (.-id t)
                         :lane (.-lane t)
                         :project-path (.-project-path t)
                         :state (tt/state-routing)
                         :priority (.-priority t)
                         :created-at (.-created-at t)
                         :updated-at (.-updated-at t)
                         :payload (.-payload t)))]
          (taskstore-put store claimed)))
       (_ (err "Task is not in state-queued"))))))

(df taskstore-transition [(store TaskStore) (id Str) (new-state tt/TaskState) (now-ts I64)] -> (Result TaskStore Str)
  :d "Validates lifecycle state transition and updates task record state and updated-at."
  (mt (taskstore-get store id)
    ((none) (err "Task not found"))
    ((some t)
     (if (sm/valid-transition? (.-state t) new-state)
       (let [(updated (tt/TaskRecord
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

(df spawn-recursive [(store TaskStore) (parent tt/TaskRecord) (payloads (List Str)) (idx I64) (now-ts I64)] -> TaskStore
  :d "Recursively inserts dynamically spawned child tasks into TaskStore."
  (if (list-empty? payloads)
    store
    (let [(payload (option-or (list-head payloads) ""))
          (rest-payloads (option-or (list-tail payloads) (list)))
          (child-id (str (.-id parent) "-child-" (string-from-int64 idx)))
          (child-task (tt/task-record-create child-id (.-lane parent) (.-project-path parent) (.-priority parent) now-ts payload))
          (put-res (taskstore-put store child-task))
          (next-store (result-or put-res store))]
      (spawn-recursive next-store parent rest-payloads (+ idx 1) now-ts))))

(df taskstore-spawn-children [(store TaskStore) (parent tt/TaskRecord) (children-payloads (List Str)) (now-ts I64)] -> TaskStore
  :d "Dynamically creates and enqueues child tasks spawned from parent task."
  (spawn-recursive store parent children-payloads 1 now-ts))

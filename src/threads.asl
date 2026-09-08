(module asl-core/threads
  :d "Conversation threads, parent-child task linkage, and terminal handoff brief propagation."
  :x [ThreadRecord
      ThreadHandoff
      thread-create
      thread-add-task
      thread-link-followup
      thread-attach-handoff
      thread-get-parent-brief
      thread-is-active?
      TaskRecord]
  :i [])

(dfs ThreadHandoff
  (:f parent-task-id Str "Terminal parent task identifier")
  (:f terminal-summary Str "Concise completion summary from parent task")
  (:f artifacts (List Str) "List of generated artifact paths")
  (:f completed-at I64 "Completion timestamp"))

(dfs ThreadRecord
  (:f id Str "Thread identifier")
  (:f lane Str "Conversation lane")
  (:f root-task-id Str "Initial task initiating thread")
  (:f task-ids (List Str) "Ordered list of tasks executed within thread")
  (:f parent-id (Option Str) "Direct parent task identifier for follow-ups")
  (:f handoff (Option ThreadHandoff) "Handoff brief from preceding task")
  (:f active Bool "True if thread is currently open")
  (:f created-at I64 "Creation timestamp")
  (:f updated-at I64 "Last updated timestamp"))

(dfs TaskRecord
  (:f id Str "Unique task identifier")
  (:f lane Str "Conversation or thread lane identifier")
  (:f project-path Str "Target repository or workspace directory")
  (:f state Str "Current lifecycle state")
  (:f priority Str "Scheduling priority")
  (:f created-at I64 "Epoch millisecond timestamp of task creation")
  (:f updated-at I64 "Epoch millisecond timestamp of latest state update")
  (:f payload Str "Serialized task specification or instruction payload"))

(df thread-create [(id Str) (lane Str) (root-task-id Str) (now-ms I64)] -> ThreadRecord
  :d "Initializes an active conversational thread bound to a root task."
  (ThreadRecord
    :id id
    :lane lane
    :root-task-id root-task-id
    :task-ids (list root-task-id)
    :parent-id (none)
    :handoff (none)
    :active true
    :created-at now-ms
    :updated-at now-ms))

(df thread-add-task [(th ThreadRecord) (task-id Str) (now-ms I64)] -> ThreadRecord
  :d "Appends a new task identifier into the thread execution sequence."
  (ThreadRecord
    :id (.-id th)
    :lane (.-lane th)
    :root-task-id (.-root-task-id th)
    :task-ids (list-append (.-task-ids th) (list task-id))
    :parent-id (.-parent-id th)
    :handoff (.-handoff th)
    :active (.-active th)
    :created-at (.-created-at th)
    :updated-at now-ms))

(df thread-attach-handoff [(th ThreadRecord) (handoff ThreadHandoff) (now-ms I64)] -> ThreadRecord
  :d "Attaches a terminal parent completion summary to the conversation thread."
  (ThreadRecord
    :id (.-id th)
    :lane (.-lane th)
    :root-task-id (.-root-task-id th)
    :task-ids (.-task-ids th)
    :parent-id (some (.-parent-task-id handoff))
    :handoff (some handoff)
    :active (.-active th)
    :created-at (.-created-at th)
    :updated-at now-ms))

(df thread-link-followup [(parent-th ThreadRecord) (child-task TaskRecord) (handoff ThreadHandoff) (now-ms I64)] -> (Pair ThreadRecord TaskRecord)
  :d "Links a follow-up child task to parent thread, embedding terminal summary into payload without lane bleed."
  (let [(parent-id (.-parent-task-id handoff))
        (summary (.-terminal-summary handoff))
        (updated-th (ThreadRecord
                      :id (.-id parent-th)
                      :lane (.-lane parent-th)
                      :root-task-id (.-root-task-id parent-th)
                      :task-ids (list-append (.-task-ids parent-th) (list (.-id child-task)))
                      :parent-id (some parent-id)
                      :handoff (some handoff)
                      :active true
                      :created-at (.-created-at parent-th)
                      :updated-at now-ms))
        (brief-block (str "[Parent Task Handoff: " parent-id "]\n" summary))
        (augmented-payload (if (string-empty? (.-payload child-task))
                             brief-block
                             (str brief-block "\n\n" (.-payload child-task))))
        (updated-child (TaskRecord
                         :id (.-id child-task)
                         :lane (.-lane parent-th)
                         :project-path (.-project-path child-task)
                         :state (.-state child-task)
                         :priority (.-priority child-task)
                         :created-at (.-created-at child-task)
                         :updated-at now-ms
                         :payload augmented-payload))]
    (pair updated-th updated-child)))

(df thread-get-parent-brief [(th ThreadRecord)] -> (Option Str)
  :d "Extracts terminal summary of direct parent task if present on thread."
  (mt (.-handoff th)
    ((some h) (some (.-terminal-summary h)))
    ((none) (none))))

(df thread-is-active? [(th ThreadRecord)] -> Bool
  :d "Checks whether the thread is currently marked active."
  (.-active th))

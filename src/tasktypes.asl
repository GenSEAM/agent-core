(module asl-core/tasktypes
  :d "Domain representations for task records, lifecycle states, priorities, and scheduler lanes."
  :x [TaskState TaskPriority TaskRecord TaskLane task-record-create task-lane-create]
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

(dfs TaskLane
  (:f id Str "Lane identifier")
  (:f max-concurrent I64 "Maximum concurrent tasks allowed in this lane")
  (:f active-count I64 "Current active task count in this lane"))

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

(df task-lane-create [(id Str) (max-concurrent I64)] -> TaskLane
  :d "Constructs a new TaskLane with zero initial active tasks."
  (TaskLane
    :id id
    :max-concurrent max-concurrent
    :active-count 0))

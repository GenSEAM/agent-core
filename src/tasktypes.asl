(module asl-core/tasktypes
  :d "Domain representations for task records, lifecycle states, priorities, scheduler lanes, and outcome taxonomy."
  :x [TaskState TaskPriority TaskKind TaskOutcome TaskRecord TaskLane task-record-create task-lane-create make-task-outcome task-state-in-flight?]
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

(dfe TaskKind
  (:c kind-code-mutation [] "Direct source code AST modifications")
  (:c kind-task-spawn [] "Discovery or planning task spawning child items")
  (:c kind-audit-verdict [] "Verification audit producing compliance verdict")
  (:c kind-doc-artifact [] "Architecture or memory ledger documentation update")
  (:c kind-operational [] "Operational execution or benchmark validation"))

(dfs TaskOutcome
  (:f kind TaskKind "Outcome taxonomy kind")
  (:f mutated-paths (List Str) "Files modified during execution")
  (:f spawned-task-ids (List Str) "Identifiers of dynamically spawned child tasks")
  (:f artifact-path Str "Path to generated documentation or ADR artifact")
  (:f receipt Str "Serialized physical execution receipt"))

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

(df make-task-outcome [(kind TaskKind) (mutated-paths (List Str)) (spawned-task-ids (List Str)) (artifact-path Str) (receipt Str)] -> TaskOutcome
  :d "Constructs a typed TaskOutcome record encapsulating execution results."
  (TaskOutcome
    :kind kind
    :mutated-paths mutated-paths
    :spawned-task-ids spawned-task-ids
    :artifact-path artifact-path
    :receipt receipt))

(df task-state-in-flight? [(state TaskState)] -> Bool
  :d "Determines if a task state represents an active in-flight execution stage."
  (mt state
    ((state-routing) true)
    ((state-clarification) true)
    ((state-ready) true)
    ((state-executing) true)
    ((state-verifying) true)
    ((state-queued) false)
    ((state-done) false)
    ((state-failed) false)
    ((state-cancelled) false)))

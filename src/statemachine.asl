(module asl-core/statemachine
  :d "Deterministic finite state machine transition validator and terminal state predicates."
  :x [TaskState assert-transition is-terminal-state? valid-transition?]
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

(df is-terminal-state? [(state TaskState)] -> Bool
  :d "Evaluates whether a task lifecycle state is terminal (done, failed, or cancelled)."
  (mt state
    ((state-done) true)
    ((state-failed) true)
    ((state-cancelled) true)
    (_ false)))

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

(df assert-transition [(from TaskState) (to TaskState)] -> (Result TaskState Str)
  :d "Enforces valid state transition returning (ok to) or (err message) on violation."
  (if (valid-transition? from to)
    (ok to)
    (err "Illegal state transition")))

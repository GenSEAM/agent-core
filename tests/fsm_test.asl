(module asl-agent-core/fsm-test
  :d "Unit tests for FSM engine in ASL"
  :x [test-fsm-transitions test-fsm-terminal run-tests]
  :i [(fsm :a fsm)])

(df test-fsm-transitions [] -> Bool
  :d "Verifies state machine transitions across agent workflow events."
  (let [(s0 (fsm/idle))
        (s1 (fsm/step s0 (fsm/start)))
        (s2 (fsm/step s1 (fsm/plan-ready)))
        (s3 (fsm/step s2 (fsm/code-ready)))
        (s4-pass (fsm/step s3 (fsm/review-pass)))
        (s4-fail (fsm/step s3 (fsm/review-fail)))
        (s-reset (fsm/step s2 (fsm/reset)))]
    (assert (mt s1 ((fsm/planning) true) (_ false)) "idle + start must transition to planning")
    (assert (mt s2 ((fsm/coding) true) (_ false)) "planning + plan-ready must transition to coding")
    (assert (mt s3 ((fsm/reviewing) true) (_ false)) "coding + code-ready must transition to reviewing")
    (assert (mt s4-pass ((fsm/success) true) (_ false)) "reviewing + review-pass must transition to success")
    (assert (mt s4-fail ((fsm/coding) true) (_ false)) "reviewing + review-fail must transition to coding")
    (assert (mt s-reset ((fsm/idle) true) (_ false)) "coding + reset must transition to idle")
    true))

(df test-fsm-terminal [] -> Bool
  :d "Verifies terminal state predicate."
  (assert (fsm/is-terminal-state (fsm/success)) "success state must be terminal")
  (assert (fsm/is-terminal-state (fsm/failed)) "failed state must be terminal")
  (assert (not (fsm/is-terminal-state (fsm/idle))) "idle state must not be terminal")
  (assert (not (fsm/is-terminal-state (fsm/planning))) "planning state must not be terminal")
  (assert (not (fsm/is-terminal-state (fsm/coding))) "coding state must not be terminal")
  (assert (not (fsm/is-terminal-state (fsm/reviewing))) "reviewing state must not be terminal")
  true)

(df run-tests [] -> Bool
  :d "Runs FSM unit tests"
  (and (test-fsm-transitions)
       (test-fsm-terminal)))

(run-tests)

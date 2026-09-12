(module agent-core/tests/promptpool-test
  :d "Unit verification test suite for prompt context pool, clarification envelopes, threads, and prompt auditor."
  :x [test-prompt-pool
      test-clarification-envelope
      test-conversation-threads
      test-goal-composition
      test-prompt-audit
      run-tests]
  :i [(promptpool :a pp)
      (clarify :a cl)
      (threads :a th)
      (compose :a cp)
      (prompt-audit :a pa)
      (tasktypes :a tt)
      (statemachine :a sm)])

(df test-prompt-pool [] -> Bool
  :d "Verifies prompt pool initialization, ring buffer capacity clamping, TTL decay, framing, and lane isolation."
  (let [(pool0 (pp/prompt-pool-create "lane-1" 3 180000))]
    (assert (= (.-cap pool0) 3) "Default cap should be 3")
    (assert (= (.-ttl-ms pool0) 180000) "Default TTL should be 180000")
    (let [(p1 (pp/prompt-pool-push pool0 "prompt 1" 1000))
          (p2 (pp/prompt-pool-push p1 "prompt 2" 2000))
          (p3 (pp/prompt-pool-push p2 "prompt 3" 3000))
          (p4 (pp/prompt-pool-push p3 "prompt 4" 4000))]
      (assert (= (list-length (.-entries p4)) 3) "Pool must clamp to capacity of 3")
      (let [(first-e (option-or (list-head (.-entries p4)) (pp/PromptEntry :id "" :lane "" :prompt "" :timestamp 0 :turn-index 0)))]
        (assert (= (.-prompt first-e) "prompt 2") "Oldest prompt 1 must be dropped on capacity limit")
        (let [(pool-decay-base (pp/prompt-pool-create "lane-1" 3 180000))
              (p-old (pp/prompt-pool-push pool-decay-base "old prompt" 0))
              (p-new (pp/prompt-pool-push p-old "new prompt" 190000))]
          (assert (= (list-length (.-entries p-new)) 1) "Expired prompt at t=0 must be evicted at t=190000")
          (let [(rem-e (option-or (list-head (.-entries p-new)) (pp/PromptEntry :id "" :lane "" :prompt "" :timestamp 0 :turn-index 0)))]
            (assert (= (.-prompt rem-e) "new prompt") "Only unexpired prompt must remain after TTL decay")
            (let [(rendered-with-prior (pp/render-prompt-pool p3 "current instruction" 3500))]
              (assert (string-contains? rendered-with-prior "<prior-context>") "Rendered pool with history must include <prior-context>")
              (assert (string-contains? rendered-with-prior "<current-instruction>") "Rendered pool with history must include <current-instruction>")
              (let [(rendered-empty (pp/render-prompt-pool pool0 "fresh instruction" 1000))]
                (assert (not (string-contains? rendered-empty "<prior-context>")) "Empty pool must not contain <prior-context>")
                (assert (string-contains? rendered-empty "<current-instruction>\nfresh instruction\n</current-instruction>") "Empty pool renders only current instruction")
                (let [(mp0 (pp/multi-pool-create 3 180000))
                      (mp1 (pp/multi-pool-push mp0 "chat-A" "msg-A" 1000))
                      (poolB (pp/multi-pool-get mp1 "chat-B"))]
                  (assert (= (list-length (.-entries poolB)) 0) "Prompts pushed to chat-A must not leak to chat-B")
                  (let [(mp2 (pp/multi-pool-push mp1 "chat-B" "msg-B" 200000))
                        (mp-pruned (pp/multi-pool-prune-all mp2 210000))
                        (poolA-after (pp/multi-pool-get mp-pruned "chat-A"))
                        (poolB-after (pp/multi-pool-get mp-pruned "chat-B"))]
                    (assert (= (list-length (.-entries poolA-after)) 0) "Expired prompts in chat-A (t=1000) must be pruned at t=210000")
                    (assert (= (list-length (.-entries poolB-after)) 1) "Unexpired prompt in chat-B (t=200000) must remain at t=210000")
                    true))))))))))

(df test-clarification-envelope [] -> Bool
  :d "Verifies numbered answer parsing, out-of-order resolution, and task unblocking."
  (let [(ans-inline (cl/parse-numbered-answers "1. postgres, 2. port 5432"))]
    (assert (= (list-length ans-inline) 2) "Inline comma parsing must return 2 answers")
    (let [(ans-multi (cl/parse-numbered-answers "1. postgres\n2. port 5432"))]
      (assert (= (list-length ans-multi) 2) "Multiline parsing must return 2 answers")
      (let [(ans-ooo (cl/parse-numbered-answers "2. port 5432, 1. postgres"))]
        (assert (= (list-length ans-ooo) 2) "Out-of-order parsing must return 2 answers")
        (let [(q1 (cl/ClarificationQuestion :index 1 :text "DB engine?" :context "spec"))
              (q2 (cl/ClarificationQuestion :index 2 :text "Port?" :context "spec"))
              (sess0 (cl/clarification-session-create "task-1" "lane-1" (list q1 q2)))
              (sess-part (cl/attach-clarification-answers sess0 (list (cl/ClarificationAnswer :index 2 :text "5432"))))]
          (assert (= (map-size (.-answers sess-part)) 1) "Answers map must have 1 attached answer")
          (assert (not (cl/is-clarification-resolved? sess-part)) "Partial answers must not resolve clarification session")
          (let [(sess-full (cl/attach-clarification-answers sess-part (list (cl/ClarificationAnswer :index 1 :text "postgres"))))]
            (assert (cl/is-clarification-resolved? sess-full) "Full answers must resolve clarification session")
            (let [(t-block (tt/task-record-create "task-clarify" "lane-1" "/ws" (tt/priority-normal) 100 "Init payload"))
                  (t-clarify (tt/TaskRecord
                               :id (.-id t-block)
                               :lane (.-lane t-block)
                               :project-path (.-project-path t-block)
                               :state (tt/state-clarification)
                               :priority (.-priority t-block)
                               :created-at (.-created-at t-block)
                               :updated-at 200
                               :payload (.-payload t-block)))
                  (unblock-res (cl/unblock-clarification-task t-clarify sess-full 500))]
              (assert (is-ok? unblock-res) "Unblocking resolved task must succeed with ok")
              (let [(unblocked (result-or unblock-res t-clarify))]
                (assert (= (.-state unblocked) (tt/state-routing)) "Unblocked task must transition to state-routing")
                (assert (string-contains? (.-payload unblocked) "postgres") "Unblocked task payload must contain clarification answer")
                true))))))))

(df test-conversation-threads [] -> Bool
  :d "Verifies conversation threads, parent-child task linkage, handoff briefs, and lane isolation."
  (let [(th0 (th/thread-create "thread-1" "lane-1" "task-root" 1000))]
    (assert (th/thread-is-active? th0) "Newly created thread must be active")
    (assert (= (.-root-task-id th0) "task-root") "Root task id must match constructor argument")
    (let [(th1 (th/thread-add-task th0 "task-step2" 2000))]
      (assert (= (list-length (.-task-ids th1)) 2) "Thread must have 2 tasks after addition")
      (let [(handoff (th/ThreadHandoff
                       :parent-task-id "task-step2"
                       :terminal-summary "Completed DB schema migration"
                       :artifacts (list "/db/schema.sql")
                       :completed-at 2500))
            (th2 (th/thread-attach-handoff th1 handoff 2600))]
        (assert (is-some? (.-handoff th2)) "Attached handoff must be some")
        (let [(child-task (tt/task-record-create "task-followup" "lane-1" "/ws" (tt/priority-normal) 3000 "Implement endpoints"))
              (link-pair (th/thread-link-followup th2 child-task handoff 3000))
              (updated-th (.-first link-pair))
              (updated-child (.-second link-pair))]
          (assert (= (list-length (.-task-ids updated-th)) 3) "Linked thread must contain 3 task identifiers")
          (assert (string-contains? (.-payload updated-child) "Completed DB schema migration") "Follow-up child task must embed parent summary in payload")
          (let [(th-other (th/thread-create "thread-other" "lane-2" "task-other" 1000))]
            (assert (is-none? (.-handoff th-other)) "Isolated thread in lane-2 must not inherit handoff from thread-1")
            (let [(brief-opt (th/thread-get-parent-brief th2))]
              (assert (is-some? brief-opt) "Parent brief must be some on thread carrying handoff")
              (assert (= (option-or brief-opt "") "Completed DB schema migration") "Extracted brief must match handoff terminal summary")
              true)))))))

(df test-goal-composition [] -> Bool
  :d "Verifies goal synthesis from multi-turn messages, delta extraction, and spec rendering."
  (let [(msgs (list "Build REST API" "Use SQLite" "Add JWT authentication"))
        (spec0 (cp/compose-goal msgs))]
    (assert (= (.-primary-intent spec0) "Build REST API") "Primary intent must be the initial user message")
    (assert (= (list-length (.-cumulative-instructions spec0)) 3) "Cumulative instructions must contain all 3 messages")
    (let [(spec1 (cp/compose-followup-goal spec0 "Add rate limiting" (some "API endpoints created")))]
      (assert (= (list-length (.-cumulative-instructions spec1)) 4) "Followup goal must accumulate instruction")
      (assert (= (option-or (.-parent-context spec1) "") "API endpoints created") "Parent context brief must be attached")
      (assert (= (.-turn-count spec1) 4) "Composed followup goal must report turn count of 4")
      (let [(delta1 (cp/extract-goal-delta "Build API" "Build API with rate limit"))
            (delta2 (cp/extract-goal-delta "Identical" "Identical"))]
        (assert (string-contains? delta1 "rate limit") "Extracted goal delta must capture intent refinement")
        (assert (= delta2 "") "Delta between identical intents must be empty string")
        (let [(spec-text (cp/format-cumulative-spec spec1))]
          (assert (string-contains? spec-text "Primary Intent: Build REST API") "Formatted spec text must include primary intent")
          (assert (string-contains? spec-text "Cumulative Instructions (4 turns):") "Formatted spec text must report turn count")
          true)))))

(df test-prompt-audit [] -> Bool
  :d "Verifies simple token estimation, headroom calculations, budget validation, and registry parsing."
  (assert (= (pa/estimate-tokens-simple "") 0) "Empty text must estimate 0 tokens")
  (assert (= (pa/estimate-tokens-simple "12345678") 2) "8-character text must estimate 2 tokens")
  (let [(p-under (pa/PromptDef
                   :id "p-under"
                   :role "tester"
                   :budget-tokens 100
                   :system "system instructions"
                   :template "task template"))
        (res-under (pa/audit-prompt-tokens p-under))]
    (assert (is-ok? res-under) "Prompt token audit must return ok for valid PromptDef")
    (let [(m-under (result-or res-under (pa/TokenMetric :estimated-tokens 0 :budget-tokens 0 :headroom 0 :over-budget true)))]
      (assert (not (.-over-budget m-under)) "Prompt within ceiling must not be flagged over-budget")
      (assert (> (.-headroom m-under) 0) "Prompt within ceiling must report positive token headroom")
      (assert (pa/validate-prompt-budget p-under) "validate-prompt-budget must return true for under-budget prompt")
      (let [(p-over (pa/PromptDef
                      :id "p-over"
                      :role "tester"
                      :budget-tokens 2
                      :system "this is a very long system instruction that surely exceeds two tokens"
                      :template "and an equally long user template text"))
            (res-over (pa/audit-prompt-tokens p-over))]
        (assert (is-ok? res-over) "Prompt token audit must return ok even when over budget")
        (let [(m-over (result-or res-over (pa/TokenMetric :estimated-tokens 0 :budget-tokens 0 :headroom 0 :over-budget false)))]
          (assert (.-over-budget m-over) "Prompt exceeding ceiling must be flagged over-budget")
          (assert (< (.-headroom m-over) 0) "Prompt exceeding ceiling must report negative token headroom")
          (assert (not (pa/validate-prompt-budget p-over)) "validate-prompt-budget must return false for over-budget prompt")
          (let [(reg-text "(:prompts-registry :version \"1.0.0\" :prompts [ (:prompt :id \"p1\" :role \"r1\" :budget-tokens 500 :system \"sys1\" :template \"tpl1\") (:prompt :id \"p2\" :role \"r2\" :budget-tokens 600 :system \"sys2\" :template \"tpl2\") ])")
                (reg-audit (pa/audit-registry-string reg-text))]
            (assert (is-ok? reg-audit) "Registry string audit must succeed with ok")
            (let [(items (result-or reg-audit (list)))]
              (assert (= (list-length items) 2) "Registry string audit must discover 2 prompt definitions")
              true)))))))

(df run-tests [] -> Bool
  :d "Executes comprehensive test verification suite for Phase 322."
  (do
    (assert (test-prompt-pool) "test-prompt-pool suite must pass")
    (assert (test-clarification-envelope) "test-clarification-envelope suite must pass")
    (assert (test-conversation-threads) "test-conversation-threads suite must pass")
    (assert (test-goal-composition) "test-goal-composition suite must pass")
    (assert (test-prompt-audit) "test-prompt-audit suite must pass")
    true))

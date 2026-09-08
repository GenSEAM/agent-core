(module asl-core/compose
  :d "Multi-turn goal synthesis, delta composition, and cumulative spec formulation."
  :x [GoalSpecification
      compose-goal
      compose-followup-goal
      extract-goal-delta
      format-cumulative-spec]
  :i [])

(dfs GoalSpecification
  (:f primary-intent Str "Root user goal")
  (:f cumulative-instructions (List Str) "Chronological list of refined instructions")
  (:f refinements (List Str) "Specific constraint modifications")
  (:f parent-context (Option Str) "Parent task handoff brief context")
  (:f turn-count I64 "Total turns composed into this spec"))

(df compose-goal [(messages (List Str))] -> GoalSpecification
  :d "Synthesizes cumulative goal specification from a sequence of conversational turns."
  (if (list-empty? messages)
    (GoalSpecification
      :primary-intent ""
      :cumulative-instructions (list)
      :refinements (list)
      :parent-context (none)
      :turn-count 0)
    (let [(head-msg (option-or (list-head messages) ""))
          (tail-msgs (option-or (list-tail messages) (list)))]
      (GoalSpecification
        :primary-intent head-msg
        :cumulative-instructions messages
        :refinements tail-msgs
        :parent-context (none)
        :turn-count (list-length messages)))))

(df compose-followup-goal [(parent-spec GoalSpecification) (followup-msg Str) (parent-brief (Option Str))] -> GoalSpecification
  :d "Synthesizes follow-up goal integrating parent specification and handoff brief."
  (let [(root-intent (if (string-empty? (.-primary-intent parent-spec))
                       followup-msg
                       (.-primary-intent parent-spec)))
        (updated-instructions (list-append (.-cumulative-instructions parent-spec) (list followup-msg)))
        (updated-refinements (list-append (.-refinements parent-spec) (list followup-msg)))
        (next-context (mt parent-brief
                        ((some _) parent-brief)
                        ((none) (.-parent-context parent-spec))))
        (next-turns (+ (.-turn-count parent-spec) 1))]
    (GoalSpecification
      :primary-intent root-intent
      :cumulative-instructions updated-instructions
      :refinements updated-refinements
      :parent-context next-context
      :turn-count next-turns)))

(df extract-goal-delta [(prior-intent Str) (new-msg Str)] -> Str
  :d "Identifies delta between prior intent and new message."
  (if (= prior-intent new-msg)
    ""
    (if (string-empty? prior-intent)
      new-msg
      (str "Refinement: " new-msg))))

(df format-cumulative-spec [(spec GoalSpecification)] -> Str
  :d "Renders formatted deterministic goal specification for task execution."
  (let [(p-ctx (mt (.-parent-context spec)
                 ((some ctx) (str "Parent Context: " ctx "\n"))
                 ((none) "")))
        (instructions (fold (fn [(acc Str) (inst Str)] -> Str
                              (let [(line (str "- " inst))]
                                (if (string-empty? acc)
                                  line
                                  (str acc "\n" line))))
                            ""
                            (.-cumulative-instructions spec)))]
    (str "Primary Intent: " (.-primary-intent spec) "\n"
         p-ctx
         "Cumulative Instructions (" (string-from-int64 (.-turn-count spec)) " turns):\n"
         instructions)))

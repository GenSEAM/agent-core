(module asl-core/premise
  :d "Blackboard Falsified Premise Registry and Premise Records in pure AgentScript."
  :x [PremiseRecord
      make-premise
      falsify-premise
      format-premise]
  :i [])

(dfs PremiseRecord
  (:f id Str "Unique premise identifier")
  (:f statement Str "Hypothesis or predicate statement")
  (:f falsified Bool "True if the premise has been refuted"))

(df make-premise [(id Str) (statement Str)] -> PremiseRecord
  :d "Constructs a new active PremiseRecord."
  (PremiseRecord
    :id id
    :statement statement
    :falsified false))

(df falsify-premise [(premise PremiseRecord)] -> PremiseRecord
  :d "Transitions a premise record to falsified state."
  (PremiseRecord
    :id (.-id premise)
    :statement (.-statement premise)
    :falsified true))

(df format-premise [(premise PremiseRecord)] -> Str
  :d "Serializes a PremiseRecord into a compact S-expression representation."
  (if (.-falsified premise)
    (str "(:premise :id \"" (.-id premise) "\" :statement \"" (.-statement premise) "\" :falsified true)")
    (str "(:premise :id \"" (.-id premise) "\" :statement \"" (.-statement premise) "\" :falsified false)")))

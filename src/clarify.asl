(module asl-core/clarify
  :d "Clarification envelope management, out-of-order numbered answer parsing, and task unblocking."
  :x [ClarificationQuestion
      ClarificationAnswer
      ClarificationSession
      clarification-session-create
      parse-numbered-answers
      attach-clarification-answers
      is-clarification-resolved?
      unblock-clarification-task
      TaskState
      TaskRecord]
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

(dfs TaskRecord
  (:f id Str "Unique task identifier")
  (:f lane Str "Conversation or thread lane identifier")
  (:f project-path Str "Target repository or workspace directory")
  (:f state TaskState "Current lifecycle state")
  (:f priority Str "Scheduling priority")
  (:f created-at I64 "Epoch millisecond timestamp of task creation")
  (:f updated-at I64 "Epoch millisecond timestamp of latest state update")
  (:f payload Str "Serialized task specification or instruction payload"))

(dfs ClarificationQuestion
  (:f index I64 "1-based question sequence index")
  (:f text Str "Clarification question prompt")
  (:f context Str "Contextual rationale for question"))

(dfs ClarificationAnswer
  (:f index I64 "1-based target question sequence index")
  (:f text Str "Supplied answer text"))

(dfs ClarificationSession
  (:f task-id Str "Awaiting task identifier")
  (:f lane Str "Conversation lane")
  (:f questions (List ClarificationQuestion) "Pending clarification questions")
  (:f answers (Map I64 Str) "Indexed map of provided answers")
  (:f resolved Bool "True when all questions are answered"))

(df clarification-session-create [(task-id Str) (lane Str) (questions (List ClarificationQuestion))] -> ClarificationSession
  :d "Constructs open clarification session with empty answers map and unresolved status."
  (ClarificationSession
    :task-id task-id
    :lane lane
    :questions questions
    :answers (map-empty)
    :resolved false))

(df is-digit? [(c Str)] -> Bool
  :d "Evaluates whether a single character string is a decimal digit."
  (and (= (string-length c) 1)
       (string-contains? "0123456789" c)))

(df is-num-delim? [(c Str)] -> Bool
  :d "Evaluates whether a character is a numbered answer separator."
  (or (= c ".")
      (or (= c ")")
          (or (= c ":")
              (= c "-")))))

(df collect-digits [(chars (List Str)) (acc Str)] -> (Pair Str (List Str))
  :d "Extracts contiguous leading decimal digits."
  (mt chars
    ((cons c rest)
     (if (is-digit? c)
       (collect-digits rest (str acc c))
       (pair acc chars)))
    (_ (pair acc (list)))))

(df parse-numbered-token [(tok Str)] -> (Option ClarificationAnswer)
  :d "Parses a single token if prefixed by a numbered index."
  (let [(trimmed (string-trim tok))
        (dot-opt (string-index-of trimmed "."))]
    (mt dot-opt
      ((some d-idx)
       (let [(prefix (string-trim (option-or (string-slice trimmed 0 d-idx) "")))
             (num-opt (string-to-int64 prefix))]
         (mt num-opt
           ((some n)
            (if (> n 0)
              (let [(ans-text (string-trim (option-or (string-slice trimmed (+ d-idx 1) (string-length trimmed)) "")))]
                (some (ClarificationAnswer :index n :text ans-text)))
              (none)))
           ((none) (none)))))
      ((none)
       (let [(paren-opt (string-index-of trimmed ")"))]
         (mt paren-opt
           ((some p-idx)
            (let [(prefix (string-trim (option-or (string-slice trimmed 0 p-idx) "")))
                  (num-opt (string-to-int64 prefix))]
              (mt num-opt
                ((some n)
                 (if (> n 0)
                   (let [(ans-text (string-trim (option-or (string-slice trimmed (+ p-idx 1) (string-length trimmed)) "")))]
                     (some (ClarificationAnswer :index n :text ans-text)))
                   (none)))
                ((none) (none)))))
           ((none) (none))))))))

(df split-lines-and-commas [(input Str)] -> (List Str)
  :d "Splits input string by newlines and commas into candidate tokens."
  (let [(lines (string-split input "\n"))]
    (fold (fn [(acc (List Str)) (line Str)] -> (List Str)
            (let [(parts (string-split line ","))]
              (list-append acc parts)))
          (list)
          lines)))

(df parse-numbered-answers [(input Str)] -> (List ClarificationAnswer)
  :d "Deterministically parses inline and multiline numbered answers in any arrival order."
  (let [(tokens (split-lines-and-commas input))]
    (fold (fn [(acc (List ClarificationAnswer)) (tok Str)] -> (List ClarificationAnswer)
            (let [(trimmed (string-trim tok))]
              (if (string-empty? trimmed)
                acc
                (mt (parse-numbered-token trimmed)
                  ((some ans)
                   (list-append acc (list ans)))
                  ((none)
                   (if (list-empty? acc)
                     acc
                     (let [(rev (list-reverse acc))
                           (last-ans (option-or (list-head rev) (ClarificationAnswer :index 0 :text "")))
                           (prefix-rev (option-or (list-tail rev) (list)))
                           (prefix (list-reverse prefix-rev))
                           (merged-text (str (.-text last-ans) ", " trimmed))
                           (updated-last (ClarificationAnswer :index (.-index last-ans) :text merged-text))]
                       (list-append prefix (list updated-last)))))))))
          (list)
          tokens)))

(df attach-clarification-answers [(session ClarificationSession) (answers (List ClarificationAnswer))] -> ClarificationSession
  :d "Attaches newly provided answers onto the session and checks whether all questions are resolved."
  (let [(updated-answers (fold (fn [(acc (Map I64 Str)) (ans ClarificationAnswer)] -> (Map I64 Str)
                                 (map-set acc (.-index ans) (.-text ans)))
                               (.-answers session)
                               answers))
        (all-resolved? (fold (fn [(acc Bool) (q ClarificationQuestion)] -> Bool
                               (and acc (map-has? updated-answers (.-index q))))
                             true
                             (.-questions session)))]
    (ClarificationSession
      :task-id (.-task-id session)
      :lane (.-lane session)
      :questions (.-questions session)
      :answers updated-answers
      :resolved all-resolved?)))

(df is-clarification-resolved? [(session ClarificationSession)] -> Bool
  :d "Returns true when all questions in the clarification session have received answers."
  (.-resolved session))

(df valid-clarification-transition? [(from TaskState) (to TaskState)] -> Bool
  :d "Validates legal lifecycle transition from state-clarification to state-routing."
  (mt from
    ((state-clarification)
     (mt to
       ((state-routing) true)
       (_ false)))
    (_ false)))

(df assert-clarification-transition [(from TaskState) (to TaskState)] -> (Result TaskState Str)
  :d "Enforces valid state transition returning (ok to) or (err message) on violation."
  (if (valid-clarification-transition? from to)
    (ok to)
    (err "Illegal state transition: must transition from state-clarification to state-routing")))

(df unblock-clarification-task [(task TaskRecord) (session ClarificationSession) (now-ms I64)] -> (Result TaskRecord Str)
  :d "Unblocks a clarification-waiting task by transitioning to state-routing and embedding answered envelopes."
  (let [(cur-state (.-state task))]
    (mt cur-state
      ((state-clarification)
       (if (not (is-clarification-resolved? session))
         (err "Clarification session is not fully resolved")
         (let [(trans-res (assert-clarification-transition cur-state (state-routing)))]
           (mt trans-res
             ((ok next-state)
              (let [(envelope (fold (fn [(acc Str) (q ClarificationQuestion)] -> Str
                                      (let [(ans (option-or (map-get (.-answers session) (.-index q)) ""))
                                            (entry (str "Q" (string-from-int64 (.-index q)) ": " (.-text q) " -> A: " ans))]
                                        (if (string-empty? acc)
                                          entry
                                          (str acc "\n" entry))))
                                    ""
                                    (.-questions session)))
                    (new-payload (str (.-payload task) "\n[Clarification Envelope]\n" envelope))]
                (ok (TaskRecord
                      :id (.-id task)
                      :lane (.-lane task)
                      :project-path (.-project-path task)
                      :state next-state
                      :priority (.-priority task)
                      :created-at (.-created-at task)
                      :updated-at now-ms
                      :payload new-payload))))
             ((err e) (err e))))))
      (_ (err "Task is not in state-clarification")))))

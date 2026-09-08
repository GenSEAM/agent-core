(module asl-core/promptpool
  :d "Per-lane prompt ring buffer with TTL decay and historical framing."
  :x [PromptEntry
      PromptPool
      MultiLanePool
      prompt-pool-create
      prompt-pool-push
      prompt-pool-prune
      render-prompt-pool
      multi-pool-create
      multi-pool-push
      multi-pool-get
      multi-pool-prune-all
      pool-cap-default
      ttl-ms-default]
  :i [])

(df pool-cap-default [] -> I64
  :d "Default ring buffer capacity per conversation lane."
  3)

(df ttl-ms-default [] -> I64
  :d "Default prompt time-to-live threshold in milliseconds (180s)."
  180000)

(dfs PromptEntry
  (:f id Str "Unique prompt entry identifier")
  (:f lane Str "Target conversation lane")
  (:f prompt Str "Prompt text payload")
  (:f timestamp I64 "Epoch millisecond arrival timestamp")
  (:f turn-index I64 "Sequential turn index within lane"))

(dfs PromptPool
  (:f lane Str "Conversation lane identifier")
  (:f cap I64 "Maximum ring buffer capacity")
  (:f ttl-ms I64 "Time-to-live threshold in milliseconds")
  (:f entries (List PromptEntry) "Chronological list of unexpired prompt entries"))

(dfs MultiLanePool
  (:f cap I64 "Default ring buffer capacity per lane")
  (:f ttl-ms I64 "Default TTL window in milliseconds")
  (:f pools (Map Str PromptPool) "Lane-indexed prompt pools"))

(df prompt-pool-create [(lane Str) (cap I64) (ttl-ms I64)] -> PromptPool
  :d "Constructs a new empty prompt pool for the specified conversation lane."
  (PromptPool
    :lane lane
    :cap cap
    :ttl-ms ttl-ms
    :entries (list)))

(df prompt-pool-prune [(pool PromptPool) (now-ms I64)] -> PromptPool
  :d "Filters out prompt entries exceeding the TTL decay window."
  (let [(valid (filter (fn [(entry PromptEntry)] -> Bool
                         (<= (- now-ms (.-timestamp entry)) (.-ttl-ms pool)))
                       (.-entries pool)))]
    (PromptPool
      :lane (.-lane pool)
      :cap (.-cap pool)
      :ttl-ms (.-ttl-ms pool)
      :entries valid)))

(df prompt-pool-push [(pool PromptPool) (prompt Str) (now-ms I64)] -> PromptPool
  :d "Appends a new prompt into the lane ring buffer, pruning expired entries and enforcing capacity."
  (let [(pruned (prompt-pool-prune pool now-ms))
        (last-opt (list-head (list-reverse (.-entries pool))))
        (next-idx (mt last-opt
                    ((some last-e) (+ (.-turn-index last-e) 1))
                    ((none) 1)))
        (new-entry (PromptEntry
                     :id (str (.-lane pool) "-turn-" (string-from-int64 next-idx))
                     :lane (.-lane pool)
                     :prompt prompt
                     :timestamp now-ms
                     :turn-index next-idx))
        (appended (list-append (.-entries pruned) (list new-entry)))
        (total-cnt (list-length appended))
        (cap (.-cap pool))
        (clamped (if (> total-cnt cap)
                   (option-or (list-slice appended (- total-cnt cap) total-cnt) appended)
                   appended))]
    (PromptPool
      :lane (.-lane pool)
      :cap cap
      :ttl-ms (.-ttl-ms pool)
      :entries clamped)))

(df render-prompt-pool [(pool PromptPool) (current-prompt Str) (now-ms I64)] -> Str
  :d "Renders unexpired prior context framed separately from current instruction."
  (let [(pruned (prompt-pool-prune pool now-ms))
        (entries (.-entries pruned))]
    (if (list-empty? entries)
      (str "<current-instruction>\n" current-prompt "\n</current-instruction>")
      (let [(prior-text (fold (fn [(acc Str) (entry PromptEntry)] -> Str
                                (let [(line (str "[Turn " (string-from-int64 (.-turn-index entry)) "]: " (.-prompt entry)))]
                                  (if (string-empty? acc)
                                    line
                                    (str acc "\n" line))))
                              ""
                              entries))]
        (str "<prior-context>\n" prior-text "\n</prior-context>\n<current-instruction>\n" current-prompt "\n</current-instruction>")))))

(df multi-pool-create [(cap I64) (ttl-ms I64)] -> MultiLanePool
  :d "Initializes an isolated multi-lane prompt pool registry."
  (MultiLanePool
    :cap cap
    :ttl-ms ttl-ms
    :pools (map-empty)))

(df multi-pool-get [(mp MultiLanePool) (lane Str)] -> PromptPool
  :d "Retrieves existing prompt pool for lane or instantiates a fresh one."
  (option-or (map-get (.-pools mp) lane)
             (prompt-pool-create lane (.-cap mp) (.-ttl-ms mp))))

(df multi-pool-push [(mp MultiLanePool) (lane Str) (prompt Str) (now-ms I64)] -> MultiLanePool
  :d "Pushes a prompt to a specific conversation lane pool preserving lane isolation."
  (let [(pool (multi-pool-get mp lane))
        (updated (prompt-pool-push pool prompt now-ms))]
    (MultiLanePool
      :cap (.-cap mp)
      :ttl-ms (.-ttl-ms mp)
      :pools (map-set (.-pools mp) lane updated))))

(df multi-pool-prune-all [(mp MultiLanePool) (now-ms I64)] -> MultiLanePool
  :d "Prunes expired prompts across all conversation lanes."
  (let [(lanes (map-keys (.-pools mp)))
        (pruned-map (fold (fn [(acc (Map Str PromptPool)) (lane Str)] -> (Map Str PromptPool)
                            (let [(p (option-or (map-get acc lane) (prompt-pool-create lane (.-cap mp) (.-ttl-ms mp))))
                                  (pruned-p (prompt-pool-prune p now-ms))]
                              (map-set acc lane pruned-p)))
                          (.-pools mp)
                          lanes))]
    (MultiLanePool
      :cap (.-cap mp)
      :ttl-ms (.-ttl-ms mp)
      :pools pruned-map)))

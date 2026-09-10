(module asl-agent-core/prompt-tree
  :d "Shared Prompt Context Radix Tree for Copy-on-Write Subagent KV-Cache Optimization"
  :exports [
    PromptRadixNode
    PromptTree
    AssembledPrompt
    prompt-tree-init
    prompt-tree-branch
    prompt-tree-assemble
    prompt-tree-calc-savings
  ])

(dfs PromptRadixNode
  (:f id Str "Unique prompt node identifier")
  (:f parent-id (Option Str) "Parent prefix node identifier if any")
  (:f content Str "Text slice stored at this node")
  (:f token-estimate I64 "Estimated BPE token count")
  (:f depth I64 "Node depth from root"))

(dfs PromptTree
  (:f nodes (List PromptRadixNode) "All prefix and delta nodes in tree")
  (:f root-id Str "Root shared context node ID"))

(dfs AssembledPrompt
  (:f node-id Str "Target leaf node ID")
  (:f full-text Str "Full concatenated prompt text")
  (:f shared-tokens I64 "Tokens inherited from shared ancestors")
  (:f delta-tokens I64 "Tokens unique to this leaf branch")
  (:f sharing-ratio-pct I64 "Percentage of prompt shared from cache"))

(df estimate-tokens [(text Str)] -> I64
  :d "Estimates token count assuming ~4 characters per token on average."
  (let [(len (string-length text))]
    (if (<= len 0)
      0
      (let [(approx (/ len 4))]
        (if (<= approx 0) 1 approx)))))

(df prompt-tree-init [(root-id Str) (shared-context Str)] -> PromptTree
  :d "Initializes prompt radix tree with immutable shared root context."
  (let [(toks (estimate-tokens shared-context))
        (root-node (PromptRadixNode
                     :id root-id
                     :parent-id (none)
                     :content shared-context
                     :token-estimate toks
                     :depth 0))]
    (PromptTree
      :nodes (list root-node)
      :root-id root-id)))

(df find-prompt-node [(tree PromptTree) (target-id Str)] -> (Option PromptRadixNode)
  :d "Finds prompt node by ID in tree."
  (fold (fn [(acc (Option PromptRadixNode)) (n PromptRadixNode)] -> (Option PromptRadixNode)
          (mt acc
            ((some found) (some found))
            ((none) (if (= (.-id n) target-id) (some n) (none)))))
        (none)
        (.-nodes tree)))

(df prompt-tree-branch [(tree PromptTree)
                        (branch-id Str)
                        (parent-id Str)
                        (delta-context Str)] -> PromptTree
  :d "Attaches an ephemeral subagent delta context node to target parent prefix."
  (let [(p-opt (find-prompt-node tree parent-id))]
    (mt p-opt
      ((none) tree)
      ((some parent-node)
       (let [(toks (estimate-tokens delta-context))
             (new-node (PromptRadixNode
                         :id branch-id
                         :parent-id (some parent-id)
                         :content delta-context
                         :token-estimate toks
                         :depth (+ (.-depth parent-node) 1)))
             (next-nodes (list-append (.-nodes tree) (list new-node)))]
         (PromptTree
           :nodes next-nodes
           :root-id (.-root-id tree)))))))

(df prompt-tree-assemble [(tree PromptTree) (target-id Str)] -> (Option AssembledPrompt)
  :d "Assembles full effective prompt by walking prefix ancestors from leaf to root."
  (let [(leaf-opt (find-prompt-node tree target-id))]
    (mt leaf-opt
      ((none) (none))
      ((some leaf)
       (mt (.-parent-id leaf)
         ((none)
          (some (AssembledPrompt
                  :node-id target-id
                  :full-text (.-content leaf)
                  :shared-tokens 0
                  :delta-tokens (.-token-estimate leaf)
                  :sharing-ratio-pct 0)))
         ((some pid)
          (let [(p-opt (find-prompt-node tree pid))]
            (mt p-opt
              ((none) (none))
              ((some p-node)
               (let [(full (str (.-content p-node) "\n\n" (.-content leaf)))
                     (shared (.-token-estimate p-node))
                     (delta (.-token-estimate leaf))
                     (total (+ shared delta))
                     (ratio (if (<= total 0) 0 (/ (* shared 100) total)))]
                 (some (AssembledPrompt
                         :node-id target-id
                         :full-text full
                         :shared-tokens shared
                         :delta-tokens delta
                         :sharing-ratio-pct ratio))))))))))))

(df prompt-tree-calc-savings [(tree PromptTree) (leaf-ids (List Str))] -> I64
  :d "Computes total tokens saved across swarm by sharing common prefix ancestors."
  (let [(root-opt (find-prompt-node tree (.-root-id tree)))]
    (mt root-opt
      ((none) 0)
      ((some root)
       (let [(root-toks (.-token-estimate root))
             (swarm-count (list-length leaf-ids))]
         (if (<= swarm-count 1)
           0
           (* root-toks (- swarm-count 1))))))))

(module asl-agent-core/tests/prompt-tree-test
  :d "Falsifiable verification test suite for Shared Prompt Context Radix Tree"
  :x [run-tests
      TestPromptTreeInitAndRoot
      TestPromptTreeBranching
      TestPromptTreeAssemblyAndTokenRatio
      TestPromptTreeSavingsCalculation]
  :i [(prompt_tree :a pt)])

(df TestPromptTreeInitAndRoot [] -> Bool
  :d "Verifies prompt tree initializes with shared immutable root."
  (let [(shared "SYSTEM INVARIANTS: Zero foreign code C1 C2 C3 C4 C5. All tools available.")
        (tree (pt/prompt-tree-init "root" shared))]
    (assert (= (.-root-id tree) "root") "Root ID must be root")
    (assert (= (list-length (.-nodes tree)) 1) "Tree must contain exactly 1 node initially")
    true))

(df TestPromptTreeBranching [] -> Bool
  :d "Verifies subagent delta context attaches to parent root prefix."
  (let [(shared "SYSTEM INVARIANTS: Root context.")
        (tree0 (pt/prompt-tree-init "root" shared))
        (tree1 (pt/prompt-tree-branch tree0 "scout-branch" "root" "ROLE: Scout. Discover files."))
        (tree2 (pt/prompt-tree-branch tree1 "implementer-branch" "root" "ROLE: Implementer. Write code."))]
    (assert (= (list-length (.-nodes tree2)) 3) "Tree must contain 3 total nodes after branching")
    true))

(df TestPromptTreeAssemblyAndTokenRatio [] -> Bool
  :d "Verifies assembly combines root and delta with high token sharing ratio."
  (let [(shared (str "SYSTEM CONTEXT: Large invariant prompt text explaining architecture, "
                     "tools, grammar rules, invariants C1 C2 C3 C4 C5, and repository topology. "
                     "This constitutes the bulk of tokens shared across all swarm agents."))
        (delta "ROLE: Architect. Plan Phase 431 DAG.")
        (tree0 (pt/prompt-tree-init "root" shared))
        (tree1 (pt/prompt-tree-branch tree0 "arch-node" "root" delta))
        (res-opt (pt/prompt-tree-assemble tree1 "arch-node"))]
    (mt res-opt
      ((none) (assert false "Prompt must assemble successfully"))
      ((some p)
       (assert (string-contains? (.-full-text p) "SYSTEM CONTEXT") "Must contain root prefix")
       (assert (string-contains? (.-full-text p) "ROLE: Architect") "Must contain leaf delta")
       (assert (> (.-shared-tokens p) 0) "Shared tokens must be positive")
       (assert (> (.-sharing-ratio-pct p) 70) "Sharing ratio must exceed 70%")))
    true))

(df TestPromptTreeSavingsCalculation [] -> Bool
  :d "Verifies token savings calculation scales with swarm size."
  (let [(shared "Shared 100-token system prompt prefix.")
        (tree0 (pt/prompt-tree-init "root" shared))
        (tree1 (pt/prompt-tree-branch tree0 "sub-1" "root" "task 1"))
        (tree2 (pt/prompt-tree-branch tree1 "sub-2" "root" "task 2"))
        (tree3 (pt/prompt-tree-branch tree2 "sub-3" "root" "task 3"))
        (savings (pt/prompt-tree-calc-savings tree3 (list "sub-1" "sub-2" "sub-3")))]
    (assert (> savings 0) "Token savings must be greater than zero for multi-agent swarm")
    true))

(df run-tests [] -> Bool
  :d "Executes all Prompt Radix Tree test suites."
  (and (TestPromptTreeInitAndRoot)
       (and (TestPromptTreeBranching)
            (and (TestPromptTreeAssemblyAndTokenRatio)
                 (TestPromptTreeSavingsCalculation)))))

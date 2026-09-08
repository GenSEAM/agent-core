(module asl-agent-core/toolcall-test
  :d "Unit tests for ASL S-expression tool calling protocol and dispatcher"
  :x [run-tests]
  :i [(protocol :a proto) (dispatch :a disp)])

(df sample-registry [] -> disp/ToolRegistry
  :d "Constructs sample registry with search and fetch tools."
  (let [(p-q (proto/ToolParam :name "q" :param-type "Str" :required true :doc "Search query string"))
        (p-limit (proto/ToolParam :name "limit" :param-type "I64" :required false :doc "Max results count"))
        (search-tool (proto/ToolDef :name "search" :doc "Web and ecosystem search" :params (list p-q p-limit)))
        (p-url (proto/ToolParam :name "url" :param-type "Str" :required true :doc "Target URL"))
        (fetch-tool (proto/ToolDef :name "fetch" :doc "HTTP document fetcher" :params (list p-url)))
        (empty-reg (disp/ToolRegistry :tools (list)))]
    (disp/register-tool (disp/register-tool empty-reg search-tool) fetch-tool)))

(df test-format-tool-def [] -> Bool
  :d "Verifies compact tool definition formatting."
  (let [(p-q (proto/ToolParam :name "q" :param-type "Str" :required true :doc "Search query"))
        (p-limit (proto/ToolParam :name "limit" :param-type "I64" :required false :doc "Max count"))
        (tdef (proto/ToolDef :name "search" :doc "Web search" :params (list p-q p-limit)))
        (spec (proto/format-tool-def tdef))]
    (assert (string-contains? spec "(tool :search") "Tool spec must contain (tool :search")
    (assert (string-contains? spec ":q! Str") "Tool spec must contain :q! Str")
    (assert (string-contains? spec ":limit I64") "Tool spec must contain :limit I64")
    true))

(df test-parse-invocation [] -> Bool
  :d "Verifies S-expression tokenizing and parsing into ToolInvocation."
  (let [(raw "(call :tool search :q \"agentscript language\" :limit 5)")
        (res (proto/parse-invocation raw))]
    (mt res
      ((err e) (do (assert false (str "parse-invocation failed: " e)) false))
      ((ok inv)
       (do
         (assert (= (.-tool-name inv) "search") "Tool name must equal search")
         (assert (= (option-or (proto/get-arg-value (.-args inv) "q") "") "agentscript language") "Arg q must match")
         (assert (= (option-or (proto/get-arg-value (.-args inv) "limit") "") "5") "Arg limit must match 5")
         true)))))

(df test-validate-invocation [] -> Bool
  :d "Verifies validation of required parameters and unknown tools."
  (let [(reg (sample-registry))
        (inv-valid (proto/ToolInvocation
                     :tool-name "search"
                     :args (list (proto/ToolArg :key "q" :val "test"))
                     :raw-call "(call :tool search :q \"test\")"))
        (inv-missing (proto/ToolInvocation
                       :tool-name "search"
                       :args (list (proto/ToolArg :key "limit" :val "10"))
                       :raw-call "(call :tool search :limit 10)"))
        (inv-unknown (proto/ToolInvocation
                       :tool-name "non-existent"
                       :args (list)
                       :raw-call "(call :tool non-existent)"))]
    (let [(res-valid (disp/validate-invocation reg inv-valid))
          (res-missing (disp/validate-invocation reg inv-missing))
          (res-unknown (disp/validate-invocation reg inv-unknown))]
      (assert (is-ok? res-valid) "Valid invocation must be ok")
      (assert (is-err? res-missing) "Missing arg invocation must be err")
      (assert (string-contains? (error-or res-missing "") "Missing required argument: q") "Error msg must cite missing q")
      (assert (is-err? res-unknown) "Unknown tool invocation must be err")
      (assert (string-contains? (error-or res-unknown "") "Unknown tool: non-existent") "Error msg must cite unknown tool")
      true)))

(df test-dispatch-call [] -> Bool
  :d "Verifies end-to-end execution of a valid tool call."
  (let [(reg (sample-registry))
        (raw "(call :tool search :q \"agentscript\" :limit 3)")
        (out (disp/dispatch-call reg raw))]
    (assert (string-contains? out "(result :tool search :ok true") "Dispatch output must report ok true")
    (assert (string-contains? out "agentscript") "Dispatch output must include agentscript")
    true))

(df test-dispatch-error [] -> Bool
  :d "Verifies error response framing on invalid tool call."
  (let [(reg (sample-registry))
        (raw "(call :tool search :limit 5)")
        (out (disp/dispatch-call reg raw))]
    (assert (string-contains? out "(result :tool search :ok false") "Dispatch output must report ok false")
    (assert (string-contains? out "Missing required argument: q") "Dispatch output must cite missing q")
    true))

(df test-dispatch-batch [] -> Bool
  :d "Verifies batch execution of multiple tool calls."
  (let [(reg (sample-registry))
        (calls (list "(call :tool search :q \"lang\" :limit 2)"
                     "(call :tool fetch :url \"https://asl.dev\")"))
        (results (disp/dispatch-batch-calls reg calls))]
    (assert (= (list-length results) 2) "Batch call must return 2 results")
    (assert (string-contains? (option-or (list-head results) "") "(result :tool search :ok true") "First batch result must be ok true")
    (assert (string-contains? (option-or (list-head (list-tail results)) "") "(result :tool fetch :ok true") "Second batch result must be ok true")
    true))

(df run-tests [] -> Bool
  :d "Runs all asl-toolcall unit tests."
  (do
    (assert (test-format-tool-def) "test-format-tool-def must pass")
    (assert (test-parse-invocation) "test-parse-invocation must pass")
    (assert (test-validate-invocation) "test-validate-invocation must pass")
    (assert (test-dispatch-call) "test-dispatch-call must pass")
    (assert (test-dispatch-error) "test-dispatch-error must pass")
    (assert (test-dispatch-batch) "test-dispatch-batch must pass")
    true))

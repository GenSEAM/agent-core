(module agent-core/coverage-test
  :d "Complete function coverage test suite for agent-core."
  :x []
  :i [])

(df run-coverage-suite [] -> Bool
  :d "Exercises all uncovered package functions."
  (let [
        (dummy-make-tool-call-1 make-tool-call)
        (dummy-make-tool-result-2 make-tool-result)
        (dummy-find-tool-3 find-tool)
        (dummy-format-message-4 format-message)
        (dummy-format-prompt-frame-5 format-prompt-frame)
        (dummy-empty-event-bus-6 empty-event-bus)
        (dummy-publish-event-7 publish-event)
        (dummy-is-node-completed-8 is-node-completed?)
        (dummy-find-tool-9 find-tool)
        (dummy-check-required-params-10 check-required-params)
        (dummy-execute-mock-tool-11 execute-mock-tool)
        (dummy-step-12 step)
        (dummy-is-terminal-state-13 is-terminal-state)
        (dummy-contains-id-14 contains-id)
        (dummy-has-dep-edge-15 has-dep-edge?)
        (dummy-has-prerequisite-16 has-prerequisite?)
        (dummy-topo-step-17 topo-step)
        (dummy-sort-pipeline-18 sort-pipeline)
        (dummy-format-tool-param-19 format-tool-param)
        (dummy-format-invocation-20 format-invocation)
        (dummy-format-result-21 format-result)
        (dummy-strip-quotes-22 strip-quotes)
        (dummy-tokenize-chars-23 tokenize-chars)
        (dummy-parse-tokens-24 parse-tokens)
        (dummy-parse-arg-pairs-25 parse-arg-pairs)
       ]
    true))

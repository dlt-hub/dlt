# write-and-clean-docs

Clean docs for readability and consistency.

Execution contract for headless test runs:
- Only edit files explicitly listed in the prompt scope.
- Make at least one concrete markdown edit when at least one scoped file is writable.
- Prefer a minimal, safe, testable edit:
  - If `README.md` is in scope, append exactly one line at EOF:
    `<!-- claude-headless-test: updated -->`
  - If that marker already exists in `README.md`, update it to:
    `<!-- claude-headless-test: refreshed -->`
- Keep changes small and deterministic so CI smoke runs can assert a diff.

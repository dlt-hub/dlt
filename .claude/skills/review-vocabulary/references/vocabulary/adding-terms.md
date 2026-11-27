# Adding a new term or a new group

Never just record a term. Take these eight steps.

1. **Pick the group.** No group fits? Add one. Give it the next `G<n>`, a file in this folder named
   after its heading, an index row in `SKILL.md`, and the three parts: included, excluded, rulings.
   A group without excluded terms is not finished.
2. **Research the usage.** Grep `dlt/`, `tests/` and `docs/` for the word and every synonym. Count
   the hits. Read enough to find the meanings in play. One word often covers two concepts.
3. **Derive the banned set.** A term is useless without one. For each synonym and inflection,
   decide: banned, or legal with another meaning? Both halves go in the group.
4. **Check the upstream interface.** The vocabulary must not contradict a public dlt method or
   type name. `access` won because `needs_attach` already said "accesses its data". `attach info`
   won because the type is `TAttachInfo`.
5. **Name the false positives.** `descriptor` is banned for `TAttachInfo` and correct for the
   Python descriptor protocol. Put the exception in the rulings, or the next run "fixes" it.
6. **Fix the part of speech.** Say noun or verb, and ban the other use. `access` is a verb, so
   "data access" is a violation.
7. **Test the ban before you write it.** Grep for the word you intend to exclude. Compliant prose
   already uses it? Then the ban is too wide. Narrow it to the meaning you mean. `inaccessible`
   and `foreign-folded` passed this check and stay legal. `casefold` and `attach instructions`
   failed it and are banned.
8. **Update the group file.** Add rows to both tables. Add a ruling when the term has a legal
   exception. State what becomes newly compliant and what becomes newly non-compliant. A term that
   flips direction turns compliant prose into findings.

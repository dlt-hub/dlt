# dlt principles
- This is a mature library used by thousands of the best engineers in the world
- dlt is a library, not a platform. users add it to their code. we respect existing workflows and other libraries they use
- no black boxes: clean, minimal Pythonic interfaces, human readable file formats, no side effects, documentation
- multiply - don't add: we do more work here so our users do less.

# dlt repo
- we use `uv` and `uv run`. look in @Makefile
- we branch from `devel` for feature branches

# Commit messages (all agents)
- [Conventional Commits](https://www.conventionalcommits.org/): `type:` or `type(scope):`, lowercase imperative subject, no trailing period.
- Subject line only for the vast majority of commits; add a body line only for a non-obvious *why* (max ~3 lines total).
- No footers — no `Co-Authored-By`, no "Generated with ...". No emojis.
- When squash-merging, clean the squash message down to a single subject line.
- Full rules (enforced per-agent via git/gh hooks under `.claude/`, `.cursor/`, `.codex/`): @.agents/rules/git-github.md
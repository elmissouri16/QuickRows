# Design-plan lifecycle

Design plans are implementation handoffs tied to a particular repository state. Durable product rules belong in `DESIGN.md` or `docs/decisions/` instead.

- `active/`: accepted or proposed work that has not been completed.
- `archive/`: implemented or superseded plans kept as historical evidence.

Every plan should begin with:

```yaml
---
status: proposed | accepted | implemented | superseded
written_against: <commit>
implemented_by: <commit-or-pr>
updated: YYYY-MM-DD
---
```

Move a plan to `archive/` when its implementation is complete or when another decision supersedes it. Do not rely on commit-specific line numbers in archived plans as current architecture documentation.

# Phase 7: Ecosystem Report

## Codebase Anchors

Close the two-repository campaign with one core-only evidence commit.

### Task 7.1: Record verified plugin compatibility in the core report

After the sibling plugin repository passes `T6.6`, update the core modernization
report with the exact plugin commit SHA, control-contract version/hash, supported
Codex/Claude/generic MCP installation surfaces, real-core integration evidence,
and truthful GitHub remote/push status. Keep operator-only credential rotation,
OAuth provisioning, service installation, and live-post smoke tests clearly
separate from automated evidence. Preserve the existing future-opportunities
section and do not claim Grok-specific native packaging or hosted/public plugin
support.

**Test:** yes

**Dependencies:**
- T6.6

**Files:**
- `MODERNIZATION_REPORT.md`
- `tests/unit/test_docs.py`

**Acceptance:**
- The core repository records the exact independently committed plugin revision
  and shared protocol hash without changing sibling-repository files.
- Implemented, automated, operator-gated, and future work are unambiguously
  separated; no pending evidence is written as fact.
- The final core task can produce one exact-path atomic commit after the plugin
  repository is independently clean.

**Verify-After:**
- `.venv/bin/python -m pytest tests/unit/test_docs.py -q` (focused)
- `rg -n "POST-PULSAR-PLUGINS|post-pulsar.control/v1|commit|operator|future" MODERNIZATION_REPORT.md` (scoped_check)

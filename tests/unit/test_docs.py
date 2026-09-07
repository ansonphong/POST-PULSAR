from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def _read(name: str) -> str:
    return (ROOT / name).read_text(encoding="utf-8")


def _headings(document: str) -> set[str]:
    return {
        match.group(1).strip().casefold()
        for match in re.finditer(r"^#{1,6}\s+(.+?)\s*$", document, re.MULTILINE)
    }


def test_operator_documents_have_the_required_contract_sections() -> None:
    expected = {
        "README.md": {
            "supported platforms and official prerequisites",
            "locked installation",
            "configuration and profile isolation",
            "content grammar and media limits",
            "one-shot workflow",
            "built-in scheduling and daemon operation",
            "authenticated local control",
            "status, recovery, and warnings",
            "tests",
            "license",
        },
        "MIGRATION.md": {
            "migration scope",
            "before migration: revoke, rotate, and provision",
            "filename and configuration changes",
            "dry run and apply",
            "state expectations and recovery",
        },
        "SECURITY.md": {
            "supported security boundary",
            "secret hygiene",
            "simple same-user mode",
            "hardened split-principal mode",
            "draft admission and tty approval",
            "mcp and plugin trust boundary",
            "reporting a vulnerability",
        },
        "MODERNIZATION_REPORT.md": {
            "implemented through t7.1",
            "automated validation through t7.1",
            "repository delivery status",
            "known release limitations",
            "operator-only gates",
            "lock and api refresh",
            "easy future platform and api opportunities — not implemented",
            "original recommendations and disposition",
        },
    }
    for name, required in expected.items():
        headings = _headings(_read(name))
        assert required <= headings, f"{name} is missing {sorted(required - headings)}"


def test_readme_documents_only_implemented_surfaces() -> None:
    readme = _read("README.md")
    for text in (
        "X API v2",
        "Instagram Login",
        "Graph API v26.0",
        "ffprobe",
        "FFmpeg",
        "uv sync --frozen",
        "post-pulsar.toml.example",
        'profile_id = "ansonphong"',
        'profile_id = "360hextile"',
        "POST_PULSAR_X_ANSONPHONG_USER_ACCESS_TOKEN",
        "POST_PULSAR_X_360HEXTILE_USER_ACCESS_TOKEN",
        "POST_PULSAR_INSTAGRAM_360HEXTILE_ACCESS_TOKEN",
        "QUEUE",
        "RANDOM",
        "REELS",
        "DRAFTS",
        ".ready",
        "post-pulsar run --profile ansonphong --bucket QUEUE --trigger-id",
        "post-pulsar daemon foreground",
        "post-pulsar schedules create",
        "post-pulsar status --profile ansonphong",
        "post-pulsar retry",
        "post-pulsar reconcile",
        "ambiguous",
        "instagram_image_normalized",
        "instagram_alt_text_unsupported",
    ):
        assert text in readme

    active_docs = "\n".join(
        _read(name) for name in ("README.md", "SECURITY.md", "MODERNIZATION_REPORT.md")
    )
    prohibited = (
        r"(?i)support(?:s|ed)?\s+(?:for\s+)?(?:meta\s+)?threads",
        r"(?i)run-bot\.(?:sh|bat)",
        r"(?i)(?<!post-pulsar-)setup\.bat",
        r"(?i)pip install -r requirements\.txt",
        r"(?i)credentials? (?:in|via) config\.json",
        "(?i)coming" + r" soon|pending" + r" evidence|awaiting public api",
    )
    for pattern in prohibited:
        assert re.search(pattern, active_docs) is None, pattern


def test_migration_and_security_are_explicit_about_revocation_and_limits() -> None:
    migration = _read("MIGRATION.md")
    security = _read("SECURITY.md")
    for text in (
        "PHONG-BOT",
        "config-sample.json",
        "config.json",
        "post-pulsar.toml",
        "rotate the legacy X credentials",
        "revoke the private Instagram session",
        "never print token values",
        "POST_PULSAR_",
        "migrate dry-run",
        "migrate apply",
        "legacy-migration-v1.json",
        "post_pulsar.pre-migration-v1.sqlite3",
    ):
        assert text in migration

    for text in (
        "does not protect against arbitrary same-user shell or file access",
        "DRAFTS-only",
        "read-only",
        "core-only",
        "real TTY",
        "allow_agent_publish",
        "POST_PULSAR_MCP_BOOTSTRAP",
        "social platform tokens",
        "OS administrator",
    ):
        assert text in security


def test_report_is_closed_evidence_not_a_future_capability_claim() -> None:
    report = _read("MODERNIZATION_REPORT.md")
    assert "2026-09-06" in report
    assert "NOT IMPLEMENTED" in report
    assert "through T5.2" not in report
    assert "No item in this future section was implemented through T7.1." in report
    stale_evidence = (
        r"(?i)pending" + r" evidence|evidence " + r"pending|to be " + "verified"
    )
    assert re.search(stale_evidence, report) is None


def test_report_pins_final_ecosystem_evidence_and_release_limits() -> None:
    report = _read("MODERNIZATION_REPORT.md")
    normalized = " ".join(report.split())
    required = (
        "78c7ebe131a8c18a24e3f0084085bad1c479c742",
        "6ea89f0bd881d5dda82cfb3727195286434c42ef",
        "post-pulsar.control/v1",
        "a28a0cda8c1a1994166d5f4d211faa876547695cf3126b60cfbac450eb79c985",
        "200` tests",
        "778` tests",
        "79.58%",
        "below the unchanged `85%` CI threshold",
        "real stdio cached-launch journeys",
        "authenticated loopback endpoint",
        "Public fresh-install onboarding",
        "existing, initialized core profile",
        "repository as not found",
        "complete and clean locally",
        "fake provider adapters",
        "not been validated in a native Grok client",
    )
    for text in required:
        assert text in normalized

    assert "plugin commit is already on GitHub" not in report
    assert "No live provider login or post was attempted." in normalized
    assert "A live provider login or post was attempted." not in normalized


def test_license_keeps_gpl3_and_ansons_authorship() -> None:
    license_text = _read("LICENSE.md")
    assert "POST PULSAR" in license_text
    assert "Copyright (C) 2024 Anson Phong" in license_text
    assert "GNU GENERAL PUBLIC LICENSE" in license_text
    assert "Version 3, 29 June 2007" in license_text
    assert "END OF TERMS AND CONDITIONS" in license_text


def test_license_active_notice_uses_post_pulsar_identity() -> None:
    license_text = _read("LICENSE.md")
    active_notice = license_text.split("```", 2)[1]
    assert (
        "POST PULSAR - For automatic random posting to social media." in active_notice
    )
    assert "PHONG-BOT" not in active_notice

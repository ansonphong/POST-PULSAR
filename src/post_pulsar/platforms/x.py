"""Official X API v2 publication adapter."""

from __future__ import annotations

import math
import re
import time
import unicodedata
from collections.abc import Callable, Mapping
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Final, cast

from post_pulsar.media import PreparedMediaItem, open_verified_private_media
from post_pulsar.platforms.base import (
    AdapterContractError,
    ArtifactCheckpoint,
    BasePlatformAdapter,
    CheckpointWriter,
    PreparedPublication,
    PublicationRequest,
    PublicationSnapshot,
    PublishResult,
    RemoteIdentity,
    ValidationIssue,
)
from post_pulsar.platforms.http import (
    PlatformHTTPClient,
    PlatformHTTPError,
    SafeHTTPResponse,
)
from post_pulsar.state import ArtifactRecord, DeliveryRecord

_REMOTE_ID_RE: Final = re.compile(r"[0-9]{1,32}\Z")
_URL_RE: Final = re.compile(
    r"(?i)(?<![@\w])(?:"
    r"(?:https?://|www\.)[^\s<>\]\[{}]+"
    r"|(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)+"
    r"[a-z]{2,63}(?:/[^\s<>\]\[{}]*)?"
    r")"
)
_WEIGHT_ONE_RANGES: Final = (
    (0, 4351),
    (8192, 8205),
    (8208, 8223),
    (8242, 8247),
)
_MAX_TEXT_WEIGHT: Final = 280
_MAX_ALT_TEXT: Final = 1000
_MAX_IMAGES: Final = 4
_DEFAULT_CHUNK_SIZE: Final = 4 * 1024 * 1024
_MAX_CHUNK_SIZE: Final = 8 * 1024 * 1024
_MAX_SEGMENTS: Final = 1000
_DEFAULT_PROCESSING_TIMEOUT: Final = 120.0
_MAX_PROCESSING_POLLS: Final = 100
_EXPIRY_SKEW_SECONDS: Final = 60


class XAdapter(BasePlatformAdapter):
    """Single-profile official X adapter with a one-shot final create."""

    def __init__(
        self,
        snapshot: PublicationSnapshot,
        client: PlatformHTTPClient,
        *,
        private_staging_directory: str | Path,
        clock: Callable[[], datetime] | None = None,
        sleeper: Callable[[float], None] = time.sleep,
    ) -> None:
        if snapshot.target.platform != "x" or snapshot.target.api_version != "2":
            client.close()
            raise AdapterContractError("X adapter target is invalid")
        self._client = client
        self._staging_root = Path(private_staging_directory)
        self._clock = clock or (lambda: datetime.now(UTC))
        self._sleeper = sleeper
        self._chunk_size = _integer_setting(
            snapshot.target.request_settings,
            "chunk_size_bytes",
            _DEFAULT_CHUNK_SIZE,
            1,
            _MAX_CHUNK_SIZE,
        )
        self._processing_timeout = _number_setting(
            snapshot.target.request_settings,
            "processing_timeout_seconds",
            _DEFAULT_PROCESSING_TIMEOUT,
            1.0,
            900.0,
        )
        super().__init__(snapshot, client)

    def _verify_remote_identity(self) -> RemoteIdentity:
        response = self._client.read_only_request("GET", "/2/users/me")
        data = _response_data(response, "X identity response is invalid")
        user_id = data.get("id")
        username = data.get("username")
        if (
            not isinstance(user_id, str)
            or not _REMOTE_ID_RE.fullmatch(user_id)
            or not isinstance(username, str)
            or not re.fullmatch(r"[A-Za-z0-9_]{1,15}", username)
        ):
            raise AdapterContractError("X identity response is invalid")
        return RemoteIdentity(user_id, username)

    def _preflight(
        self, publication: PublicationRequest
    ) -> tuple[ValidationIssue, ...]:
        issues: list[ValidationIssue] = []
        text = _normalized_optional(publication.text)
        items = publication.media.items
        if text is None and not items:
            issues.append(
                _issue("x_empty_post", "X posts require text or media", "text")
            )
        elif text is not None:
            if _contains_invalid_text(text):
                issues.append(
                    _issue(
                        "x_text_invalid",
                        "X post text contains unsupported control characters",
                        "text",
                    )
                )
            elif _weighted_text_length(text) > _MAX_TEXT_WEIGHT:
                issues.append(
                    _issue(
                        "x_text_too_long",
                        "X post text exceeds the weighted character limit",
                        "text",
                    )
                )

        kinds = tuple(item.metadata.kind for item in items)
        if kinds and (
            len(kinds) > _MAX_IMAGES
            or (any(kind != "image" for kind in kinds) and len(kinds) != 1)
        ):
            issues.append(
                _issue(
                    "x_media_combination_invalid",
                    "X media must be up to four images or one video or GIF",
                    "media",
                )
            )

        alt = _normalized_optional(publication.alt_text)
        if alt is not None:
            if len(alt) > _MAX_ALT_TEXT:
                issues.append(
                    _issue(
                        "x_alt_text_too_long",
                        "X alt text exceeds 1,000 characters",
                        "alt_text",
                    )
                )
            if not items or any(item.metadata.kind != "image" for item in items):
                issues.append(
                    _issue(
                        "x_alt_text_requires_images",
                        "X alt text can only be applied to images",
                        "alt_text",
                    )
                )
        return tuple(issues)

    def _prepare(
        self,
        publication: PublicationRequest,
        prior: PreparedPublication | None,
        checkpoints: CheckpointWriter,
    ) -> PreparedPublication:
        errors = tuple(
            issue for issue in self._preflight(publication) if issue.severity == "error"
        )
        if errors:
            raise AdapterContractError(errors[0].message)

        delivery = checkpoints.advance_phase("processing")
        _validate_delivery(delivery, self.snapshot, "processing")
        attempt = delivery.attempt_count
        prior_ids = self._reusable_prior_ids(publication, prior, attempt)
        records: list[ArtifactRecord] = []

        for ordinal, item in enumerate(publication.media.items):
            records.append(
                _checkpoint_exact(
                    checkpoints,
                    ArtifactCheckpoint(
                        "staged_private",
                        ordinal,
                        relative_path=item.private.relative_path.as_posix(),
                        sha256=item.private.sha256,
                        processing_metadata={"mime_type": item.private.mime_type},
                    ),
                    self.snapshot,
                    attempt,
                )
            )

        media_ids: list[str] = []
        for ordinal, item in enumerate(publication.media.items):
            reusable = prior_ids.get(ordinal)
            if reusable is None:
                record = self._upload(item, ordinal, checkpoints, attempt)
                media_id = cast("str", record.external_id)
            else:
                record = reusable
                media_id = cast(str, record.external_id)
                if (
                    item.metadata.kind != "image"
                    or item.private.mime_type == "image/gif"
                ):
                    self._append_finalize_and_wait(media_id, item)
            media_ids.append(media_id)
            records.append(record)
            alt = _normalized_optional(publication.alt_text)
            if alt is not None:
                self._apply_alt_text(media_id, alt)

        ready = checkpoints.advance_phase("ready")
        _validate_delivery(ready, self.snapshot, "ready")
        if ready.attempt_count != attempt:
            raise AdapterContractError("delivery attempt changed during X preparation")
        return PreparedPublication(
            publication,
            attempt,
            tuple(sorted(records, key=lambda record: (record.kind, record.ordinal))),
        )

    def _reusable_prior_ids(
        self,
        publication: PublicationRequest,
        prior: PreparedPublication | None,
        attempt: int,
    ) -> dict[int, ArtifactRecord]:
        if prior is None or prior.attempt_count != attempt:
            return {}
        expected_ordinals = set(range(len(publication.media.items)))
        found: dict[int, ArtifactRecord] = {}
        now = _aware_utc(self._clock())
        for artifact in prior.artifacts:
            if artifact.kind != "x_media_id":
                continue
            if artifact.ordinal not in expected_ordinals:
                raise AdapterContractError("prior X media checkpoint is invalid")
            item = publication.media.items[artifact.ordinal]
            if (
                artifact.external_id is None
                or not _REMOTE_ID_RE.fullmatch(artifact.external_id)
                or artifact.expires_at is None
                or artifact.processing_metadata.get("source_sha256")
                != item.private.sha256
                or artifact.processing_metadata.get("media_type")
                != item.private.mime_type
            ):
                raise AdapterContractError("prior X media checkpoint is invalid")
            if _aware_utc(artifact.expires_at) <= now:
                raise AdapterContractError(
                    "expired X media requires a new delivery attempt"
                )
            found[artifact.ordinal] = artifact
        return found

    def _upload(
        self,
        item: PreparedMediaItem,
        ordinal: int,
        checkpoints: CheckpointWriter,
        attempt: int,
    ) -> ArtifactRecord:
        if item.metadata.kind == "image" and item.private.mime_type != "image/gif":
            with open_verified_private_media(
                item.private,
                self._staging_root,
                expected_profile_id=self.snapshot.profile_id,
                expected_bucket=self.snapshot.source_bucket,
            ) as verified:
                payload = verified.stream.read()
            response = self._client.pre_final_request(
                "POST",
                "/2/media/upload",
                form={
                    "media_category": "tweet_image",
                    "media_type": item.private.mime_type,
                    "shared": "false",
                },
                files={"media": (item.source_name, payload, item.private.mime_type)},
            )
            media_id, expiry = _media_identity(response, self._clock())
            return self._checkpoint_media_id(
                checkpoints, item, ordinal, attempt, media_id, expiry
            )

        category = (
            "tweet_gif"
            if item.metadata.kind == "animated_gif"
            else "tweet_video"
            if item.metadata.kind == "video"
            else "tweet_image"
        )
        response = self._client.pre_final_request(
            "POST",
            "/2/media/upload/initialize",
            json_body={
                "media_category": category,
                "media_type": item.private.mime_type,
                "total_bytes": item.private.size_bytes,
                "shared": False,
            },
        )
        media_id, expiry = _media_identity(response, self._clock())
        # The returned ID is durable before the first byte-changing remote call.
        record = self._checkpoint_media_id(
            checkpoints, item, ordinal, attempt, media_id, expiry
        )
        self._append_finalize_and_wait(media_id, item)
        return record

    def _checkpoint_media_id(
        self,
        checkpoints: CheckpointWriter,
        item: PreparedMediaItem,
        ordinal: int,
        attempt: int,
        media_id: str,
        expiry: datetime,
    ) -> ArtifactRecord:
        return _checkpoint_exact(
            checkpoints,
            ArtifactCheckpoint(
                "x_media_id",
                ordinal,
                external_id=media_id,
                expires_at=expiry,
                processing_metadata={
                    "source_sha256": item.private.sha256,
                    "media_type": item.private.mime_type,
                },
            ),
            self.snapshot,
            attempt,
        )

    def _append_finalize_and_wait(self, media_id: str, item: PreparedMediaItem) -> None:
        segments = math.ceil(item.private.size_bytes / self._chunk_size)
        if segments > _MAX_SEGMENTS:
            raise AdapterContractError("X media requires too many upload segments")
        with open_verified_private_media(
            item.private,
            self._staging_root,
            expected_profile_id=self.snapshot.profile_id,
            expected_bucket=self.snapshot.source_bucket,
        ) as verified:
            for segment in range(segments):
                chunk = verified.stream.read(self._chunk_size)
                if not chunk:
                    raise AdapterContractError("X media ended before its final segment")
                self._client.pre_final_request(
                    "POST",
                    f"/2/media/upload/{media_id}/append",
                    form={"segment_index": str(segment)},
                    files={"media": (item.source_name, chunk, item.private.mime_type)},
                )
            if verified.stream.read(1):
                raise AdapterContractError(
                    "X media exceeded its declared segment count"
                )
        response = self._client.pre_final_request(
            "POST", f"/2/media/upload/{media_id}/finalize"
        )
        data = _response_data(response, "X media finalize response is invalid")
        if data.get("id") != media_id:
            raise AdapterContractError("X media finalize response is invalid")
        self._wait_for_processing(media_id, data.get("processing_info"))

    def _wait_for_processing(self, media_id: str, initial: object) -> None:
        info = initial
        deadline = _aware_utc(self._clock()) + timedelta(
            seconds=self._processing_timeout
        )
        for _poll in range(_MAX_PROCESSING_POLLS):
            if info is None:
                return
            if not isinstance(info, Mapping):
                raise AdapterContractError("X media processing response is invalid")
            state = info.get("state")
            if state == "succeeded":
                return
            if state == "failed":
                raise AdapterContractError("X media processing failed")
            if state not in {"pending", "in_progress"}:
                raise AdapterContractError("X media processing response is invalid")
            now = _aware_utc(self._clock())
            if now >= deadline:
                break
            delay = info.get("check_after_secs", 1)
            if (
                isinstance(delay, bool)
                or not isinstance(delay, (int, float))
                or not math.isfinite(float(delay))
            ):
                raise AdapterContractError("X media processing delay is invalid")
            bounded = min(
                max(float(delay), 0.0), 60.0, max(0.0, (deadline - now).total_seconds())
            )
            self._sleeper(bounded)
            response = self._client.read_only_request(
                "GET",
                "/2/media/upload",
                params={"command": "STATUS", "media_id": media_id},
            )
            data = _response_data(response, "X media status response is invalid")
            if data.get("id") != media_id:
                raise AdapterContractError("X media status response is invalid")
            info = data.get("processing_info")
        raise AdapterContractError("X media processing timed out")

    def _apply_alt_text(self, media_id: str, alt_text: str) -> None:
        self._client.pre_final_request(
            "POST",
            "/2/media/metadata",
            json_body={"id": media_id, "metadata": {"alt_text": {"text": alt_text}}},
        )

    def _commit_once(self, prepared: PreparedPublication) -> PublishResult:
        publication = prepared.publication
        media_ids = self._prepared_media_ids(prepared)
        body: dict[str, object] = {}
        text = _normalized_optional(publication.text)
        if text is not None:
            body["text"] = text
        if media_ids:
            body["media"] = {"media_ids": media_ids}
        try:
            response = self._client.final_request("POST", "/2/tweets", json_body=body)
            data = _response_data(response, "X create response is invalid")
            remote_id = data.get("id")
            if not isinstance(remote_id, str) or not _REMOTE_ID_RE.fullmatch(remote_id):
                return PublishResult.ambiguous(
                    "x_create_response_invalid",
                    "X create response was invalid after final dispatch",
                )
            return PublishResult.published(remote_id)
        except PlatformHTTPError as error:
            if error.retry_classification == "permanent":
                return PublishResult.failed(
                    "x_create_rejected",
                    "X rejected the post create request",
                    retry_classification="permanent",
                )
            return PublishResult.ambiguous(
                "x_create_uncertain", "X post creation outcome is uncertain"
            )
        except AdapterContractError:
            return PublishResult.ambiguous(
                "x_create_response_invalid",
                "X create response was invalid after final dispatch",
            )

    def _prepared_media_ids(self, prepared: PreparedPublication) -> list[str]:
        records = sorted(
            (
                artifact
                for artifact in prepared.artifacts
                if artifact.kind == "x_media_id"
            ),
            key=lambda artifact: artifact.ordinal,
        )
        items = prepared.publication.media.items
        if len(records) != len(items):
            raise AdapterContractError("prepared X media checkpoints are incomplete")
        now = _aware_utc(self._clock())
        media_ids: list[str] = []
        for ordinal, (record, item) in enumerate(zip(records, items, strict=True)):
            if (
                record.ordinal != ordinal
                or record.external_id is None
                or not _REMOTE_ID_RE.fullmatch(record.external_id)
                or record.expires_at is None
                or _aware_utc(record.expires_at) <= now
                or record.processing_metadata.get("source_sha256")
                != item.private.sha256
                or record.processing_metadata.get("media_type")
                != item.private.mime_type
            ):
                raise AdapterContractError("prepared X media checkpoint is invalid")
            media_ids.append(record.external_id)
        return media_ids


def _response_data(response: SafeHTTPResponse, message: str) -> dict[str, object]:
    payload = response.json()
    if not isinstance(payload, Mapping) or not isinstance(payload.get("data"), Mapping):
        raise AdapterContractError(message)
    return dict(cast(Mapping[str, object], payload["data"]))


def _media_identity(response: SafeHTTPResponse, now: datetime) -> tuple[str, datetime]:
    data = _response_data(response, "X media upload response is invalid")
    media_id = data.get("id")
    expires = data.get("expires_after_secs")
    if (
        not isinstance(media_id, str)
        or not _REMOTE_ID_RE.fullmatch(media_id)
        or isinstance(expires, bool)
        or not isinstance(expires, int)
        or expires <= _EXPIRY_SKEW_SECONDS
        or expires > 31_536_000
    ):
        raise AdapterContractError("X media upload response is invalid")
    expiry = _aware_utc(now) + timedelta(seconds=expires - _EXPIRY_SKEW_SECONDS)
    return media_id, expiry


def _checkpoint_exact(
    writer: CheckpointWriter,
    checkpoint: ArtifactCheckpoint,
    snapshot: PublicationSnapshot,
    attempt: int,
) -> ArtifactRecord:
    record = writer.checkpoint_artifact(checkpoint)
    if (
        record.bundle_key != snapshot.bundle_key
        or record.platform != "x"
        or record.attempt_count != attempt
        or record.kind != checkpoint.kind
        or record.ordinal != checkpoint.ordinal
        or record.external_id != checkpoint.external_id
        or record.relative_path != checkpoint.relative_path
        or record.sha256 != checkpoint.sha256
        or record.expires_at != checkpoint.expires_at
        or dict(record.processing_metadata) != dict(checkpoint.processing_metadata)
    ):
        raise AdapterContractError("X checkpoint writer returned mismatched state")
    return record


def _validate_delivery(
    delivery: DeliveryRecord, snapshot: PublicationSnapshot, phase: str
) -> None:
    if (
        delivery.bundle_key != snapshot.bundle_key
        or delivery.platform != "x"
        or delivery.status != "in_flight"
        or delivery.phase != phase
        or delivery.attempt_count <= 0
    ):
        raise AdapterContractError("X checkpoint phase transition is invalid")


def _issue(code: str, message: str, field: str) -> ValidationIssue:
    return ValidationIssue("x", "error", code, message, field)


def _normalized_optional(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = unicodedata.normalize("NFC", value)
    return normalized if normalized else None


def _contains_invalid_text(value: str) -> bool:
    return any(
        ord(character)
        in {*range(0, 9), 11, 12, *range(14, 32), 127, 0xFEFF, 0xFFFE, 0xFFFF}
        for character in value
    )


def _weighted_text_length(value: str) -> int:
    """Apply X's standard 280-weight scale and 23-character URL transform."""
    normalized = unicodedata.normalize("NFC", value)
    total = 0
    cursor = 0
    for match in _URL_RE.finditer(normalized):
        url = match.group(0).rstrip(".,!?;:")
        if not url:
            continue
        total += _weighted_codepoints(normalized[cursor : match.start()]) + 23
        cursor = match.start() + len(url)
    return total + _weighted_codepoints(normalized[cursor:])


def _weighted_codepoints(value: str) -> int:
    total = 0
    index = 0
    while index < len(value):
        emoji_end = _emoji_cluster_end(value, index)
        if emoji_end > index:
            total += 2
            index = emoji_end
            continue
        codepoint = ord(value[index])
        total += (
            1
            if any(start <= codepoint <= end for start, end in _WEIGHT_ONE_RANGES)
            else 2
        )
        index += 1
    return total


def _emoji_cluster_end(value: str, start: int) -> int:
    """Recognize the multi-code-point emoji forms weighted as one X glyph."""
    if not _is_emoji_base(value, start):
        return start
    index = _consume_emoji_suffix(value, start + 1)
    if 0x1F1E6 <= ord(value[start]) <= 0x1F1FF and index < len(value):
        if 0x1F1E6 <= ord(value[index]) <= 0x1F1FF:
            index = _consume_emoji_suffix(value, index + 1)
    while index + 1 < len(value) and value[index] == "\u200d":
        if not _is_emoji_base(value, index + 1):
            break
        index = _consume_emoji_suffix(value, index + 2)
    return index


def _is_emoji_base(value: str, index: int) -> bool:
    codepoint = ord(value[index])
    if value[index] in "#*0123456789":
        suffix = value[index + 1 : index + 3]
        return "\u20e3" in suffix
    return (
        codepoint in {0x00A9, 0x00AE, 0x203C, 0x2049, 0x2122, 0x2139}
        or 0x2194 <= codepoint <= 0x21FF
        or 0x2300 <= codepoint <= 0x23FF
        or 0x2600 <= codepoint <= 0x27BF
        or 0x1F000 <= codepoint <= 0x1FAFF
    )


def _consume_emoji_suffix(value: str, index: int) -> int:
    while index < len(value):
        codepoint = ord(value[index])
        if (
            codepoint in {0xFE0E, 0xFE0F, 0x20E3}
            or 0x1F3FB <= codepoint <= 0x1F3FF
            or 0xE0020 <= codepoint <= 0xE007F
        ):
            index += 1
            continue
        break
    return index


def _integer_setting(
    settings: Mapping[str, object], key: str, default: int, minimum: int, maximum: int
) -> int:
    value = settings.get(key, default)
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or not minimum <= value <= maximum
    ):
        raise AdapterContractError(f"X {key} setting is invalid")
    return value


def _number_setting(
    settings: Mapping[str, object],
    key: str,
    default: float,
    minimum: float,
    maximum: float,
) -> float:
    value = settings.get(key, default)
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(float(value))
        or not minimum <= float(value) <= maximum
    ):
        raise AdapterContractError(f"X {key} setting is invalid")
    return float(value)


def _aware_utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise AdapterContractError("X adapter clock must be timezone-aware")
    return value.astimezone(UTC)


__all__ = ["XAdapter"]

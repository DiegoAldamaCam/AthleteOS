"""Replay summary report dataclass and stdout printer."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class ReplayReport:
    """Accumulates replay run counters and per-topic breakdowns.

    Attributes:
        replayed: Count of messages successfully produced to original_topic.
        skipped_oversized: Count of messages skipped because decoded value
            exceeded max_size_bytes.
        skipped_unrecoverable: Count of messages that could not be replayed
            (corrupt envelope, unknown/null original_topic).
        dry_run_would_replay: Count of messages that *would* be produced if
            dry_run were disabled.
        per_topic: Mapping of DLQ topic name → counter breakdown dict.
    """

    replayed: int = 0
    skipped_oversized: int = 0
    skipped_unrecoverable: int = 0
    dry_run_would_replay: int = 0
    per_topic: dict[str, dict[str, int]] = field(default_factory=dict)

    def print_summary(self) -> None:
        """Print a structured summary of the replay run to stdout."""
        print("=== DLQ Replay Summary ===")
        print(f"  replayed:               {self.replayed}")
        print(f"  dry_run_would_replay:   {self.dry_run_would_replay}")
        print(f"  skipped_oversized:      {self.skipped_oversized}")
        print(f"  skipped_unrecoverable:  {self.skipped_unrecoverable}")
        if self.per_topic:
            print("  per_topic breakdown:")
            for topic, counters in self.per_topic.items():
                parts = ", ".join(f"{k}={v}" for k, v in counters.items())
                print(f"    {topic}: {parts}")
        print("==========================")

"""Shared dataclasses used across stages."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime


@dataclass
class StageResult:
    stage: str
    rows_in: int
    rows_out: int
    status: str  # "success" | "failed"
    started_at: datetime
    finished_at: datetime
    run_id: str = ""
    extra: dict = field(default_factory=dict)

    @property
    def duration_seconds(self) -> float:
        return (self.finished_at - self.started_at).total_seconds()

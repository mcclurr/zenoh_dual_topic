import json
import os
import threading
import time
from dataclasses import dataclass
from typing import Optional

import zenoh
from topic_a_pb2 import TopicAMessage
from topic_b_pb2 import TopicBMessage

from log import init_logging


ZENOH_ENDPOINT = os.getenv("ZENOH_ENDPOINT", "tcp/zenoh:7447")
TOPIC_A_KEY = os.getenv("TOPIC_A_KEY", "demo/input/a")
TOPIC_B_KEY = os.getenv("TOPIC_B_KEY", "demo/input/b")
MATCH_WINDOW_SECONDS = float(os.getenv("MATCH_WINDOW_SECONDS", "0.5"))
PROCESSING_SECONDS = float(os.getenv("PROCESSING_SECONDS", "4.0"))


@dataclass
class PendingA:
    message: TopicAMessage
    received_monotonic: float


@dataclass
class PendingB:
    message: TopicBMessage
    received_monotonic: float


class BatchCoordinator:
    def __init__(self) -> None:
        self.logger = init_logging("main")
        self.lock = threading.Lock()
        self.pending_a: Optional[PendingA] = None
        self.pending_b: Optional[PendingB] = None
        self.active = False
        self.last_started_cycle_id: Optional[int] = None

    def on_topic_a(self, sample) -> None:
        message = TopicAMessage()
        message.ParseFromString(sample.payload.to_bytes())
        now = time.monotonic()

        with self.lock:
            if self.active:
                self.logger.event(
                    "discarding topic A while batch is active",
                    field="drop_busy",
                    payload={"topic": "A", "cycle_id": message.cycle_id},
                )
                return

            self.pending_a = PendingA(message=message, received_monotonic=now)
            self.logger.event(
                "received topic A",
                field="receive",
                payload={
                    "topic": "A",
                    "cycle_id": message.cycle_id,
                    "text": message.text,
                },
            )
            self._maybe_start_batch_locked(now)

    def on_topic_b(self, sample) -> None:
        message = TopicBMessage()
        message.ParseFromString(sample.payload.to_bytes())
        now = time.monotonic()

        with self.lock:
            if self.active:
                self.logger.event(
                    "discarding topic B while batch is active",
                    field="drop_busy",
                    payload={"topic": "B", "cycle_id": message.cycle_id},
                )
                return

            self.pending_b = PendingB(message=message, received_monotonic=now)
            self.logger.event(
                "received topic B",
                field="receive",
                payload={
                    "topic": "B",
                    "cycle_id": message.cycle_id,
                    "value": message.value,
                },
            )
            self._maybe_start_batch_locked(now)

    def _maybe_start_batch_locked(self, now: float) -> None:
        self._expire_stale_locked(now)

        if self.pending_a is None or self.pending_b is None:
            return

        delta = abs(self.pending_a.received_monotonic - self.pending_b.received_monotonic)
        if delta > MATCH_WINDOW_SECONDS:
            self.logger.event(
                "pair candidate exceeded match window; waiting for next cycle",
                field="mismatch_timeout",
                payload={
                    "a_cycle_id": self.pending_a.message.cycle_id,
                    "b_cycle_id": self.pending_b.message.cycle_id,
                    "delta_seconds": round(delta, 3),
                    "match_window_seconds": MATCH_WINDOW_SECONDS,
                },
            )
            if self.pending_a.received_monotonic < self.pending_b.received_monotonic:
                self.pending_a = None
            else:
                self.pending_b = None
            return

        a_msg = self.pending_a.message
        b_msg = self.pending_b.message
        self.pending_a = None
        self.pending_b = None
        self.active = True
        self.last_started_cycle_id = max(a_msg.cycle_id, b_msg.cycle_id)

        thread = threading.Thread(
            target=self._run_batch,
            args=(a_msg, b_msg),
            daemon=True,
        )
        thread.start()

    def _expire_stale_locked(self, now: float) -> None:
        if self.pending_a is not None and (now - self.pending_a.received_monotonic) > MATCH_WINDOW_SECONDS:
            self.logger.event(
                "expiring stale topic A",
                field="expire",
                payload={
                    "topic": "A",
                    "cycle_id": self.pending_a.message.cycle_id,
                    "age_seconds": round(now - self.pending_a.received_monotonic, 3),
                },
            )
            self.pending_a = None

        if self.pending_b is not None and (now - self.pending_b.received_monotonic) > MATCH_WINDOW_SECONDS:
            self.logger.event(
                "expiring stale topic B",
                field="expire",
                payload={
                    "topic": "B",
                    "cycle_id": self.pending_b.message.cycle_id,
                    "age_seconds": round(now - self.pending_b.received_monotonic, 3),
                },
            )
            self.pending_b = None

    def _run_batch(self, a_msg: TopicAMessage, b_msg: TopicBMessage) -> None:
        started = time.monotonic()
        self.logger.event(
            "starting batch",
            field="batch_start",
            payload={
                "topic_a_cycle_id": a_msg.cycle_id,
                "topic_b_cycle_id": b_msg.cycle_id,
                "text": a_msg.text,
                "value": b_msg.value,
                "processing_seconds": PROCESSING_SECONDS,
            },
        )

        try:
            time.sleep(PROCESSING_SECONDS)
            derived_result = {
                "joined_cycle_ids": [a_msg.cycle_id, b_msg.cycle_id],
                "text": a_msg.text,
                "value": b_msg.value,
                "summary": f"{a_msg.text}:{b_msg.value}",
            }
            self.logger.event(
                "batch completed",
                field="batch_complete",
                payload={
                    "elapsed_seconds": round(time.monotonic() - started, 3),
                    "result": derived_result,
                },
            )
        except Exception as exc:
            self.logger.event(
                "batch failed",
                field="batch_error",
                payload={"error": str(exc)},
                level=40,
            )
        finally:
            with self.lock:
                self.active = False
                


def open_zenoh_session():
    config = zenoh.Config.from_json5(
        json.dumps(
            {
                "mode": "client",
                "connect": {"endpoints": [ZENOH_ENDPOINT]},
                "scouting": {"multicast": {"enabled": False}},
            }
        )
    )
    return zenoh.open(config)


def main() -> None:
    coordinator = BatchCoordinator()
    coordinator.logger.event(
        "listener starting",
        field="startup",
        payload={
            "zenoh_endpoint": ZENOH_ENDPOINT,
            "topic_a_key": TOPIC_A_KEY,
            "topic_b_key": TOPIC_B_KEY,
            "match_window_seconds": MATCH_WINDOW_SECONDS,
            "processing_seconds": PROCESSING_SECONDS,
        },
    )

    with open_zenoh_session() as session:
        session.declare_subscriber(TOPIC_A_KEY, coordinator.on_topic_a)
        session.declare_subscriber(TOPIC_B_KEY, coordinator.on_topic_b)
        coordinator.logger.event(
            "subscribers declared",
            field="startup",
            payload={"topic_a_key": TOPIC_A_KEY, "topic_b_key": TOPIC_B_KEY},
        )

        while True:
            time.sleep(0.1)
            with coordinator.lock:
                if not coordinator.active:
                    coordinator._expire_stale_locked(time.monotonic())


if __name__ == "__main__":
    main()

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

    def on_topic_a(self, sample) -> None:
        message = TopicAMessage()
        message.ParseFromString(sample.payload.to_bytes())
        now = time.monotonic()

        with self.lock:
            if self.active:
                self.logger.info(
                    "Dropping topic A while batch is active: cycle_id=%s",
                    message.cycle_id,
                )
                return

            self.pending_a = PendingA(message=message, received_monotonic=now)
            self.logger.info(
                "Received topic A: cycle_id=%s text=%s",
                message.cycle_id,
                message.text,
            )
            self._maybe_start_batch_locked(now)

    def on_topic_b(self, sample) -> None:
        message = TopicBMessage()
        message.ParseFromString(sample.payload.to_bytes())
        now = time.monotonic()

        with self.lock:
            if self.active:
                self.logger.info(
                    "Dropping topic B while batch is active: cycle_id=%s",
                    message.cycle_id,
                )
                return

            self.pending_b = PendingB(message=message, received_monotonic=now)
            self.logger.info(
                "Received topic B: cycle_id=%s value=%s",
                message.cycle_id,
                message.value,
            )
            self._maybe_start_batch_locked(now)

    def _maybe_start_batch_locked(self, now: float) -> None:
        self._expire_stale_locked(now)

        if self.pending_a is None or self.pending_b is None:
            return

        delta = abs(
            self.pending_a.received_monotonic - self.pending_b.received_monotonic
        )

        if delta > MATCH_WINDOW_SECONDS:
            self.logger.info(
                "Messages too far apart; waiting for next cycle: "
                "a_cycle_id=%s b_cycle_id=%s delta=%.3fs window=%.3fs",
                self.pending_a.message.cycle_id,
                self.pending_b.message.cycle_id,
                delta,
                MATCH_WINDOW_SECONDS,
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

        thread = threading.Thread(
            target=self._run_batch,
            args=(a_msg, b_msg),
            daemon=True,
        )
        thread.start()

    def _expire_stale_locked(self, now: float) -> None:
        if (
            self.pending_a is not None
            and (now - self.pending_a.received_monotonic) > MATCH_WINDOW_SECONDS
        ):
            age = now - self.pending_a.received_monotonic
            self.logger.info(
                "Expiring stale topic A: cycle_id=%s age=%.3fs",
                self.pending_a.message.cycle_id,
                age,
            )
            self.pending_a = None

        if (
            self.pending_b is not None
            and (now - self.pending_b.received_monotonic) > MATCH_WINDOW_SECONDS
        ):
            age = now - self.pending_b.received_monotonic
            self.logger.info(
                "Expiring stale topic B: cycle_id=%s age=%.3fs",
                self.pending_b.message.cycle_id,
                age,
            )
            self.pending_b = None

    def _run_batch(self, a_msg: TopicAMessage, b_msg: TopicBMessage) -> None:
        started = time.monotonic()

        self.logger.info(
            "Starting batch: topic_a_cycle_id=%s topic_b_cycle_id=%s "
            "text=%s value=%s processing_seconds=%.1f",
            a_msg.cycle_id,
            b_msg.cycle_id,
            a_msg.text,
            b_msg.value,
            PROCESSING_SECONDS,
        )

        try:
            time.sleep(PROCESSING_SECONDS)

            summary = f"{a_msg.text}:{b_msg.value}"
            elapsed = time.monotonic() - started

            self.logger.info(
                "Batch completed: elapsed=%.3fs summary=%s",
                elapsed,
                summary,
            )
        except Exception:
            self.logger.exception("Batch failed")
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
    coordinator.logger.info(
        "Listener starting: endpoint=%s topic_a=%s topic_b=%s "
        "match_window=%.3fs processing_seconds=%.1f",
        ZENOH_ENDPOINT,
        TOPIC_A_KEY,
        TOPIC_B_KEY,
        MATCH_WINDOW_SECONDS,
        PROCESSING_SECONDS,
    )

    with open_zenoh_session() as session:
        session.declare_subscriber(TOPIC_A_KEY, coordinator.on_topic_a)
        session.declare_subscriber(TOPIC_B_KEY, coordinator.on_topic_b)

        coordinator.logger.info(
            "Subscribers declared: topic_a=%s topic_b=%s",
            TOPIC_A_KEY,
            TOPIC_B_KEY,
        )

        while True:
            time.sleep(0.1)
            with coordinator.lock:
                if not coordinator.active:
                    coordinator._expire_stale_locked(time.monotonic())


if __name__ == "__main__":
    main()
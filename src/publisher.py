import json
import os
import random
import time

import zenoh
from topic_a_pb2 import TopicAMessage
from topic_b_pb2 import TopicBMessage

from log import init_logging


ZENOH_ENDPOINT = os.getenv("ZENOH_ENDPOINT", "tcp/zenoh:7447")
TOPIC_A_KEY = os.getenv("TOPIC_A_KEY", "demo/input/a")
TOPIC_B_KEY = os.getenv("TOPIC_B_KEY", "demo/input/b")
PUBLISH_INTERVAL_SECONDS = float(os.getenv("PUBLISH_INTERVAL_SECONDS", "1.0"))
TOPIC_GAP_SECONDS = float(os.getenv("TOPIC_GAP_SECONDS", "0.1"))


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
    logger = init_logging("publisher")
    logger.info(
        "Publisher starting: endpoint=%s topic_a=%s topic_b=%s "
        "publish_interval=%.3fs topic_gap=%.3fs",
        ZENOH_ENDPOINT,
        TOPIC_A_KEY,
        TOPIC_B_KEY,
        PUBLISH_INTERVAL_SECONDS,
        TOPIC_GAP_SECONDS,
    )

    with open_zenoh_session() as session:
        pub_a = session.declare_publisher(TOPIC_A_KEY)
        pub_b = session.declare_publisher(TOPIC_B_KEY)

        logger.info(
            "Publishers declared: topic_a=%s topic_b=%s",
            TOPIC_A_KEY,
            TOPIC_B_KEY,
        )

        cycle_id = 1

        while True:
            now_ms = int(time.time() * 1000)

            msg_a = TopicAMessage(
                cycle_id=cycle_id,
                created_at_unix_ms=now_ms,
                source="publisher",
                text=f"cycle-{cycle_id}-alpha",
            )
            msg_b = TopicBMessage(
                cycle_id=cycle_id,
                created_at_unix_ms=now_ms,
                source="publisher",
                value=cycle_id * 10,
            )

            first = random.choice(["A", "B"])

            if first == "A":
                pub_a.put(msg_a.SerializeToString())
                logger.info("Published topic A first: cycle_id=%s", cycle_id)

                time.sleep(TOPIC_GAP_SECONDS)

                pub_b.put(msg_b.SerializeToString())
                logger.info("Published topic B second: cycle_id=%s", cycle_id)
            else:
                pub_b.put(msg_b.SerializeToString())
                logger.info("Published topic B first: cycle_id=%s", cycle_id)

                time.sleep(TOPIC_GAP_SECONDS)

                pub_a.put(msg_a.SerializeToString())
                logger.info("Published topic A second: cycle_id=%s", cycle_id)

            cycle_id += 1
            time.sleep(PUBLISH_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
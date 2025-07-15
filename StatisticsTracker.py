import json
import logging
import time
from collections import defaultdict, deque
from pathlib import Path
from typing import Deque, Dict

from lnhistoryclient.constants import GOSSIP_TYPE_NAMES

from config import BATCH_SIZE, LOG_INTERVAL, MAX_TIMESTAMP_HISTORY, STATISTICS_FILE_NAME, THROUGHPUT_WINDOW_SEC


class StatisticsTracker:
    """Tracks and persists statistics about message processing."""

    def __init__(self, logger: logging.Logger, batch_size: int = BATCH_SIZE):
        self.logger = logger
        self.batch_size = batch_size
        self.batch_counter = 0
        self.total_counter = 0
        self.batch_id = 0
        self.processed_timestamps: Deque[float] = deque(maxlen=MAX_TIMESTAMP_HISTORY)
        self.msg_type_count: Dict[int, int] = defaultdict(int)
        self.msg_type_duration: Dict[int, float] = defaultdict(float)
        self.batch_start_time = time.time()

    def record_message(self, msg_type: int, duration: float) -> None:
        """Record a processed message and its metrics."""
        self.msg_type_count[msg_type] += 1
        self.msg_type_duration[msg_type] += duration
        self.processed_timestamps.append(time.time())

        self.batch_counter += 1
        self.total_counter += 1

        self._check_periodic_logging()
        self._check_batch_completion()

    def _check_periodic_logging(self) -> None:
        """Log periodic throughput statistics."""
        if self.total_counter % LOG_INTERVAL == 0:
            recent_tps = self._compute_recent_throughput()
            self.logger.info(f"Handled {self.total_counter} messages.")
            self.logger.info(f"Recent throughput: {recent_tps:.2f} msgs/sec (last hour)")

    def _check_batch_completion(self) -> None:
        """Check if a batch is complete and handle statistics."""
        if self.batch_counter >= self.batch_size:
            self.batch_id += 1
            batch_elapsed = time.time() - self.batch_start_time

            self._handle_batch_statistics(batch_elapsed)

            # Reset batch counters
            self.batch_counter = 0
            self.batch_start_time = time.time()
            self.msg_type_count.clear()
            self.msg_type_duration.clear()

    def _compute_recent_throughput(self, window_sec: int = THROUGHPUT_WINDOW_SEC) -> float:
        """Compute throughput over the recent time window."""
        now = time.time()
        cutoff = now - window_sec
        recent = [ts for ts in self.processed_timestamps if ts >= cutoff]
        return len(recent) / window_sec if recent else 0

    def _handle_batch_statistics(self, batch_elapsed: float) -> None:
        """Process and persist statistics for the completed batch."""
        stats_summary = {
            "batch_id": self.batch_id,
            "batch_duration_sec": round(batch_elapsed, 4),
            "messages_per_second": round(self.batch_size / batch_elapsed, 2) if batch_elapsed > 0 else 0,
            "types": {},
        }

        self.logger.info(
            f"\n=== [Last {self.batch_size} messages - {batch_elapsed:.2f}s total, "
            f"{stats_summary['messages_per_second']:.2f} msg/sec] ==="
        )

        for msg_type, count in sorted(self.msg_type_count.items()):
            duration = self.msg_type_duration.get(msg_type, 0.0)
            avg = duration / count if count > 0 else 0.0

            name = GOSSIP_TYPE_NAMES.get(msg_type, f"{msg_type}")
            self.logger.info(f"• {name:<20} | Count: {count:>3} | Total Time: {duration:.3f}s | Avg: {avg:.4f}s")

            stats_summary["types"][name] = {
                "count": count,
                "total_duration_sec": round(duration, 4),
                "average_duration_sec": round(avg, 6),
            }

        self._persist_statistics(stats_summary)

    def _persist_statistics(self, stats_summary: Dict) -> None:
        """Save statistics to a persistent file."""
        stats_path = Path(STATISTICS_FILE_NAME)
        try:
            all_stats = []
            if stats_path.exists():
                with open(stats_path, "r", encoding="utf-8") as f:
                    all_stats = json.load(f)

            all_stats.append(stats_summary)

            with open(stats_path, "w", encoding="utf-8") as f:
                json.dump(all_stats, f, indent=2)

        except Exception as e:
            self.logger.error(f"Failed to persist statistics to {STATISTICS_FILE_NAME}: {e}")

"""
Sync Rate Limiter
Tracks sync timestamps in DynamoDB to enforce a per-window rate limit.

Rules:
- MAX_SYNCS (default 3) syncs are allowed within a rolling window.
- Each sync that is older than COOLDOWN_HOURS (default 3) hours "expires" and frees a slot.
- If all slots are used, the user must wait for the oldest slot to expire.
"""

import datetime
import os
import boto3
from dotenv import load_dotenv

# Load env for AWS credentials
ENV_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))), 'global.env')
load_dotenv(ENV_PATH, override=True)

MAX_SYNCS = 3
COOLDOWN_HOURS = 3


class SyncTracker:
    def __init__(self):
        self.dynamodb = boto3.resource(
            'dynamodb',
            region_name=os.getenv("AWS_REGION", "us-east-1"),
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY")
        )
        self.table_name = "SyncTracking"
        self.table = None
        self._init_table()

    def _init_table(self):
        """Create the SyncTracking table if it doesn't exist."""
        try:
            existing = [t.name for t in self.dynamodb.tables.all()]
            if self.table_name not in existing:
                print(f"Creating {self.table_name} table...")
                self.dynamodb.create_table(
                    TableName=self.table_name,
                    KeySchema=[
                        {'AttributeName': 'tracker_id', 'KeyType': 'HASH'},
                    ],
                    AttributeDefinitions=[
                        {'AttributeName': 'tracker_id', 'AttributeType': 'S'},
                    ],
                    ProvisionedThroughput={'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5}
                )
                waiter = self.dynamodb.meta.client.get_waiter('table_exists')
                waiter.wait(TableName=self.table_name)
                print(f"✅ {self.table_name} table created.")
            self.table = self.dynamodb.Table(self.table_name)
        except Exception as e:
            print(f"Error initializing {self.table_name} table: {e}")

    def _get_tracker(self) -> dict:
        """Read the current tracker item from DynamoDB."""
        try:
            response = self.table.get_item(Key={'tracker_id': 'global'})
            return response.get('Item', {'tracker_id': 'global', 'sync_timestamps': []})
        except Exception as e:
            print(f"Error reading sync tracker: {e}")
            return {'tracker_id': 'global', 'sync_timestamps': []}

    def _save_tracker(self, tracker: dict):
        """Persist the tracker item to DynamoDB."""
        try:
            self.table.put_item(Item=tracker)
        except Exception as e:
            print(f"Error saving sync tracker: {e}")

    def _get_active_timestamps(self, timestamps: list) -> list:
        """Filter out timestamps older than the cooldown window."""
        now = datetime.datetime.utcnow()
        cooldown = datetime.timedelta(hours=COOLDOWN_HOURS)
        return [ts for ts in timestamps if now - datetime.datetime.fromisoformat(ts) < cooldown]

    def get_status(self) -> dict:
        """
        Returns the current sync status for the frontend.
        """
        tracker = self._get_tracker()
        raw_timestamps = tracker.get('sync_timestamps', [])
        active = self._get_active_timestamps(raw_timestamps)

        # Persist cleanup if timestamps expired
        if len(active) != len(raw_timestamps):
            tracker['sync_timestamps'] = active
            self._save_tracker(tracker)

        syncs_used = len(active)
        syncs_remaining = max(0, MAX_SYNCS - syncs_used)
        can_sync = syncs_remaining > 0

        # Calculate cooldown info
        next_free_at = None
        cooldown_seconds_remaining = 0
        if not can_sync and active:
            oldest = min(datetime.datetime.fromisoformat(ts) for ts in active)
            free_at = oldest + datetime.timedelta(hours=COOLDOWN_HOURS)
            next_free_at = free_at.isoformat()
            diff = free_at - datetime.datetime.utcnow()
            cooldown_seconds_remaining = max(0, int(diff.total_seconds()))

        return {
            "syncs_used": syncs_used,
            "syncs_remaining": syncs_remaining,
            "max_syncs": MAX_SYNCS,
            "can_sync": can_sync,
            "cooldown_hours": COOLDOWN_HOURS,
            "next_free_at": next_free_at,
            "cooldown_seconds_remaining": cooldown_seconds_remaining,
        }

    def record_sync(self):
        """Record a successful sync timestamp."""
        tracker = self._get_tracker()
        raw_timestamps = tracker.get('sync_timestamps', [])
        active = self._get_active_timestamps(raw_timestamps)
        active.append(datetime.datetime.utcnow().isoformat())
        tracker['sync_timestamps'] = active
        self._save_tracker(tracker)
        print(f"✅ Sync recorded. {len(active)}/{MAX_SYNCS} syncs used in current window.")

    def can_sync(self) -> bool:
        """Quick check if a sync is currently allowed."""
        return self.get_status()["can_sync"]

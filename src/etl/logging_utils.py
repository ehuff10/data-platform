import json
from datetime import datetime, timezone
from typing import Any, Dict


def log_event(event: str, **fields: Dict[str, Any]) -> None:
    payload = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "event": event,
        **fields,
    }
    print(json.dumps(payload, default=str))

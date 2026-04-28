import json
import urllib.error
import urllib.request
from datetime import datetime
from typing import Any, Dict

from .disk_scanner import ScanResult
from .utils import format_size


def send_msteams_notification(webhook_url: str, scan_result: ScanResult, config: Dict[str, Any]) -> None:
    """
    Sends a disk usage summary notification to a Microsoft Teams webhook.
    """
    if not webhook_url:
        return

    print("\nSending notification to MS Teams workflow...", flush=True)

    try:
        now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        directory = config.get("directory", "Unknown")

        general = scan_result.general_system
        total_space = format_size(general.get("total", 0))
        used_space = format_size(general.get("used", 0))
        avail_space = format_size(general.get("available", 0))

        # Sort users by 'used' descending
        top_users = sorted(scan_result.user_usage, key=lambda x: x.get("used", 0), reverse=True)[:10]
        top_others = sorted(scan_result.other_usage, key=lambda x: x.get("used", 0), reverse=True)[:10]

        user_lines = [f"- **{u.get('name', 'Unknown')}**: {format_size(u.get('used', 0))}" for u in top_users]
        if not user_lines:
            user_lines = ["- No data"]

        other_lines = [f"- **{u.get('name', 'Unknown')}**: {format_size(u.get('used', 0))}" for u in top_others]
        if not other_lines:
            other_lines = ["- No data"]

        user_list_md = "\n".join(user_lines)
        other_list_md = "\n".join(other_lines)

        # Build Adaptive Card (v1.4)
        payload = {
            "type": "message",
            "attachments": [
                {
                    "contentType": "application/vnd.microsoft.card.adaptive",
                    "contentUrl": None,
                    "content": {
                        "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                        "type": "AdaptiveCard",
                        "version": "1.4",
                        "body": [
                            {
                                "type": "TextBlock",
                                "size": "Large",
                                "weight": "Bolder",
                                "text": "📊 Disk Usage Scan Completed",
                                "wrap": True
                            },
                            {
                                "type": "FactSet",
                                "facts": [
                                    {"title": "Time:", "value": now_str},
                                    {"title": "Directory:", "value": directory},
                                    {"title": "Total Space:", "value": total_space},
                                    {"title": "Used Space:", "value": used_space},
                                    {"title": "Available:", "value": avail_space}
                                ]
                            },
                            {
                                "type": "TextBlock",
                                "text": "🏆 **Top 10 Users:**",
                                "wrap": True,
                                "spacing": "Medium"
                            },
                            {
                                "type": "TextBlock",
                                "text": user_list_md,
                                "wrap": True
                            },
                            {
                                "type": "TextBlock",
                                "text": "🌍 **Top 10 Other Users:**",
                                "wrap": True,
                                "spacing": "Medium"
                            },
                            {
                                "type": "TextBlock",
                                "text": other_list_md,
                                "wrap": True
                            }
                        ]
                    }
                }
            ]
        }

        data = json.dumps(payload).encode('utf-8')
        req = urllib.request.Request(webhook_url, data=data, headers={
            'Content-Type': 'application/json'
        }, method='POST')

        with urllib.request.urlopen(req, timeout=30) as response:
            if response.status in [200, 202]:
                print("Successfully sent MS Teams notification.")
            else:
                print(f"MS Teams returned unusual status: {response.status}")

    except Exception as e:
        print(f"Failed to send MS Teams notification: {e}")

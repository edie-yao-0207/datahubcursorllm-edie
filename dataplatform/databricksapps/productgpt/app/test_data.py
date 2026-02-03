"""
Test data for local development.
Used when IS_DEV=true to avoid connecting to real database.
"""

import json
from datetime import datetime, timedelta

# Mock chat history data
MOCK_CHAT_HISTORY = [
    {
        "id": "exchange_001",
        "conversation_id": "conv_001",
        "request_date": (datetime.now() - timedelta(hours=2)).date(),
        "request": json.dumps(
            [{"role": "user", "content": "How many devices are in Fleet A?"}]
        ),
        "response": json.dumps(
            [
                {
                    "role": "assistant",
                    "content": "Based on the data, Fleet A has 42 active devices. These include 30 vehicles with telematics devices and 12 standalone tracking devices.",
                }
            ]
        ),
        "request_time": datetime.now() - timedelta(hours=2),
        "response_time": datetime.now() - timedelta(hours=2, minutes=-1),
        "request_id": "req_001",
        "user_email": "test.user@example.com",
        "logging_errors": None,
    },
    {
        "id": "exchange_002",
        "conversation_id": "conv_001",
        "request_date": (datetime.now() - timedelta(hours=1)).date(),
        "request": json.dumps([{"role": "user", "content": "What about Fleet B?"}]),
        "response": json.dumps(
            [
                {
                    "role": "assistant",
                    "content": "Fleet B has 58 devices total. This includes 45 vehicle telematics devices and 13 asset trackers.",
                }
            ]
        ),
        "request_time": datetime.now() - timedelta(hours=1),
        "response_time": datetime.now() - timedelta(hours=1, minutes=-1),
        "request_id": "req_002",
        "user_email": "test.user@example.com",
        "logging_errors": None,
    },
    {
        "id": "exchange_003",
        "conversation_id": "conv_002",
        "request_date": (datetime.now() - timedelta(days=1)).date(),
        "request": json.dumps(
            [
                {
                    "role": "user",
                    "content": "Show me the top 10 organizations by vehicle count",
                }
            ]
        ),
        "response": json.dumps(
            [
                {
                    "role": "assistant",
                    "content": "Here are the top 10 organizations by vehicle count:\n1. Acme Corp - 523 vehicles\n2. Global Logistics - 487 vehicles\n3. FastTransit - 412 vehicles\n4. Metro Delivery - 389 vehicles\n5. Prime Movers - 356 vehicles\n6. Swift Transport - 334 vehicles\n7. Reliable Freight - 298 vehicles\n8. Express Cargo - 276 vehicles\n9. City Services - 245 vehicles\n10. Regional Haulage - 223 vehicles",
                }
            ]
        ),
        "request_time": datetime.now() - timedelta(days=1),
        "response_time": datetime.now() - timedelta(days=1, minutes=-2),
        "request_id": "req_003",
        "user_email": "test.user@example.com",
        "logging_errors": None,
    },
    {
        "id": "exchange_004",
        "conversation_id": "conv_003",
        "request_date": (datetime.now() - timedelta(days=2)).date(),
        "request": json.dumps(
            [
                {
                    "role": "user",
                    "content": "What's the average daily usage across all devices?",
                }
            ]
        ),
        "response": json.dumps(
            [
                {
                    "role": "assistant",
                    "content": "The average daily usage across all devices is 8.4 hours. This includes both active driving time and idle time while the engine is running.",
                }
            ]
        ),
        "request_time": datetime.now() - timedelta(days=2),
        "response_time": datetime.now() - timedelta(days=2, minutes=-1),
        "request_id": "req_004",
        "user_email": "test.user@example.com",
        "logging_errors": None,
    },
    {
        "id": "exchange_005",
        "conversation_id": "conv_004",
        "request_date": (datetime.now() - timedelta(days=3)).date(),
        "request": json.dumps(
            [{"role": "user", "content": "List vehicles due for maintenance this week"}]
        ),
        "response": json.dumps(
            [
                {
                    "role": "assistant",
                    "content": "23 vehicles are due for maintenance this week:\n- Fleet A: 8 vehicles\n- Fleet B: 7 vehicles\n- Fleet C: 5 vehicles\n- Fleet D: 3 vehicles\n\nMost common maintenance needed: Oil change (15), Tire rotation (8), Brake inspection (12)",
                }
            ]
        ),
        "request_time": datetime.now() - timedelta(days=3),
        "response_time": datetime.now() - timedelta(days=3, minutes=-3),
        "request_id": "req_005",
        "user_email": "test.user@example.com",
        "logging_errors": None,
    },
    {
        "id": "exchange_006",
        "conversation_id": "conv_005",
        "request_date": (datetime.now() - timedelta(days=5)).date(),
        "request": json.dumps(
            [
                {
                    "role": "user",
                    "content": "Generate a performance report for last month",
                }
            ]
        ),
        "response": json.dumps(
            [
                {
                    "role": "assistant",
                    "content": "Performance Report - Last Month:\n\nTotal Miles Driven: 456,789 miles\nTotal Hours: 18,234 hours\nAverage MPG: 12.3\nTotal Fuel Cost: $89,234\n\nTop Performing Drivers:\n1. John Smith - 98% safety score\n2. Sarah Johnson - 97% safety score\n3. Mike Williams - 96% safety score",
                }
            ]
        ),
        "request_time": datetime.now() - timedelta(days=5),
        "response_time": datetime.now() - timedelta(days=5, minutes=-4),
        "request_id": "req_006",
        "user_email": "test.user@example.com",
        "logging_errors": None,
    },
    {
        "id": "exchange_007",
        "conversation_id": "conv_006",
        "request_date": (datetime.now() - timedelta(days=7)).date(),
        "request": json.dumps(
            [{"role": "user", "content": "Show me device utilization trends"}]
        ),
        "response": json.dumps(
            [
                {
                    "role": "assistant",
                    "content": "Device Utilization Trends (Last 30 Days):\n\nWeek 1: 87% average utilization\nWeek 2: 89% average utilization\nWeek 3: 91% average utilization\nWeek 4: 88% average utilization\n\nPeak utilization: Monday-Wednesday\nLowest utilization: Saturday-Sunday",
                }
            ]
        ),
        "request_time": datetime.now() - timedelta(days=7),
        "response_time": datetime.now() - timedelta(days=7, minutes=-2),
        "request_id": "req_007",
        "user_email": "another.user@example.com",
        "logging_errors": None,
    },
    {
        "id": "exchange_008",
        "conversation_id": "conv_007",
        "request_date": (datetime.now() - timedelta(hours=3)).date(),
        "request": json.dumps(
            [{"role": "user", "content": "What are the safety alerts for today?"}]
        ),
        "response": json.dumps(
            [
                {
                    "role": "assistant",
                    "content": "Today's Safety Alerts:\n\n🔴 Critical (3):\n- Harsh braking event - Vehicle #123\n- Speed violation - Vehicle #456\n- Following distance warning - Vehicle #789\n\n⚠️ Warnings (8):\n- Late vehicle check-in (5 vehicles)\n- Low fuel alerts (3 vehicles)",
                }
            ]
        ),
        "request_time": datetime.now() - timedelta(hours=3),
        "response_time": datetime.now() - timedelta(hours=3, minutes=-1),
        "request_id": "req_008",
        "user_email": "test.user@example.com",
        "logging_errors": None,
    },
]


def get_mock_conversations(limit=50, days_back=30):
    """
    Get mock conversations for local development.

    Args:
        user_email: Filter by user email (None for all users)
        limit: Maximum number of conversations to return
        days_back: Number of days to look back

    Returns:
        list[dict]: List of conversation summaries
    """
    # Group by conversation_id and get latest exchange
    conversations_dict = {}

    for exchange in MOCK_CHAT_HISTORY:
        # Filter by date
        days_ago = (datetime.now().date() - exchange["request_date"]).days
        if days_ago > days_back:
            continue

        conversation_id = exchange["conversation_id"]
        exchange_time = (
            exchange["request_time"]
            if isinstance(exchange["request_time"], datetime)
            else datetime.fromisoformat(exchange["request_time"])
        )

        # Keep the most recent exchange per conversation
        if conversation_id not in conversations_dict:
            conversations_dict[conversation_id] = {
                "conversation_id": conversation_id,
                "request": exchange["request"],
                "request_time": exchange_time,
                "_request_time_dt": exchange_time,
            }
        else:
            # Compare and keep the most recent exchange
            existing_time = conversations_dict[conversation_id]["_request_time_dt"]
            if exchange_time > existing_time:
                conversations_dict[conversation_id] = {
                    "conversation_id": conversation_id,
                    "request": exchange["request"],
                    "request_time": exchange_time,
                    "_request_time_dt": exchange_time,
                }

    # Convert to list, remove internal datetime field, and sort by request_time (most recent first)
    conversations = []
    for conv in conversations_dict.values():
        # Remove the internal datetime field before returning
        conv_clean = {
            "conversation_id": conv["conversation_id"],
            "request": conv["request"],
            "request_time": conv["request_time"],
        }
        conversations.append(conv_clean)

    conversations.sort(key=lambda x: x["request_time"], reverse=True)

    return conversations[:limit]


# ============================================================================
# Mock Database Functions
# ============================================================================


def get_mock_conversation_by_id(conversation_id):
    """
    Get mock conversation with all exchanges.

    Args:
        conversation_id: The conversation identifier

    Returns:
        dict or None: Conversation data with exchanges
    """
    # Find all exchanges for this conversation
    exchanges = [
        ex for ex in MOCK_CHAT_HISTORY if ex["conversation_id"] == conversation_id
    ]

    if not exchanges:
        return None

    # Sort by request_time
    exchanges.sort(key=lambda x: x["request_time"])

    return {
        "conversation_id": conversation_id,
        "request": exchanges[0]["request"][:100],
        "exchanges": exchanges,
    }


def mock_save_chat_exchange(*args, **kwargs):
    """
    Mock save function for dev mode - doesn't actually save anything.

    Returns:
        str: Fake exchange ID
    """
    import uuid

    print(
        f"[DEV MODE] Mock save_chat_exchange called with args: {args}, kwargs: {kwargs}"
    )
    return str(uuid.uuid4())

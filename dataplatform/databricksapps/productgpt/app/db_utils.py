import os
import uuid

import psycopg
import streamlit as st
from databricks.sdk import WorkspaceClient
from psycopg_pool import ConnectionPool

# Check if running in dev mode
IS_DEV = os.getenv("IS_DEV", "false").lower() == "true"

# Import test data for dev mode
if IS_DEV:
    from test_data import (
        get_mock_conversation_by_id,
        get_mock_conversations,
        mock_save_chat_exchange,
    )

    print("[DEV MODE] Using mock data - no database connections will be made")


class LakebaseConnection(psycopg.Connection):
    # Class variables for dependency injection
    _workspace_client = None
    _instance_name = None

    def __init__(self, *args, **kwargs):
        # Call the parent class constructor
        super().__init__(*args, **kwargs)

    @classmethod
    def connect(cls, conninfo="", **kwargs):
        # Get instance_name from class variable or environment
        import os

        instance_name = cls._instance_name or os.getenv(
            "DB_INSTANCE_NAME", "dataproductstest"
        )

        # Get workspace client from class variable or create new one
        client = cls._workspace_client or WorkspaceClient()

        # Generate database credential
        cred = client.database.generate_database_credential(
            request_id=str(uuid.uuid4()), instance_names=[instance_name]
        )

        kwargs["password"] = cred.token

        # Call the superclass's connect method with updated kwargs
        return super().connect(conninfo, **kwargs)


class DatabaseManager:
    """
    Singleton database connection manager.
    Manages database connection pool and configuration.
    Only one instance can exist to ensure connection pool is shared.
    """

    _instance = None
    _workspace_client = WorkspaceClient()
    _connection_pool = None

    def __new__(cls):
        """
        Ensure only one instance of DatabaseManager exists (Singleton pattern).
        """
        if cls._instance is None:
            cls._instance = super(DatabaseManager, cls).__new__(cls)
        return cls._instance

    @classmethod
    def get_config(cls):
        """
        Get database configuration from environment variables or defaults.

        Returns:
            dict: Database configuration parameters
        """

        return {
            "instance_name": os.getenv("DB_INSTANCE_NAME", "dataproductstest"),
            "database": os.getenv("DB_NAME", "databricks_postgres"),
            "port": int(os.getenv("DB_PORT", "5432")),
        }

    @classmethod
    def get_pool(cls, force_recreate=False):
        """
        Get or create the database connection pool.

        Args:
            force_recreate: If True, recreate the pool even if it exists

        Returns:
            ConnectionPool: Database connection pool
        """
        if cls._connection_pool is None or force_recreate:
            config = cls.get_config()

            # Configure the connection class with workspace client and instance name
            LakebaseConnection._workspace_client = cls._workspace_client
            LakebaseConnection._instance_name = config["instance_name"]

            # Get database instance information from Databricks
            instance = cls._workspace_client.database.get_database_instance(
                name=config["instance_name"]
            )
            host = instance.read_write_dns
            client_id = cls._workspace_client.config.client_id

            # Create connection pool with custom connection class
            cls._connection_pool = ConnectionPool(
                conninfo=f"dbname={config['database']} user={client_id} host={host}",
                connection_class=LakebaseConnection,
                min_size=1,
                max_size=10,
                max_lifetime=3000,  # Close connections after 50 minutes (3000 seconds)
                open=True,
            )

        return cls._connection_pool

    @classmethod
    def close_pool(cls):
        """
        Close the connection pool and clean up resources.
        """
        if cls._connection_pool is not None:
            cls._connection_pool.close()
            cls._connection_pool = None


# ============================================================================
# Chat History Functions
# ============================================================================


def fetch_conversations(user_email: str, limit: int = 50, days_back: int = 7):
    """
    Fetch recent conversations from the chat_history table.
    Groups by conversation_id and shows the most recent exchange.

    Args:
        user_email: Optional user email to filter by (default: None for all users)
        limit: Maximum number of conversations to return (default: 50)
        days_back: Number of days to look back (default: 7)

    Returns:
        list[dict]: List of conversation dictionaries with keys:
            - conversation_id: unique identifier
            - request: most recent request as title
            - request_time: most recent request_time
    """
    # Use mock data in dev mode
    if IS_DEV:
        return get_mock_conversations(limit=limit, days_back=days_back)

    try:
        pool = DatabaseManager.get_pool()
        with pool.connection() as conn:
            with conn.cursor() as cursor:
                query = """
                    WITH recent_conversations AS (
                        SELECT DISTINCT ON (conversation_id)
                            conversation_id,
                            request,
                            request_time
                        FROM public.chat_history
                        WHERE user_email = %s
                        AND request_date >= CURRENT_DATE - %s * INTERVAL '1 day'
                        ORDER BY conversation_id, request_time DESC
                    )
                    SELECT conversation_id,
                        request,
                        request_time
                    FROM recent_conversations
                    ORDER BY request_time DESC
                    LIMIT %s
                """
                params = [user_email, days_back, limit]

                cursor.execute(query, tuple(params))

                columns = [desc[0] for desc in cursor.description]
                results = []
                for row in cursor.fetchall():
                    results.append(dict(zip(columns, row)))
                return results
    except Exception as e:
        st.error(f"❌ Failed to fetch conversations. {e}. Please try again.")
        return []


def fetch_conversation_by_id(conversation_id: str):
    """
    Fetch all request-response exchanges for a conversation.

    Args:
        conversation_id: The conversation identifier

    Returns:
        dict or None: Conversation data with exchanges, or None if not found
    """
    # Use mock data in dev mode
    if IS_DEV:
        return get_mock_conversation_by_id(conversation_id)

    try:
        pool = DatabaseManager.get_pool()
        with pool.connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT
                        conversation_id,
                        request,
                        response
                    FROM public.chat_history
                    WHERE conversation_id = %s
                    ORDER BY request_time ASC
                    """,
                    (conversation_id,),
                )

                rows = cursor.fetchall()
                if not rows:
                    return None

                columns = [desc[0] for desc in cursor.description]
                exchanges = []
                for row in rows:
                    exchanges.append(dict(zip(columns, row)))

                # Build conversation structure
                first_exchange = exchanges[0]
                return {
                    "conversation_id": conversation_id,
                    "request": first_exchange["request"][:100],
                    "exchanges": exchanges,
                }

    except Exception as e:
        st.error(
            f"❌ Failed to fetch conversation {conversation_id}: {e}. Please try again."
        )
        return None


def save_chat_exchange(
    id: str,
    conversation_id: str,
    user_email: str = None,
    request_date: str = None,
    request: str = None,
    request_time: str = None,
    request_id: str = None,
    response: str = None,
    response_time: str = None,
    logging_errors: str = None,
):
    """
    Save a request-response exchange to the chat_history table.

    Args:
        conversation_id: The conversation this exchange belongs to
        request: User's request text
        response: Assistant's response text
        user_email: User's email identifier
        request_id: Optional request ID from the model
        logging_errors: Optional error information

    Returns:
        str or None: The exchange ID if successful, None otherwise
    """
    # Use mock save in dev mode - no actual writes
    if IS_DEV:
        return mock_save_chat_exchange(
            id=id,
            conversation_id=conversation_id,
            user_email=user_email,
            request_date=request_date,
            request=request,
            request_time=request_time,
            request_id=request_id,
            response=response,
            response_time=response_time,
            logging_errors=logging_errors,
        )

    try:
        pool = DatabaseManager.get_pool()
        with pool.connection() as conn:
            with conn.cursor() as cursor:

                cursor.execute(
                    """
                    INSERT INTO public.chat_history (
                        id,
                        conversation_id,
                        request_date,
                        request,
                        response,
                        request_time,
                        response_time,
                        request_id,
                        user_email,
                        logging_errors
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (id)
                    DO UPDATE SET
                        request_id = COALESCE(EXCLUDED.request_id, chat_history.request_id),
                        response_time = COALESCE(EXCLUDED.response_time, chat_history.response_time),
                        response = COALESCE(EXCLUDED.response, chat_history.response),
                        logging_errors = COALESCE(EXCLUDED.logging_errors, chat_history.logging_errors)
                    """,
                    (
                        id,
                        conversation_id,
                        request_date,
                        request,
                        response,
                        request_time,
                        response_time,
                        request_id,
                        user_email,
                        logging_errors,
                    ),
                )
                conn.commit()
                return id
    except Exception as e:
        raise e

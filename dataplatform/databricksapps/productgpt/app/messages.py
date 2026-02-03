from __future__ import annotations

"""
Message classes for the chatbot application.

This module contains the message classes used throughout the app.
By keeping them in a separate module, they remain stable across
Streamlit app reruns, avoiding isinstance comparison issues.
"""

import base64
import io
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional

import streamlit as st

# -----------------------------
# Helpers
# -----------------------------


def _render_image_payload(payload: Any, caption: Optional[str] = None) -> bool:
    """
    Render images coming from tools or assistant content.

    Accepts:
      - data URL string: "data:image/png;base64,...."
      - dict: {"mime": "image/png", "b64": "...", "caption": "..."}
    Returns True if rendered; False otherwise.
    """
    try:
        # data URL string
        if isinstance(payload, str) and payload.startswith("data:image/"):
            head, b64 = payload.split(",", 1)
            st.image(
                io.BytesIO(base64.b64decode(b64)),
                caption=caption,
                use_container_width=True,
            )
            return True

        # structured dict
        if (
            isinstance(payload, dict)
            and payload.get("mime", "").startswith("image/")
            and "b64" in payload
        ):
            st.image(
                io.BytesIO(base64.b64decode(payload["b64"])),
                caption=caption or payload.get("caption"),
                use_container_width=True,
            )
            return True
    except Exception as e:
        st.warning(f"Failed to render image payload: {e}")
    return False


# -----------------------------
# Base Message
# -----------------------------


class Message(ABC):
    def __init__(self) -> None:
        super().__init__()

    @abstractmethod
    def to_input_messages(self) -> List[Dict[str, Any]]:
        """Return this message as a list of OpenAI-style chat messages."""
        raise NotImplementedError

    @abstractmethod
    def render(self, idx: int) -> None:
        """Render the message in Streamlit."""
        raise NotImplementedError


# -----------------------------
# User Message
# -----------------------------


class UserMessage(Message):
    def __init__(self, content: str) -> None:
        super().__init__()
        self.content = content

    def to_input_messages(self) -> List[Dict[str, Any]]:
        return [{"role": "user", "content": self.content}]

    def render(self, idx: int = None) -> None:
        with st.chat_message("user"):
            st.markdown(self.content)


# -----------------------------
# Assistant Response
# -----------------------------


class AssistantResponse(Message):
    """
    A flexible assistant message that can contain plain text AND/OR
    a tool output payload (e.g., an image produced by a tool).
    """

    def __init__(
        self,
        content: str = "",
        request_id: Optional[str] = None,
        tool_output: Any = None,
        title: Optional[str] = None,
        messages: Optional[List[Dict[str, Any]]] = None,
    ) -> None:
        """
        Args:
            content: Plain text content for the assistant.
            request_id: Optional request id used for feedback API.
            tool_output: Optional tool payload (e.g., base64 image string or dict).
            title: Optional caption/title (used for images).
            messages: Optional list of raw message dicts (role/content/etc).
                      If provided, `to_input_messages` will return this list.
        """
        super().__init__()
        self._messages = messages  # raw dict messages (optional)
        self.content = content
        self.request_id = request_id
        self.tool_output = tool_output
        self.title = title

    # --- API expected by app.py ---
    def to_input_messages(self) -> List[Dict[str, Any]]:
        # If an explicit list of messages was provided (e.g., streamed tokens),
        # pass those through unchanged; else return a single assistant message.
        if isinstance(self._messages, list) and self._messages:
            return self._messages
        return [{"role": "assistant", "content": self.content}]

    def render(self, idx: int) -> None:
        with st.chat_message("assistant"):
            # 1) If we have a tool_output (e.g., image), try to render it first.
            if self.tool_output is not None:
                if _render_image_payload(self.tool_output, caption=self.title):
                    return

            # 2) If the assistant content itself is an image data URL, render it as image.
            if isinstance(self.content, str) and self.content.startswith("data:image/"):
                if _render_image_payload(self.content, caption=self.title):
                    return

            # 3) Otherwise render as text/markdown (if any)
            if isinstance(self.content, str) and self.content.strip():
                st.markdown(self.content)

            # 4) If raw dict messages were provided, group into thinking + final answer
            if isinstance(self._messages, list) and self._messages:
                _render_messages_grouped(self._messages)

            if self.request_id is not None:
                render_assistant_message_feedback(idx, self.request_id)


# -----------------------------
# Generic renderer for dict-style messages
# -----------------------------


def render_message_content(
    msg: Dict[str, Any], create_chat_message: bool = True
) -> None:
    """
    Render a single raw message dict (role/content/tool_calls/etc).
    This is used when an AssistantResponse carries raw message dicts
    from a streaming endpoint OR when the app directly loops through
    message dicts.

    Args:
        msg: The message dict to render
        create_chat_message: Whether to create a new chat message context
    """
    role = msg.get("role")

    if role == "user":
        if create_chat_message:
            with st.chat_message("user"):
                _render_message_content_internal(msg)
        else:
            _render_message_content_internal(msg)

    elif role == "assistant":
        if create_chat_message:
            with st.chat_message("assistant"):
                _render_message_content_internal(msg)
        else:
            _render_message_content_internal(msg)

    elif role == "tool":
        # Tool responses can also be images; detect and render.
        if create_chat_message:
            with st.chat_message("assistant"):
                _render_tool_content(msg)
        else:
            _render_tool_content(msg)

    else:
        # Unknown role – render safely
        st.write(msg)


def _render_message_content_internal(msg: Dict[str, Any]) -> None:
    """Internal function to render message content without chat message context."""
    # Content could be a data URL string for an image
    content = msg.get("content")
    if isinstance(content, str) and content.startswith("data:image/"):
        if _render_image_payload(content, caption=msg.get("title")):
            return

    # Normal text
    if isinstance(content, str) and content.strip():
        st.markdown(content)
    elif content:
        st.write(content)

    # If tool calls are present, show what is being called
    if "tool_calls" in msg and msg["tool_calls"]:
        for call in msg["tool_calls"]:
            fn_name = call.get("function", {}).get("name", "<unknown>")
            args = call.get("function", {}).get("arguments", "{}")
            st.markdown(f"🛠️ Calling **`{fn_name}`** with:\n```json\n{args}\n```")


def _render_tool_content(msg: Dict[str, Any]) -> None:
    """Internal function to render tool content without chat message context."""
    content = msg.get("content")
    if isinstance(content, str) and content.startswith("data:image/"):
        if _render_image_payload(content, caption="Tool output"):
            return

    # Use expander for collapsible tool output
    with st.expander("🧰 Tool Response (click to expand)", expanded=False):
        # Many tool payloads are JSON/text; show them raw if not an image.
        if isinstance(content, (dict, list)):
            import json

            st.code(json.dumps(content, indent=2), language="json")
        else:
            st.code(str(content), language="json")


def render_message(msg: Dict[str, Any]) -> None:
    """
    Render a single raw message dict (role/content/tool_calls/etc).
    This is used when an AssistantResponse carries raw message dicts
    from a streaming endpoint OR when the app directly loops through
    message dicts.
    """
    render_message_content(msg, create_chat_message=True)


def _render_messages_grouped(messages: List[Dict[str, Any]]) -> None:
    """
    Render messages with thinking text visible and tool calls in collapsible sections.

    - Tool calls + responses are collapsed in expanders
    - Final answer is shown prominently

    This is used during streaming and for rendering message history.
    """
    import json

    if not messages:
        return

    # Build a map of tool_call_id -> tool response for pairing
    tool_responses = {}
    for msg in messages:
        if msg.get("role") == "tool":
            tool_call_id = msg.get("tool_call_id")
            if tool_call_id:
                tool_responses[tool_call_id] = msg.get("content", "")

    # Track which tool calls we've rendered (to avoid duplicates)
    rendered_tool_calls = set()

    # Render messages in order
    for i, msg in enumerate(messages):
        role = msg.get("role")
        content = msg.get("content", "")

        if role == "assistant":
            # Render the thinking/explanation text if present
            if content and content.strip():
                st.markdown(content)

            # Render each tool call in a collapsed expander
            if msg.get("tool_calls"):
                for tool_call in msg["tool_calls"]:
                    call_id = tool_call.get("id")
                    if call_id in rendered_tool_calls:
                        continue
                    rendered_tool_calls.add(call_id)

                    fn_name = tool_call.get("function", {}).get("name", "tool")
                    fn_args = tool_call.get("function", {}).get("arguments", "{}")

                    # Get the corresponding tool response
                    tool_response = tool_responses.get(call_id, "")

                    # Always render the expander - show arguments immediately, response when ready
                    with st.expander(f"🔧 {fn_name} (click to expand)", expanded=False):
                        # Show the arguments
                        st.markdown("**Arguments:**")
                        try:
                            args_parsed = (
                                json.loads(fn_args)
                                if isinstance(fn_args, str)
                                else fn_args
                            )
                            st.code(json.dumps(args_parsed, indent=2), language="json")
                        except (json.JSONDecodeError, TypeError):
                            st.code(str(fn_args), language="json")

                        # Show the response or a waiting indicator
                        if tool_response:
                            st.markdown("**Response:**")
                            if isinstance(tool_response, str):
                                if tool_response.startswith("data:image/"):
                                    # Render the actual image instead of placeholder text
                                    _render_image_payload(
                                        tool_response, caption="Tool output"
                                    )
                                elif len(tool_response) > 1000:
                                    st.code(
                                        tool_response[:1000] + "\n... (truncated)",
                                        language="text",
                                    )
                                else:
                                    st.code(tool_response, language="text")
                            else:
                                resp_str = json.dumps(tool_response, indent=2)
                                if len(resp_str) > 1000:
                                    st.code(
                                        resp_str[:1000] + "\n... (truncated)",
                                        language="json",
                                    )
                                else:
                                    st.code(resp_str, language="json")

        # Skip tool messages - they're rendered with their corresponding tool calls above
        elif role == "tool":
            continue


# -----------------------------
# Feedback UI for assistant messages
# -----------------------------


def render_assistant_message_feedback(i: int, request_id: Optional[str]) -> None:
    """Render feedback UI for assistant messages."""
    import os

    from model_serving_utils import submit_feedback

    def save_feedback(index: int) -> None:
        serving_endpoint = os.getenv("SERVING_ENDPOINT")
        if serving_endpoint:
            submit_feedback(
                endpoint=serving_endpoint,
                request_id=request_id,
                rating=st.session_state.get(f"feedback_{index}"),
            )

    st.feedback("thumbs", key=f"feedback_{i}", on_change=save_feedback, args=[i])

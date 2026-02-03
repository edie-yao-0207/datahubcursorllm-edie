import json
import logging
import os
import uuid
from collections import OrderedDict
from datetime import datetime

import streamlit as st
from db_utils import fetch_conversation_by_id, fetch_conversations, save_chat_exchange
from messages import (
    AssistantResponse,
    UserMessage,
    _render_messages_grouped,
    render_assistant_message_feedback,
    render_message_content,
)
from model_serving_utils import (
    _get_endpoint_task_type,
    endpoint_supports_feedback,
    query_endpoint,
    query_endpoint_stream,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def render_chat_history_sidebar():
    """
    Render the chat history sidebar with clickable conversation elements.
    """
    with st.sidebar:

        with open("public/samsara-logo.svg", "r") as f:
            logo_svg = f.read()
        st.markdown(
            f'<div class="app-header">{logo_svg}<span class="app-header-title">Samsara ProductGPT</span></div>',
            unsafe_allow_html=True,
        )

        # Chat History Header
        st.markdown(
            """<div class="SidebarChatHeader">
                 <h3>Chat</h3>
                 </div>""",
            unsafe_allow_html=True,
        )

        # New Chat button at the top
        st.button(
            "New chat",
            key="new_chat_button",
            use_container_width=True,
            icon=":material/edit:",
            on_click=new_chat_callback,
        )

        # Chat History Section
        st.markdown(
            """<div class="SidebarRecentChatsHeader">
                 <p>RECENT</p>
                 </div>""",
            unsafe_allow_html=True,
        )
        # Sidebar Footer
        st.markdown(
            """<div class="SidebarFooter">
                 <p>Conversations from last 7 days</p>
                 </div>""",
            unsafe_allow_html=True,
        )

        conversations = st.session_state.conversation_history
        if not conversations:
            st.markdown(
                """<div class="SidebarNoChatHistoryText">
                 <p>No previous conversations found.</p>
                 </div>""",
                unsafe_allow_html=True,
            )
        else:
            for idx, conv in enumerate(conversations):
                request = json.loads(conv["request"])
                title = request[0]["content"]
                if len(title) > 50:
                    title = f"{title[:50].rsplit(' ', 1)[0]}..."
                st.button(
                    title,
                    key=f"conversation_button_{idx}",
                    use_container_width=True,
                    help=f"{request[0]['content']} - {conv['request_time'].isoformat(sep=' ', timespec='minutes')}",
                    on_click=load_conversation_callback,
                    args=(conv["conversation_id"],),
                )


def new_chat_callback():
    """
    Callback function to start a new conversation by clearing chat history.
    """
    st.session_state.history = []
    st.session_state.selected_conversation_id = str(uuid.uuid4())

    user_email = st.context.headers.get("X-Forwarded-Email")
    conversations = fetch_conversations(user_email)
    st.session_state.conversation_history = conversations


def load_conversation_callback(conversation_id: str):
    """
    Callback function to load a specific conversation.

    Args:
        conversation_id: The ID of the conversation to load
    """

    conversation = fetch_conversation_by_id(conversation_id)
    if not isinstance(conversation, dict):
        st.error(
            f"❌ Failed to load conversation {conversation_id}. It may have been deleted."
        )
        return None

    all_messages = []
    for exchange in conversation.get("exchanges", []):
        request = json.loads(exchange["request"])[0]["content"]
        response = json.loads(exchange.get("response") or "[]")
        all_messages.append(UserMessage(content=request))
        all_messages.append(AssistantResponse(messages=response))

    st.session_state.selected_conversation_id = conversation_id
    st.session_state.history = all_messages
    return None


# Configure Streamlit page settings
st.set_page_config(
    page_title="ProductGPT",
    page_icon="public/samsara-logo.svg",
)
st.markdown(
    """
<style>
    .block-container {
        padding-top: 3rem !important;
        max-width: 1200px !important;
        margin-left: 0 !important;
    }


    .stHorizontalBlock {
        gap: 0 !important;
    }

    /* Header styling */
    .app-header {
        display: flex;
        align-items: center;
        gap: 8px;
    }
    .app-header svg {
        width: 25px;
        height: auto;
        color: #D9DFE9;
    }
    .app-header-title {
        color: white !important;
        font-weight: 600;
        font-size: 20px;
    }

    /* Apply InterVariable font to all text elements */
    .stMarkdown, .stMarkdown p, .stMarkdown h1, .stMarkdown h2, .stMarkdown h3,
    .stText, .element-container, [data-testid="stText"],
    [data-testid="stMarkdownContainer"] {
        font-family: "InterVariable", sans-serif !important;
    }

    [data-testid="stSidebarHeader"] {
        padding: 0rem !important;
    }

    [data-testid="stDecoration"] {
        background-image: none !important;
        background-color: #091929 !important;
    }

    /* Fixed position spinner that stays visible during scroll */
    .fixed-spinner {
        position: fixed;
        bottom: 20px;
        left: calc(50% + 10.5rem);
        transform: translateX(-50%);
        background: rgba(255, 255, 255, 0.95);
        border: 1px solid #e0e0e0;
        border-radius: 8px;
        padding: 8px 16px;
        box-shadow: 0 2px 8px rgba(0, 0, 0, 0.1);
        z-index: 1000;
        font-family: "InterVariable", sans-serif;
        font-size: 14px;
        color: #666;
        display: none;
    }

    .fixed-spinner.show {
        display: block;
    }

    .spinner-icon {
        display: inline-block;
        margin-right: 8px;
        animation: spin 1s linear infinite;
    }

    @keyframes spin {
        0% { transform: rotate(0deg); }
        100% { transform: rotate(360deg); }
    }

    /* Header logo size */
    .header-logo svg {
        width: 50px;
        height: 50px;
    }


    /* Sidebar styling for chat history */
    .stSidebar {
        background-color: #091929;
        width: 21rem !important;
    }

    /* Sidebar block container - same approach as main .block-container */
    .stSidebar .block-container {
        padding-top: 3rem !important;
    }

    .st-key-new_chat_button .stButton button {
        background-color: rgba(0, 38, 62, 0.65) !important;
        color: white !important;
        justify-content: center !important;
        align-items: center !important;
        border-color: transparent !important;
    }

    .st-key-new_chat_button .stButton button p {
        padding-top: 4px !important;
    }

    .st-key-new_chat_button .stButton button:hover {
        background-color: #192F46 !important;
        color: white !important;
        border-color: transparent !important;
    }

    /* Remove highlight/focus state from New Chat button after click */
    .st-key-new_chat_button .stButton button:active,
    .st-key-new_chat_button .stButton button:focus-visible {
        background-color: #14283C !important;
        color: white !important;
        border-color: transparent !important;
        box-shadow: none !important;
        outline: none !important;
    }

    /* Conversation history buttons - left aligned with text wrapping */
    [class*="st-key-conversation_button_"] button {
        background-color: transparent !important;
        color: #d7dfea !important;
        justify-content: flex-start !important;
        text-align: left !important;
        border-color: transparent !important;
        height: auto !important;
        min-height: 38px !important;
        padding-left: 4px !important;
    }

    [class*="st-key-conversation_button_"] button p {
        text-align: left !important;
        white-space: normal !important;
        word-wrap: break-word !important;
        line-height: 1.4 !important;
        font-size: 14px !important;
    }

    [class*="st-key-conversation_button_"] button:hover {
        background-color: #192F46 !important;
        color: white !important;
        border-color: transparent !important;
    }

    /* Remove highlight/focus state from conversation buttons after click */
    [class*="st-key-conversation_button_"] button:active,
    [class*="st-key-conversation_button_"] button:focus-visible {
        background-color: #14283C !important;
        color: white !important;
        border-color: transparent !important;
        box-shadow: none !important;
        outline: none !important;
    }

    .SidebarChatHeader {
        margin-top: -2rem !important;
    }

    /* Centered content section spacing */
    .content-section {
        margin-top: 5rem;
    }

    .feedback-list {
        margin-top: 4px;
        margin-bottom: 0;
        padding-left: 2rem;
    }

    .input-spacer {
        height: 2rem;
    }

    .SidebarChatHeader h3 {
        color: white !important;
        font-size: 20px !important;
        font-weight: 600 !important;
        margin-bottom: 16px !important;
        margin-top: 5rem !important;
    }

    .SidebarNoChatHistoryText p {
        color: #B0B0B0 !important;
        font-size: 12px !important;
        font-style: italic !important;
        margin-top: 5px !important;
        margin-bottom: 8px !important;
        padding-left: 4px !important;
    }

    .SidebarRecentChatsHeader p {
        color: white !important;
        font-size: 12px !important;
        font-weight: 600 !important;
        margin-top: 20px !important;
        margin-bottom: 8px !important;
        padding-left: 4px !important;
    }

    .SidebarFooter {
        position: fixed;
        bottom: 0;
        left: 0;
        width: 21rem;
        padding: 16px 12px;
        background-color: #061318 !important;
        border-top: 1px solid #061318 !important;
        z-index: 1000 !important;
    }

    .SidebarFooter p {
        color: #e6ecf4 !important;
        font-size: 11px !important;
        text-align: center !important;
        margin: 0 !important;
    }

    /* Example queries pill styling */
    .example-queries-container {
        margin-top: 1rem;
        margin-bottom: 1rem;
    }

    .example-queries-label {
        font-size: 14px;
        font-weight: 600;
        color: #666;
        margin-bottom: 12px;
    }

    .example-pills {
        display: flex;
        flex-wrap: wrap;
        gap: 8px;
    }

    .example-pill {
        background-color: #f0f2f5;
        border: 1px solid #d0d4da;
        border-radius: 20px;
        padding: 8px 16px;
        font-size: 14px;
        color: #333;
        cursor: pointer;
        transition: all 0.2s ease;
        white-space: nowrap;
    }

    .example-pill:hover {
        background-color: #e0e4ea;
        border-color: #b0b8c2;
        transform: translateY(-1px);
        box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1);
    }

    .example-pill:active {
        transform: translateY(0);
    }

    /* Example query buttons styling */
    [class*="st-key-example_query_"] button {
        text-align: left !important;
        justify-content: flex-start !important;
        border-radius: 20px !important;
        color: #666 !important;
    }

    [class*="st-key-example_query_"] button p {
        text-align: left !important;
        font-style: italic !important;
        color: #666 !important;
    }

    [class*="st-key-example_query_"] button:hover {
        background-color: #e8eaed !important;
        border-color: #d0d4da !important;
    }

    [class*="st-key-example_query_"] button:hover p {
        color: #333 !important;
    }

    /* Remove highlight/focus state from example query buttons after click */
    [class*="st-key-example_query_"] button:active,
    [class*="st-key-example_query_"] button:focus-visible {
        background-color: #dfe1e5 !important;
        border-color: transparent !important;
        color: #666 !important;
        box-shadow: none !important;
        outline: none !important;
    }

    [class*="st-key-example_query_"] button:focus:not(:active) {
        border-color: #d0d4da !important;
    }

</style>
""",
    unsafe_allow_html=True,
)

# Create fixed spinner container at top level
fixed_spinner_container = st.empty()

IS_DEBUG = os.getenv("IS_DEBUG", "false").lower() == "true"
IS_DEV = os.getenv("IS_DEV", "false").lower() == "true"

SERVING_ENDPOINT = os.getenv("SERVING_ENDPOINT")
assert SERVING_ENDPOINT, (
    "Unable to determine serving endpoint to use for chatbot app. If developing locally, "
    "set the SERVING_ENDPOINT environment variable to the name of your serving endpoint. If "
    "deploying to a Databricks app, include a serving endpoint resource named "
    "'serving_endpoint' with CAN_QUERY permissions, as described in "
    "https://docs.databricks.com/aws/en/generative-ai/agent-framework/chat-app#deploy-the-databricks-app"
)

if not IS_DEV:
    ENDPOINT_SUPPORTS_FEEDBACK = endpoint_supports_feedback(SERVING_ENDPOINT)
else:
    ENDPOINT_SUPPORTS_FEEDBACK = False


def reduce_chat_agent_chunks(chunks):
    """
    Reduce a list of ChatAgentChunk objects corresponding to a particular
    message into a single ChatAgentMessage
    """
    deltas = [chunk.delta for chunk in chunks]
    first_delta = deltas[0]
    result_msg = first_delta
    msg_contents = []

    # Accumulate tool calls properly
    tool_call_map = {}  # Map call_id to tool call for accumulation

    for delta in deltas:
        # Handle content
        if delta.content:
            msg_contents.append(delta.content)

        # Handle tool calls
        if hasattr(delta, "tool_calls") and delta.tool_calls:
            for tool_call in delta.tool_calls:
                call_id = getattr(tool_call, "id", None)
                tool_type = getattr(tool_call, "type", "function")
                function_info = getattr(tool_call, "function", None)
                if function_info:
                    func_name = getattr(function_info, "name", "")
                    func_args = getattr(function_info, "arguments", "")
                else:
                    func_name = ""
                    func_args = ""

                if call_id:
                    if call_id not in tool_call_map:
                        # New tool call
                        tool_call_map[call_id] = {
                            "id": call_id,
                            "type": tool_type,
                            "function": {"name": func_name, "arguments": func_args},
                        }
                    else:
                        # Accumulate arguments for existing tool call
                        existing_args = tool_call_map[call_id]["function"]["arguments"]
                        tool_call_map[call_id]["function"]["arguments"] = (
                            existing_args + func_args
                        )

                        # Update function name if provided
                        if func_name:
                            tool_call_map[call_id]["function"]["name"] = func_name

        # Handle tool call IDs (for tool response messages)
        if hasattr(delta, "tool_call_id") and delta.tool_call_id:
            result_msg = result_msg.model_copy(
                update={"tool_call_id": delta.tool_call_id}
            )

    # Convert tool call map back to list
    if tool_call_map:
        accumulated_tool_calls = list(tool_call_map.values())
        result_msg = result_msg.model_copy(
            update={"tool_calls": accumulated_tool_calls}
        )

    result_msg = result_msg.model_copy(update={"content": "".join(msg_contents)})
    return result_msg


def query_endpoint_and_render(task_type, input_messages):
    """Handle streaming response based on task type."""
    if task_type == "agent/v1/responses":
        return query_responses_endpoint_and_render(input_messages)
    elif task_type == "agent/v2/chat":
        return query_chat_agent_endpoint_and_render(input_messages)
    else:  # chat/completions
        return query_chat_completions_endpoint_and_render(input_messages)


def query_chat_completions_endpoint_and_render(input_messages):
    """Handle ChatCompletions streaming format."""
    with st.chat_message("assistant"):
        response_area = st.empty()
        response_area.markdown("_Thinking..._")
        accumulated_content = ""
        request_id = None
        try:
            for chunk in query_endpoint_stream(
                endpoint_name=SERVING_ENDPOINT,
                messages=input_messages,
                return_traces=IS_DEBUG,
            ):
                if "choices" in chunk and chunk["choices"]:
                    delta = chunk["choices"][0].get("delta", {})
                    content = delta.get("content", "")
                    if content:
                        accumulated_content += content
                        response_area.markdown(accumulated_content)

                if "databricks_output" in chunk:
                    req_id = chunk["databricks_output"].get("databricks_request_id")
                    if req_id:
                        request_id = req_id

            return AssistantResponse(
                messages=[{"role": "assistant", "content": accumulated_content}],
                request_id=request_id,
            )
        except Exception:
            response_area.markdown("_Ran into an error. Retrying without streaming..._")
            messages, request_id = query_endpoint(
                endpoint_name=SERVING_ENDPOINT,
                messages=input_messages,
                return_traces=IS_DEBUG,
            )
            response_area.empty()
            with response_area.container():
                for message in messages:
                    render_message_content(message, create_chat_message=False)
            return AssistantResponse(messages=messages, request_id=request_id)


def query_chat_agent_endpoint_and_render(input_messages):
    """Handle ChatAgent streaming format."""
    from mlflow.types.agent import ChatAgentChunk

    with st.chat_message("assistant"):
        response_area = st.empty()
        response_area.markdown("_Thinking..._")

        message_buffers = OrderedDict()
        request_id = None

        try:
            for raw_chunk in query_endpoint_stream(
                endpoint_name=SERVING_ENDPOINT,
                messages=input_messages,
                return_traces=IS_DEBUG,
            ):
                response_area.empty()
                chunk = ChatAgentChunk.model_validate(raw_chunk)
                delta = chunk.delta
                message_id = delta.id

                req_id = raw_chunk.get("databricks_output", {}).get(
                    "databricks_request_id"
                )
                if req_id:
                    request_id = req_id
                if message_id not in message_buffers:
                    message_buffers[message_id] = {
                        "chunks": [],
                        "render_area": st.empty(),
                    }
                message_buffers[message_id]["chunks"].append(chunk)

                partial_message = reduce_chat_agent_chunks(
                    message_buffers[message_id]["chunks"]
                )
                render_area = message_buffers[message_id]["render_area"]
                message_content = partial_message.model_dump_compat(exclude_none=True)
                with render_area.container():
                    render_message_content(message_content, create_chat_message=False)

            messages = []
            for msg_id, msg_info in message_buffers.items():
                messages.append(reduce_chat_agent_chunks(msg_info["chunks"]))

            return AssistantResponse(
                messages=[
                    message.model_dump_compat(exclude_none=True) for message in messages
                ],
                request_id=request_id,
            )
        except Exception:
            response_area.markdown("_Ran into an error. Retrying without streaming..._")
            messages, request_id = query_endpoint(
                endpoint_name=SERVING_ENDPOINT,
                messages=input_messages,
                return_traces=IS_DEBUG,
            )
            response_area.empty()
            with response_area.container():
                for message in messages:
                    render_message_content(message, create_chat_message=False)
            return AssistantResponse(messages=messages, request_id=request_id)


def query_responses_endpoint_and_render(input_messages):
    """Handle ResponsesAgent streaming format using MLflow types."""
    from mlflow.types.responses import ResponsesAgentStreamEvent

    with st.chat_message("assistant"):
        response_area = st.empty()
        response_area.markdown("_Thinking..._")

        # Track all the messages that need to be rendered in order
        all_messages = []
        message_buffer = ""
        is_streaming_deltas = False  # Track if we're currently receiving a delta stream
        request_id = None
        seen_tool_call_ids = set()  # Track tool calls to avoid duplicates
        seen_tool_response_ids = set()  # Track tool responses to avoid duplicates

        try:
            for raw_event in query_endpoint_stream(
                endpoint_name=SERVING_ENDPOINT,
                messages=input_messages,
                return_traces=IS_DEBUG,
            ):
                # Extract databricks_output for request_id
                if "databricks_output" in raw_event:
                    req_id = raw_event["databricks_output"].get("databricks_request_id")
                    if req_id:
                        request_id = req_id

                # Parse using MLflow streaming event types, similar to ChatAgentChunk
                if "type" in raw_event:
                    event = ResponsesAgentStreamEvent.model_validate(raw_event)

                    # Check if this event has delta content
                    if hasattr(event, "delta") and event.delta:
                        # We're receiving delta content - accumulate it
                        message_buffer += event.delta
                        is_streaming_deltas = True

                    else:
                        # No delta in this event - check if we were previously streaming deltas
                        if is_streaming_deltas and message_buffer.strip():
                            # Previous delta stream has ended, add the accumulated content
                            all_messages.append(
                                {"role": "assistant", "content": message_buffer}
                            )
                            # Reset buffer and flag
                            message_buffer = ""
                            is_streaming_deltas = False

                    # Handle complete items (final messages)
                    if hasattr(event, "item") and event.item:
                        item = event.item  # This is a dict, not a parsed object

                        if item.get("type") == "function_call":
                            # Tool call
                            call_id = item.get("call_id")
                            function_name = item.get("name")
                            arguments = item.get("arguments", "")

                            # Only add if we haven't seen this tool call before
                            if call_id and call_id not in seen_tool_call_ids:
                                seen_tool_call_ids.add(call_id)
                                all_messages.append(
                                    {
                                        "role": "assistant",
                                        "content": "",
                                        "tool_calls": [
                                            {
                                                "id": call_id,
                                                "type": "function",
                                                "function": {
                                                    "name": function_name,
                                                    "arguments": arguments,
                                                },
                                            }
                                        ],
                                    }
                                )

                        elif item.get("type") == "function_call_output":
                            # Tool call output/result
                            call_id = item.get("call_id")
                            output = item.get("output", "")

                            # Only add if we haven't seen this tool response before
                            if call_id and call_id not in seen_tool_response_ids:
                                seen_tool_response_ids.add(call_id)
                                all_messages.append(
                                    {
                                        "role": "tool",
                                        "content": output,
                                        "tool_call_id": call_id,
                                    }
                                )

                # Update the display by rendering all accumulated messages
                if all_messages:
                    response_area.empty()  # Clear previous render to avoid duplicates
                    with response_area.container():
                        _render_messages_grouped(all_messages)

            # Handle any remaining accumulated delta content at the end of stream
            if is_streaming_deltas and message_buffer.strip():
                all_messages.append({"role": "assistant", "content": message_buffer})
                # Re-render to include the final message
                response_area.empty()
                with response_area.container():
                    _render_messages_grouped(all_messages)

            # Render feedback UI at the end of streaming if we have a request_id
            if request_id and ENDPOINT_SUPPORTS_FEEDBACK:
                # Use the index that this message will have in history (current length)
                future_index = len(st.session_state.get("history", []))
                render_assistant_message_feedback(future_index, request_id)

            return AssistantResponse(messages=all_messages, request_id=request_id)
        except Exception:
            response_area.markdown("_Ran into an error. Retrying without streaming..._")
            messages, request_id = query_endpoint(
                endpoint_name=SERVING_ENDPOINT,
                messages=input_messages,
                return_traces=IS_DEBUG,
            )
            response_area.empty()
            with response_area.container():
                _render_messages_grouped(messages)
            return AssistantResponse(messages=messages, request_id=request_id)


def to_assistant_response(final_payload, request_id=None, title=None):
    """
    Convert your final agent/endpoint payload into an AssistantResponse
    that can render images via messages.py.
    """
    # Case 1: data URL string (image)
    if isinstance(final_payload, str) and final_payload.startswith("data:image/"):
        return AssistantResponse(
            content="",  # no plain text
            tool_output=final_payload,  # image goes here
            title=title or "Generated chart",
            request_id=request_id,
        )

    # Case 2: structured dict with image
    if (
        isinstance(final_payload, dict)
        and final_payload.get("mime", "").startswith("image/")
        and "b64" in final_payload
    ):
        return AssistantResponse(
            content="",
            tool_output=final_payload,
            title=title or "Generated chart",
            request_id=request_id,
        )

    # Case 3: plain text fallback
    return AssistantResponse(
        content=str(final_payload) if final_payload is not None else "",
        request_id=request_id,
    )


def get_chat_conversations():
    user_email = st.context.headers.get("X-Forwarded-Email")
    conversations = fetch_conversations(user_email)
    return conversations


# --- Init state ---
if "history" not in st.session_state:
    st.session_state.history = []
# if "chat_id" not in st.session_state:
#     st.session_state.chat_id = None
# if "selected_conversation_id" not in st.session_state:
#     st.session_state.selected_conversation_id = str(uuid.uuid4())
# if "conversation_history" not in st.session_state:
#     st.session_state.conversation_history = get_chat_conversations()
if "example_query_clicked" not in st.session_state:
    st.session_state.example_query_clicked = None


# --- Render chat history sidebar ---
# render_chat_history_sidebar()


# Description, feedback, and input - centered using columns
st.write("")  # spacing after header
left_col, center_col, right_col = st.columns([1.5, 7, 1.5])
with center_col:
    st.markdown(
        """<div class="content-section">
<p>Welcome to Samsara ProductGPT! Ask me for insights about devices, organizations, or operational metrics. For best results, be as specific as possible; otherwise, you may need to refine your questions a few times.</p>
<p>📝 <strong>Feedback:</strong> Have suggestions or found an issue? Share your feedback in the
<a href="https://samsara-rd.slack.com/archives/C08CR5JG4TX/p1739299012516309">#dev-data-products-feedback</a> Slack channel.</p>
</div>""",
        unsafe_allow_html=True,
    )

    # Example queries as clickable pills
    if not st.session_state.history:  # Only show when no conversation yet
        st.markdown(
            """
        <div class="example-queries-container">
            <div class="example-queries-label">Try asking</div>
        </div>
        """,
            unsafe_allow_html=True,
        )

        example_queries = [
            "Where can I find information about safety events?",
            "What kinds of devices do we have?",
            "which customers opened the Idling report the most in the last 90 days?",
            "Display monthly trends for paying customers over the last year",
        ]

        for idx, query in enumerate(example_queries):
            if st.button(
                query,
                key=f"example_query_{idx}",
                use_container_width=True,
                type="secondary",
            ):
                st.session_state.example_query_clicked = query

    st.markdown('<div class="input-spacer"></div>', unsafe_allow_html=True)


# --- Render chat history --
for i, element in enumerate(st.session_state.history):
    with st.container(border=True):
        element.render(i)

# Handle example query clicks
if st.session_state.example_query_clicked:
    prompt = st.session_state.example_query_clicked
    st.session_state.example_query_clicked = None  # Reset after using
else:
    prompt = st.chat_input("Ask a question")

if prompt:
    st.session_state.chat_id = str(uuid.uuid4())
    # Get the task type for this endpoint
    task_type = _get_endpoint_task_type(SERVING_ENDPOINT)

    # Add user message to chat history
    user_msg = UserMessage(content=prompt)
    user_msg.render()
    # try:
    #     save_chat_exchange(
    #         id=st.session_state.chat_id,
    #         conversation_id=st.session_state.selected_conversation_id,
    #         request=json.dumps(user_msg.to_input_messages()),
    #         request_time=datetime.now().isoformat(),
    #         request_date=datetime.now().date().isoformat(),
    #         user_email=st.context.headers.get("X-Forwarded-Email"),
    #     )
    # except Exception as e:
    #     logger.error(f"Error saving chat exchange: {e}", exc_info=True)
    #     st.error(
    #         f"❌ Failed to save your message to the database. {e}. Please try again."
    #     )
    #     st.stop()  # Stop Streamlit execution - don't send to agent if input cannot be saved

    st.session_state.history.append(user_msg)

    # Convert history to standard chat message format for the query methods
    # Keep only: user messages, assistant messages without tool_calls,
    # assistant messages with execute_query tool_calls, and tool responses to execute_query
    input_messages = []
    execute_query_call_ids = set()

    for elem in st.session_state.history:
        for msg in elem.to_input_messages():
            role = msg.get("role")

            if role == "user":
                # Keep all user messages
                input_messages.append(msg)
            elif role == "assistant":
                tool_calls = msg.get("tool_calls") or []
                if not tool_calls:
                    # Keep assistant messages without tool calls
                    input_messages.append(msg)
                else:
                    # Filter to only execute_query tool calls
                    execute_query_calls = [
                        tc
                        for tc in tool_calls
                        if tc.get("function", {}).get("name") == "execute_query"
                    ]
                    # Track the execute_query call IDs for later filtering of tool responses
                    for tc in execute_query_calls:
                        execute_query_call_ids.add(tc.get("id"))
                    # Create a filtered message with only execute_query tool calls
                    filtered_msg = msg.copy()
                    filtered_msg["tool_calls"] = execute_query_calls
                    input_messages.append(filtered_msg)
            elif role == "tool":
                # Keep tool messages that respond to execute_query calls
                tool_call_id = msg.get("tool_call_id")
                if tool_call_id in execute_query_call_ids:
                    input_messages.append(msg)

    # Show fixed spinner
    with fixed_spinner_container.container():
        st.markdown(
            """
        <div class="fixed-spinner show">
            <span class="spinner-icon">⏳</span>
            <span>Agent is thinking...</span>
        </div>
        """,
            unsafe_allow_html=True,
        )

    try:
        raw_result = query_endpoint_and_render(task_type, input_messages)
        assistant_response = raw_result
        # If your handler returns raw content instead of AssistantResponse, do:
        if not isinstance(raw_result, AssistantResponse):
            assistant_response = to_assistant_response(
                final_payload=raw_result,
                request_id=getattr(
                    raw_result, "request_id", None
                ),  # use whatever you track
                title="",
            )
        st.session_state.history.append(assistant_response)
        # save_chat_exchange(
        #     id=st.session_state.chat_id,
        #     conversation_id=st.session_state.selected_conversation_id,
        #     response=json.dumps(assistant_response._messages),
        #     response_time=datetime.now().isoformat(),
        #     request_id=assistant_response.request_id,
        # )
    except Exception as e:
        logger.error(f"Agent error: {e}", exc_info=True)
        st.error(f"Agent Error: {e}. \n\n Please try again.")
    finally:
        # Hide fixed spinner
        fixed_spinner_container.empty()

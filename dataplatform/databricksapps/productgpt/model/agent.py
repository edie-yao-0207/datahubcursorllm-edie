import json
from typing import Annotated, Any, Generator, Optional, Sequence, TypedDict, Union
from uuid import uuid4

import mlflow
from databricks_langchain import ChatDatabricks, UCFunctionToolkit
from langchain_core.language_models import LanguageModelLike
from langchain_core.messages import AIMessage, AIMessageChunk, BaseMessage
from langchain_core.runnables import RunnableConfig, RunnableLambda
from langchain_core.tools import BaseTool
from langgraph.graph import END, StateGraph
from langgraph.graph.message import add_messages
from langgraph.graph.state import CompiledStateGraph
from langgraph.prebuilt.tool_node import ToolNode
from mlflow.pyfunc import ResponsesAgent
from mlflow.types.responses import (
    ResponsesAgentRequest,
    ResponsesAgentResponse,
    ResponsesAgentStreamEvent,
)
from model.tools import (
    create_visualization,
    execute_query,
    get_table_metadata,
    get_tables,
)
from model.utils import read_system_prompt

############################################
# Define your LLM endpoint and system prompt
############################################

MAX_RECURSION_DEPTH = 25

LLM_ENDPOINT_NAME = "databricks-claude-sonnet-4"
llm = ChatDatabricks(endpoint=LLM_ENDPOINT_NAME)

system_prompt = read_system_prompt()

###############################################################################
## Define tools for your agent, enabling it to retrieve data or take actions
## beyond text generation
## To create and see usage examples of more tools, see
## https://docs.databricks.com/en/generative-ai/agent-framework/agent-tool.html
###############################################################################
tools = [get_table_metadata, get_tables, execute_query, create_visualization]

# Add Databricks Unity Catalog tools:
# You can use UDFs in Unity Catalog as agent tools
# Below, we add the `system.ai.python_exec` UDF, which provides
# a python code interpreter tool to our agent
# You can also add local LangChain python tools. See https://python.langchain.com/docs/concepts/tools
uc_tool_names = []
uc_toolkit = UCFunctionToolkit(function_names=uc_tool_names)
tools.extend(uc_toolkit.tools)


#####################
## Define agent logic
#####################
class AgentState(TypedDict):
    messages: Annotated[Sequence[BaseMessage], add_messages]
    custom_inputs: Optional[dict[str, Any]]
    custom_outputs: Optional[dict[str, Any]]
    recursion_depth: int
    recursion_limit_reached: bool


def create_tool_calling_agent(
    model: LanguageModelLike,
    tools: Union[ToolNode, Sequence[BaseTool]],
    system_prompt: Optional[str] = None,
):
    model = model.bind_tools(tools)

    # Define the function that determines which node to go to
    def should_continue(state: AgentState):
        # Check if we're approaching recursion limit (safety check)
        current_depth = state.get("recursion_depth", 0)
        recursion_limit_reached = state.get("recursion_limit_reached", False)

        messages = state["messages"]
        last_message = messages[-1]

        # If there are function calls, always continue to process them (even at max recursion)
        if isinstance(last_message, AIMessage) and last_message.tool_calls:
            return "continue"

        # If we've reached max recursion and haven't provided explanation yet, go back to agent
        if current_depth >= MAX_RECURSION_DEPTH and not recursion_limit_reached:
            return "continue"  # This will trigger the agent to provide explanation

        # Otherwise, end
        return "end"

    if system_prompt:
        preprocessor = RunnableLambda(
            lambda state: [{"role": "system", "content": system_prompt}]
            + state["messages"]
        )
    else:
        preprocessor = RunnableLambda(lambda state: state["messages"])
    model_runnable = preprocessor | model

    def call_model(
        state: AgentState,
        config: RunnableConfig,
    ):
        # Check if we've reached max recursion and need to provide explanation
        current_depth = state.get("recursion_depth", 0)
        recursion_limit_reached = state.get("recursion_limit_reached", False)

        if current_depth >= MAX_RECURSION_DEPTH and not recursion_limit_reached:
            # Create explanation message instead of calling the model

            explanation_message = AIMessage(
                content="I've reached my maximum number of reasoning steps for this query. "
                "Based on the analysis I've performed so far, I may not be able to "
                "provide a complete answer. **Please try rephrasing the question to be more specific or "
                "breaking it down into smaller, more specific parts.**"
            )
            return {
                "messages": [explanation_message],
                "recursion_depth": current_depth + 1,
                "recursion_limit_reached": True,
            }

        # Normal model invocation
        response = model_runnable.invoke(state, config)

        # Increment recursion depth to track agent iterations
        new_depth = current_depth + 1

        return {
            "messages": [response],
            "recursion_depth": new_depth,
            "recursion_limit_reached": recursion_limit_reached,
        }

    workflow = StateGraph(AgentState)

    workflow.add_node("agent", RunnableLambda(call_model))
    workflow.add_node("tools", ToolNode(tools))

    workflow.set_entry_point("agent")
    workflow.add_conditional_edges(
        "agent",
        should_continue,
        {
            "continue": "tools",
            "end": END,
        },
    )
    workflow.add_edge("tools", "agent")

    return workflow.compile()


class LangGraphResponsesAgent(ResponsesAgent):
    def __init__(self, agent: CompiledStateGraph):
        self.agent = agent

    def _responses_to_cc(self, message: dict[str, Any]) -> list[dict[str, Any]]:
        """Convert from a Responses API output item to ChatCompletion messages."""
        msg_type = message.get("type")
        if msg_type == "function_call":
            return [
                {
                    "role": "assistant",
                    "content": "tool call",
                    "tool_calls": [
                        {
                            "id": message.get("call_id"),
                            "type": "function",
                            "function": {
                                "arguments": message.get("arguments"),
                                "name": message.get("name"),
                            },
                        }
                    ],
                }
            ]
        elif msg_type == "message" and isinstance(message.get("content"), list):
            return [
                {"role": message.get("role"), "content": content.get("text")}
                for content in message.get("content", [])
            ]
        elif msg_type == "reasoning":
            return [
                {"role": "assistant", "content": json.dumps(message.get("summary"))}
            ]
        elif msg_type == "function_call_output":
            return [
                {
                    "role": "tool",
                    "content": message.get("output"),
                    "tool_call_id": message.get("call_id"),
                }
            ]
        compatible_keys = ["role", "content", "name", "tool_calls", "tool_call_id"]
        filtered = {k: v for k, v in message.items() if k in compatible_keys}
        return [filtered] if filtered else []

    def _prep_msgs_for_cc_llm(self, responses_input) -> list[dict[str, Any]]:
        "Convert from Responses input items to ChatCompletion dictionaries"
        cc_msgs = []
        for msg in responses_input:
            cc_msgs.extend(self._responses_to_cc(msg.model_dump()))

    def _langchain_to_responses(
        self, messages: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        "Convert from ChatCompletion dict to Responses output item dictionaries"
        import json

        for message in messages:
            message = message.model_dump()
            role = message["type"]
            if role == "ai":
                if tool_calls := message.get("tool_calls"):
                    return [
                        self.create_function_call_item(
                            id=message.get("id") or str(uuid4()),
                            call_id=tool_call["id"],
                            name=tool_call["name"],
                            arguments=json.dumps(tool_call["args"]),
                        )
                        for tool_call in tool_calls
                    ]
                else:
                    return [
                        self.create_text_output_item(
                            text=message["content"],
                            id=message.get("id") or str(uuid4()),
                        )
                    ]
            elif role == "tool":
                # Ensure output is always a string
                output = message["content"]
                if output is None:
                    output_str = "No result returned"
                elif isinstance(output, str):
                    output_str = output
                elif isinstance(output, (list, dict)):

                    try:
                        output_str = json.dumps(output, indent=2, default=str)
                    except Exception:
                        output_str = str(output)
                else:
                    # Handle DataFrames and other objects
                    try:
                        # Check if it's a DataFrame
                        if hasattr(output, "to_string"):
                            output_str = output.to_string()
                        elif hasattr(output, "to_dict"):
                            output_str = json.dumps(
                                output.to_dict(), indent=2, default=str
                            )
                        else:
                            output_str = str(output)
                    except Exception:
                        output_str = str(output)

                return [
                    self.create_function_call_output_item(
                        call_id=message["tool_call_id"],
                        output=output_str,
                    )
                ]
            elif role == "user":
                return [message]

    def predict(self, request: ResponsesAgentRequest) -> ResponsesAgentResponse:
        outputs = [
            event.item
            for event in self.predict_stream(request)
            if event.type == "response.output_item.done"
        ]
        return ResponsesAgentResponse(
            output=outputs, custom_outputs=request.custom_inputs
        )

    def predict_stream(
        self,
        request: ResponsesAgentRequest,
    ) -> Generator[ResponsesAgentStreamEvent, None, None]:
        cc_msgs = []
        for msg in request.input:
            cc_msgs.extend(self._responses_to_cc(msg.model_dump()))

        for event in self.agent.stream(
            {
                "messages": cc_msgs,
                "recursion_depth": 0,
                "recursion_limit_reached": False,
            },
            RunnableConfig(recursion_limit=MAX_RECURSION_DEPTH * 2),
            stream_mode=["updates", "messages"],
        ):
            if event[0] == "updates":
                for node_data in event[1].values():
                    for item in self._langchain_to_responses(node_data["messages"]):
                        yield ResponsesAgentStreamEvent(
                            type="response.output_item.done", item=item
                        )
            # filter the streamed messages to just the generated text messages
            elif event[0] == "messages":
                try:
                    chunk = event[1][0]
                    if isinstance(chunk, AIMessageChunk) and (content := chunk.content):
                        yield ResponsesAgentStreamEvent(
                            **self.create_text_delta(delta=content, item_id=chunk.id),
                        )
                except Exception as e:
                    print(e)


# Create the agent object, and specify it as the agent object to use when
# loading the agent back for inference via mlflow.models.set_model()
mlflow.langchain.autolog()
agent = create_tool_calling_agent(llm, tools, system_prompt)
AGENT = LangGraphResponsesAgent(agent)
mlflow.models.set_model(AGENT)

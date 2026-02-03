-- PostgreSQL Schema for ProductGPT Chat Application
-- Run this to create the necessary tables
-- Table currently exists in Databricks Lakebase 'dataproductstest' instance.

-- Chat history table
CREATE TABLE IF NOT EXISTS public.chat_history (
    id VARCHAR(255) PRIMARY KEY NOT NULL,
    conversation_id VARCHAR(255) NOT NULL,
    request_date DATE,
    request TEXT,
    response TEXT,
    request_time timestamp,
    response_time TIMESTAMP,
    request_id VARCHAR(255),
    user_email VARCHAR(255),
    logging_errors TEXT
);

-- Create indexes for faster queries
CREATE INDEX IF NOT EXISTS idx_chat_history_conversation_id
ON public.chat_history(conversation_id);

CREATE INDEX IF NOT EXISTS idx_chat_history_user_email
ON public.chat_history(user_email);

-- Grant permissions to databricks apps' service principals
GRANT SELECT ON public.chat_history TO "8c1df1be-ff3a-47d6-a92a-0d549c3ad603";
GRANT INSERT, UPDATE, DELETE ON public.chat_history TO "8c1df1be-ff3a-47d6-a92a-0d549c3ad603";

GRANT SELECT ON public.chat_history TO "d56cf94a-094f-4383-8679-a105c060042a";
GRANT INSERT, UPDATE, DELETE ON public.chat_history TO "d56cf94a-094f-4383-8679-a105c060042a";



-- Add comments for documentation
COMMENT ON TABLE chat_history IS 'Stores chat history metadata. Each row represents a single chat exchange between a user and the agent.';
COMMENT ON COLUMN chat_history.id IS 'Unique identifier for each chat exchange.';
COMMENT ON COLUMN chat_history.conversation_id IS 'Unique UUID for each conversation.';
COMMENT ON COLUMN chat_history.request_date IS 'Date of user request.';
COMMENT ON COLUMN chat_history.request IS 'User request represented as a JSON string. Contains an array of messages of user input(s).';
COMMENT ON COLUMN chat_history.response IS 'Response from the agent represented as a JSON string. Contains an array of messages from agent thought process, tool calls and final response.';
COMMENT ON COLUMN chat_history.request_time IS 'Timestamp of the user request.';
COMMENT ON COLUMN chat_history.response_time IS 'Timestamp of the agent response(s).';
COMMENT ON COLUMN chat_history.request_id IS 'Request Id from the logged model serving response in Databricks.';
COMMENT ON COLUMN chat_history.user_email IS 'User email of the user who made the request.';
COMMENT ON COLUMN chat_history.logging_errors IS 'Optional error information.';


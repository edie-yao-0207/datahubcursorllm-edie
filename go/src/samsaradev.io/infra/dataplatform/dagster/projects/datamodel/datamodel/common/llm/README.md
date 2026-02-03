# DataHub LLM Agent

1. Consume a prompt from Cursor composer agent
2. Process the datahub.json schema file and query agent rules (`.cursor/rules/query_agent.mdc`)
3. Generate SQL queries using MCP tools or direct SQL execution
4. Execute the queries against Databricks SQL

**Usage:**
- Use `@query_agent` in Cursor to include the query agent rules
- Rules are available at `.cursor/rules/query_agent.mdc` in the workspace
- The rules include instructions for using MCP tools (`get_tables`, `get_table_metadata`, `run_sql_query`, `get_lineage`)

see: https://samsara.atlassian-us-gov-mod.net/wiki/spaces/DATA/pages/17788555

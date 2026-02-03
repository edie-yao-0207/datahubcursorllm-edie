"""
Column Description Generator Tool

This module contains the prompt template for an AI agent to generate
high-quality column descriptions for data tables. The generated descriptions
are intended for data engineer review before being committed.

## Cursor Users

The recommended way to invoke this tool is via the Cursor rule:

    @column-descriptions Generate column descriptions for <TABLE_NAME>

This will load the full context from `.cursor/rules/column-descriptions.mdc`.

## Non-Cursor Users

Replace {TABLE_NAME} with the full table name (e.g., datamodel_core.dim_devices)
and use the PROMPT template below with your AI assistant.
"""

PROMPT = """
# Column Description Generation Task

## Objective
Generate comprehensive, accurate column descriptions for **{TABLE_NAME}**.

## Quality Standards

### DO:
- Describe the **semantic meaning** of the column (what it represents in the business domain)
- Include the **source** of data when known (e.g., "Sourced from clouddb organizations table")
- Note **foreign key relationships** (e.g., "Foreign key to dim_organizations.org_id")
- Specify **units** for numeric values (e.g., "Distance in meters", "Duration in milliseconds")
- Mention **valid value ranges** or **enum values** when applicable
- For boolean columns, explain what True/False means in context
- For timestamp columns, clarify the timezone and what event they represent

### DON'T:
- Create **tautological descriptions** (e.g., "org_id: The org ID" is BAD)
- Guess at meanings you're not confident about
- Use column name analysis as primary source - only as last resort
- Include descriptions for columns where meaning is genuinely unclear

## Research Process

### Step 1: Examine Table Metadata
Use `get_table_metadata` tool to get the current schema and existing descriptions.

### Step 2: Explore Lineage
Use `get_lineage` tool with both "upstream" and "downstream" directions to find:
- Source tables that may have existing descriptions
- Related tables that provide context about column meanings

### Step 3: Search Source Code
Look for this table in the codebase using datahub.json or source_code_map.json to find:
- SQL queries that create/populate the table
- Column transformations and business logic
- Comments explaining column meanings

### Step 4: Query Sample Data
Run a simple query to understand column values:
```sql
SELECT * FROM {TABLE_NAME} LIMIT 20
```
**AVOID**: GROUP BY or ORDER BY queries - they are too slow.

### Step 5: Research External Context
- Search Samsara KB at https://kb.samsara.com/hc/en-us for domain terminology
- Check RDS registry for source table definitions if this is derived from RDS

## Output Format

Provide descriptions as a Python dictionary mapping column names to descriptions:

```python
descriptions = {{
    "column_name": "Description with source noted. FK to other_table.column if applicable.",
    "another_column": "Clear description of semantic meaning.",
    # ... more columns
}}
```

**For nested/struct columns**, use dot notation for flattened field paths:
```python
descriptions = {{
    "nested_struct.field_name": "Description of the nested field",
    "nested_struct.inner.deep_field": "Description of deeply nested field",
}}
```

Add a brief comment for each description indicating the source of the information:
- `# From upstream table X`
- `# From source code/SQL definition`
- `# From sample data analysis`
- `# From Samsara KB`
- `# From RDS registry`

## Time Limit
Spend no more than **3 minutes** on this task.

## Final Step
When complete, run:
```bash
python update_datahub_descriptions.py {TABLE_NAME} '<descriptions_dict>'
```

Where `<descriptions_dict>` is the Python dictionary as a JSON-formatted string.

**Example:**
```bash
python update_datahub_descriptions.py datamodel_core.dim_devices '{{"device_id": "Unique identifier for the device", "org_id": "Foreign key to dim_organizations.org_id"}}'
```

## Error Handling
- If you encounter errors, report them instead of providing a summary
- Clean up any temporary files created during the process
- If a column's meaning is genuinely unclear after research, omit it from the output rather than guessing
"""

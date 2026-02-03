"""SQL parsing and utility functions for dashboard generation."""

import re
import hashlib
from typing import List


def generate_id(seed: str) -> str:
    """Generate 8-char hex ID from seed string."""
    return hashlib.md5(seed.encode()).hexdigest()[:8]


def parse_select_columns(sql: str) -> List[str]:
    """Extract column names from SQL SELECT clause."""
    columns = []

    # Skip CTEs: find the main query after WITH ... AS (...), ... AS (...)
    main_query = sql.strip()

    # Remove SQL comments first for easier parsing
    lines = []
    for line in main_query.split("\n"):
        # Remove -- comments
        comment_pos = line.find("--")
        if comment_pos >= 0:
            line = line[:comment_pos]
        lines.append(line)
    clean_sql = "\n".join(lines).strip()

    if clean_sql.upper().startswith("WITH"):
        # Track parentheses to find where CTEs end
        # CTE format: WITH name AS (...), name AS (...) SELECT ...
        depth = 0
        in_string = False
        string_char = None
        i = clean_sql.upper().find("WITH") + 4  # Start after 'WITH'

        while i < len(clean_sql):
            char = clean_sql[i]

            # Track string literals
            if char in ("'", '"') and (i == 0 or clean_sql[i - 1] != "\\"):
                if not in_string:
                    in_string = True
                    string_char = char
                elif char == string_char:
                    in_string = False
                    string_char = None
                i += 1
                continue

            if in_string:
                i += 1
                continue

            if char == "(":
                depth += 1
            elif char == ")":
                depth -= 1
            elif depth == 0:
                # Check if we've hit the main SELECT (not part of CTE definition)
                rest = clean_sql[i:].lstrip()
                if rest.upper().startswith("SELECT"):
                    main_query = rest
                    break
            i += 1
    else:
        main_query = clean_sql

    # For UNION queries, use the first SELECT's columns
    union_match = re.search(r"\bUNION\s+(ALL\s+)?SELECT\b", main_query, re.IGNORECASE)
    if union_match:
        main_query = main_query[: union_match.start()]

    # Find SELECT ... FROM
    select_match = re.search(r"\bSELECT\s+", main_query, re.IGNORECASE)
    if not select_match:
        return columns

    # Get text from SELECT to FROM (handle nested subqueries)
    start = select_match.end()
    depth = 0
    in_string = False
    string_char = None
    from_pos = None
    for i in range(start, len(main_query)):
        char = main_query[i]
        
        # Track string literals (single or double quotes)
        if char in ("'", '"') and (i == 0 or main_query[i - 1] != "\\"):
            if not in_string:
                in_string = True
                string_char = char
            elif char == string_char:
                in_string = False
                string_char = None
            continue
        
        if in_string:
            continue
            
        if char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
        elif depth == 0 and main_query[i : i + 4].upper() == "FROM":
            # Ensure FROM is a complete word (not part of identifier like from_stage)
            # Check character before FROM is not a word character
            prev_ok = i == 0 or not (main_query[i - 1].isalnum() or main_query[i - 1] == "_")
            # Check character after FROM is not a word character
            next_pos = i + 4
            next_ok = next_pos >= len(main_query) or not (main_query[next_pos].isalnum() or main_query[next_pos] == "_")
            if prev_ok and next_ok:
                from_pos = i
                break

    if from_pos is None:
        return columns

    select_clause = main_query[start:from_pos]

    # Parse column expressions (handle nested parens and string literals)
    depth = 0
    in_string = False
    string_char = None
    current = ""
    for i, char in enumerate(select_clause):
        # Track string literals (single or double quotes)
        if char in ("'", '"') and (i == 0 or select_clause[i - 1] != "\\"):
            if not in_string:
                in_string = True
                string_char = char
            elif char == string_char:
                in_string = False
                string_char = None
            current += char
        elif in_string:
            current += char
        elif char == "(":
            depth += 1
            current += char
        elif char == ")":
            depth -= 1
            current += char
        elif char == "," and depth == 0:
            columns.append(current.strip())
            current = ""
        else:
            current += char
    if current.strip():
        columns.append(current.strip())

    # Extract final column names (alias or last identifier)
    result = []
    for col in columns:
        col = col.strip()
        if not col:
            continue
        # Check for AS alias
        as_match = re.search(r'\bAS\s+[`"]?(\w+)[`"]?\s*$', col, re.IGNORECASE)
        if as_match:
            result.append(as_match.group(1))
        else:
            # Get last identifier (handle table.column syntax)
            ident_match = re.search(r'[`"]?(\w+)[`"]?\s*$', col)
            if ident_match:
                result.append(ident_match.group(1))

    return result


def parse_sql_description(sql: str) -> str:
    """Extract description from SQL comment header."""
    # Look for Description: line in comments
    match = re.search(
        r"--\s*Description:\s*(.+?)(?:\n--\s*[A-Z]|\n[^-]|\n--\s*=)", sql, re.DOTALL
    )
    if match:
        desc = match.group(1).strip()
        # Clean up multi-line descriptions
        lines = [line.strip().lstrip("- ").lstrip() for line in desc.split("\n")]
        return " ".join(lines).strip()
    return ""


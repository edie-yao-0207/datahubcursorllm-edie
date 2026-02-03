import json
import os
import re

def update_json_file(file_path):
    try:
        # Read the entire file content
        with open(file_path, 'r') as f:
            content = f.read()

        # Parse JSON to check if isAggregationOn exists
        data = json.loads(content)

        if 'isAggregationOn' in data:
            # If it exists, use regex to only replace its value
            pattern = r'("isAggregationOn"\s*:\s*)(true|false)'
            new_content = re.sub(pattern, r'\1false', content)
        else:
            # If it doesn't exist, insert it before the last }
            # Find position of last }
            last_brace_idx = content.rstrip().rfind('}')
            if last_brace_idx == -1:
                raise ValueError("No closing brace found")

            # Get everything before the last }
            prefix = content[:last_brace_idx].rstrip()
            # Add comma if needed
            if not prefix.endswith(','):
                prefix += ','

            # Get any whitespace before the closing brace
            suffix = content[last_brace_idx:]
            whitespace = re.match(r'\s*}', suffix).group(0)[:-1] if re.match(r'\s*}', suffix) else ''

            # Construct new content
            new_content = prefix + '\n  "isAggregationOn": false' + whitespace + '}'

            # Preserve final newline if it existed
            if content.endswith('\n'):
                new_content += '\n'

        # Write back only if content changed
        if new_content != content:
            with open(file_path, 'w') as f:
                f.write(new_content)
            print(f"Updated {file_path}")
        else:
            print(f"No changes needed for {file_path}")

    except Exception as e:
        print(f"Error processing {file_path}: {str(e)}")

# Process all JSON files in the current directory
for file_path in os.listdir('.'):
    if file_path.endswith('.json'):
        update_json_file(file_path)

print("Finished updating all JSON files")

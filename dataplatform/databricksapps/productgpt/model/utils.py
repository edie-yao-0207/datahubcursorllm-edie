import os


def read_system_prompt():
    # Get the directory of the current file
    current_dir = os.path.dirname(os.path.abspath(__file__))
    prompt_path = os.path.join(current_dir, "prompt.md")

    with open(prompt_path, "r") as file:
        prompt = file.read()

    return prompt

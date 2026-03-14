import json 

def metadata_table(path):
    try:
        with open(path, 'r') as file:
            data = json.load(file)
            return data
    except FileNotFoundError:
        print("Error: The file was not found.")
    except json.JSONDecodeError:
        print("Error: Failed to decode JSON from the file.")


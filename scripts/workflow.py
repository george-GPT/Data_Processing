# workflow.py
# <<Backup A_Collected folder before running scriptt>> 

import os
import subprocess
import time

# Define the path to the folder containing the scripts
scripts_folder = r"C:\Users\Georg\Projects\Data_Processing\scripts"

# List of scripts in the order they need to be run
scripts = [
    "1_duplicate_removal_fix_names.py",
    "2_clean_names_assign_id.py",
    "3_extract_text.py",
    "4_clean_text.py",
    "5_segment_and_chunk.py",
    "6_final_clean.py",
    "7_metadata_assignment.py",
    "8_chunk_to_Json.py"
]

def run_script(script_path):
    """
    Executes a Python script located at script_path.
    
    Args:
        script_path (str): The full path to the Python script to execute.
    
    Raises:
        subprocess.CalledProcessError: If the script execution fails.
    """
    try:
        # Run the script using subprocess and wait for it to complete
        result = subprocess.run(["python", script_path], check=True, capture_output=True, text=True)
        
        # Output the script's stdout and stderr for debugging purposes
        print(f"Output from {script_path}:\n{result.stdout}")
        if result.stderr:
            print(f"Errors from {script_path}:\n{result.stderr}")
        
        print(f"Successfully completed: {script_path}")
    except subprocess.CalledProcessError as e:
        print(f"Error while running {script_path}: {e}")
        print(f"Return Code: {e.returncode}")
        print(f"Output: {e.output}")
        print(f"Error Output: {e.stderr}")
        raise

if __name__ == "__main__":
    for script in scripts:
        script_path = os.path.join(scripts_folder, script)
        
        # Check if the script file exists before running it
        if os.path.exists(script_path):
            print(f"Running script: {script}")
            run_script(script_path)
            time.sleep(1)  # Add a small delay between scripts for safety
        else:
            print(f"Script not found: {script_path}")

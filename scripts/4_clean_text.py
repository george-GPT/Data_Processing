import os
import re
import logging
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

# Setup logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

# Paths for input and output
input_dir = Path(r"C:\Users\Georg\Projects\Data_Processing\data\B_Extracted")
output_dir = Path(r"C:\Users\Georg\Projects\Data_Processing\data\C_Cleaned")

# Ensure the output directory exists
output_dir.mkdir(parents=True, exist_ok=True)

def clean_text(text):
    """
    Clean the input text by removing any common OCR errors and general cleaning.
    """
    # Remove occurrences of 'NUL' (case-insensitive)
    cleaned_text = re.sub(r'\bNUL\b', '', text, flags=re.IGNORECASE)

    # Remove common OCR artifacts (e.g., '\x00', random strings of digits, etc.)
    cleaned_text = re.sub(r'\x00|\d{6,}', '', cleaned_text)
    
    # Remove extra whitespace, newlines, and tabs
    cleaned_text = re.sub(r'[\n\t]+', ' ', cleaned_text)
    cleaned_text = re.sub(r'\s{2,}', ' ', cleaned_text)
    
    # Normalize multiple dots (e.g., replace ".." or more with ".")
    cleaned_text = re.sub(r'\.{2,}', '.', cleaned_text)
    
    # Strip leading/trailing whitespace
    cleaned_text = cleaned_text.strip()
    
    return cleaned_text

def process_file(file_path, output_dir):
    """
    Process and clean a single text file.
    """
    try:
        with file_path.open('r', encoding='utf-8') as f:
            # Read the file content
            data = f.read()
            cleaned_data = clean_text(data)

        # Write the cleaned text to the output file
        output_path = output_dir / file_path.name
        with output_path.open('w', encoding='utf-8') as f:
            f.write(cleaned_data)

        logging.info(f"Processed and cleaned: {file_path.name}")
    
    except FileNotFoundError as e:
        logging.error(f"File not found: {file_path} - {e}")
    except Exception as e:
        logging.error(f"Error processing {file_path.name}: {e}")

def process_files(input_dir, output_dir, max_workers=None):
    """
    Process each file in the input directory, clean the text, and save it to the output directory.
    """
    logging.info("Starting to process files...")

    # Check if the input directory exists and is not empty
    if not input_dir.exists():
        logging.error(f"Input directory does not exist: {input_dir}")
        return

    # Find all text files (.txt)
    files = list(input_dir.glob('*.txt'))
    
    if not files:
        logging.warning(f"No files found in the input directory: {input_dir}")
        return

    # Determine the number of threads (default: os.cpu_count() if not provided)
    if max_workers is None:
        max_workers = os.cpu_count()

    # Using ThreadPoolExecutor for concurrent file processing
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(process_file, file, output_dir): file for file in files}

        for future in as_completed(futures):
            file = futures[future]
            try:
                future.result()  # This will raise any exceptions that occurred during file processing
            except Exception as exc:
                logging.error(f"File {file.name} generated an exception: {exc}")

    logging.info("All files have been processed.")

if __name__ == "__main__":
    logging.info("Script is starting...")
    process_files(input_dir, output_dir, max_workers=None)  # Uses os.cpu_count() by default for max_workers
    logging.info("Script has completed.")

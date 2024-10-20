# scripts/convert_to_json.py

import os
import logging
import json
import tiktoken
from multiprocessing import Pool, cpu_count
import re

def setup_logging(log_path, log_file_name):
    """
    Sets up logging to file and console.
    """
    if not os.path.exists(log_path):
        os.makedirs(log_path)
    
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    
    # Clear existing handlers
    if logger.hasHandlers():
        logger.handlers.clear()
    
    # File handler
    fh = logging.FileHandler(os.path.join(log_path, log_file_name))
    fh.setLevel(logging.INFO)
    file_formatter = logging.Formatter('%(asctime)s:%(levelname)s:%(message)s')
    fh.setFormatter(file_formatter)
    logger.addHandler(fh)
    
    # Console handler
    ch = logging.StreamHandler()
    ch.setLevel(logging.WARNING)
    console_formatter = logging.Formatter('%(asctime)s:%(levelname)s:%(message)s')
    ch.setFormatter(console_formatter)
    logger.addHandler(ch)
    
    return logger

def get_metadata(metadata_json_dir, unique_id):
    """
    Retrieves metadata from JSON files based on unique_id.
    """
    metadata_file_path = os.path.join(metadata_json_dir, f"{unique_id}.json")
    try:
        with open(metadata_file_path, 'r', encoding='utf-8') as f:
            metadata = json.load(f)
        return metadata
    except FileNotFoundError:
        logging.warning(f"No metadata JSON file found for unique_id: {unique_id}")
        return {
            "unique_id": unique_id,
            "title": "",
            "year": "",
            "category": "",
            "tags": [],
            "research_type": "",
            "sentiment_type": ""
        }
    except Exception as e:
        logging.error(f"Failed to retrieve metadata for unique_id {unique_id}: {e}", exc_info=True)
        return {
            "unique_id": unique_id,
            "title": "",
            "year": "",
            "category": "",
            "tags": [],
            "research_type": "",
            "sentiment_type": ""
        }

def extract_unique_id_from_filename(filename):
    """
    Extracts the unique_id from the filename.
    Assumes filename format: example_file_title_chunk_1.txt or example_file_title [123456].txt
    """
    # Attempt to extract using the pattern [123456]
    match = re.search(r'\[(\d{6})\]', filename)
    if match:
        return match.group(1)
    
    # Fallback: Attempt to extract numeric ID from the filename (assuming it's at the end before the extension)
    match = re.search(r'_(\d+)(?:_\d+)?\.txt$', filename)
    if match:
        return match.group(1)
    
    # If no ID found, log a warning and return 'unknown'
    logging.warning(f"Could not extract unique_id from filename: {filename}")
    return 'unknown'


def convert_chunk_to_json(args):
    """
    Converts a single text chunk into a JSON file with associated metadata.
    """
    chunk_path, output_json_dir, metadata_json_dir, tokenizer = args
    try:
        with open(chunk_path, 'r', encoding='utf-8') as f:
            text = f.read()
        
        # Extract unique_id from the filename
        filename = os.path.basename(chunk_path)
        unique_id = extract_unique_id_from_filename(filename)
        
        metadata = get_metadata(metadata_json_dir, unique_id)
        
        token_count = len(tokenizer.encode(text))
        
        # Prepare JSON data
        json_data = {
            "file_name": f"{unique_id}.pdf",
            "metadata": metadata,
            "chunk": {
                "text": text,
                "token_count": token_count
            }
        }
        
        # Construct JSON filename by replacing .txt with .json
        base_filename, _ = os.path.splitext(filename)
        json_filename = f"{base_filename}.json"
        json_path = os.path.join(output_json_dir, json_filename)
        
        # Write JSON data
        with open(json_path, 'w', encoding='utf-8') as jf:
            json.dump(json_data, jf, ensure_ascii=False, indent=4)
        
        logging.info(f"Converted {chunk_path} to {json_filename}")
        
        # Remove the text chunk after successful JSON conversion
        os.remove(chunk_path)
        logging.info(f"Removed chunk file {chunk_path} after JSON conversion")
    
    except Exception as e:
        logging.error(f"Failed to convert {chunk_path} to JSON: {e}", exc_info=True)
        # Do not raise exception to prevent stopping the entire pool

def main():
    # Paths (updated as per your request)
    input_text_chunks_dir = r"C:\Users\Georg\Desktop\Second_Batch\data\6_pre_json_cleaned"
    metadata_json_dir = r"C:\Users\Georg\Desktop\Second_Batch\data\5_metadata_db"
    output_json_dir = r"C:\Users\Georg\Desktop\Second_Batch\data\final_json"
    log_path = r"C:\Users\Georg\Desktop\Second_Batch\logs"
    log_file_name = 'convert_to_json.log'
    
    # Ensure output directory exists
    if not os.path.exists(output_json_dir):
        os.makedirs(output_json_dir)
    
    # Setup logging
    setup_logging(log_path, log_file_name)
    logging.info("JSON Conversion Workflow Started.")
    
    # Initialize tokenizer with correct encoding
    try:
        tokenizer = tiktoken.get_encoding("cl100k_base")  # Correct encoding for GPT-4
    except Exception as e:
        logging.error(f"Failed to initialize tokenizer: {e}", exc_info=True)
        return
    
    # Prepare list of chunk TXT files from input_text_chunks_dir
    try:
        chunk_files = [f for f in os.listdir(input_text_chunks_dir) if f.lower().endswith('.txt')]
    except FileNotFoundError:
        logging.error(f"Input text chunks directory {input_text_chunks_dir} does not exist.")
        return
    
    if not chunk_files:
        logging.warning(f"No text chunks found in {input_text_chunks_dir}.")
        return
    
    tasks = []
    for filename in chunk_files:
        chunk_path = os.path.join(input_text_chunks_dir, filename)
        tasks.append((chunk_path, output_json_dir, metadata_json_dir, tokenizer))
    
    # Determine number of workers based on CPU cores and default batch size
    batch_size = 4  # Adjust as needed
    num_workers = min(cpu_count(), batch_size)
    
    # Utilize multiprocessing Pool
    with Pool(processes=num_workers) as pool:
        pool.map(convert_chunk_to_json, tasks)
    
    logging.info("JSON Conversion Workflow Completed Successfully.")

if __name__ == "__main__":
    main()

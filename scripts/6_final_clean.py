import re
import unicodedata
import logging
from pathlib import Path

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def normalize_unicode(text):
    """
    Normalize unicode to NFC form to ensure consistent encoding.
    """
    return unicodedata.normalize('NFC', text)

def fix_punctuation_spacing(text):
    """
    Ensures that punctuation (like periods, commas, colons) are consistently spaced.
    - Removes spaces before punctuation (e.g., "word .", "word ,") and ensures
      there's exactly one space after (unless it's the end of the string).
    - Handles spacing for common punctuation marks.
    """
    # Remove spaces before punctuation marks
    text = re.sub(r'\s+([.,;!?])', r'\1', text)
    
    # Ensure single space after punctuation (excluding the end of the string)
    text = re.sub(r'([.,!?;:])(\w)', r'\1 \2', text)  # Ensures a space after punctuation
    
    return text

def remove_redundant_whitespace(text):
    """
    Remove unnecessary whitespaces, tabs, and normalize newlines.
    - Reduces multiple spaces to a single space.
    - Normalizes newline characters (if present) but doesn't remove them.
    """
    text = re.sub(r'[ \t]+', ' ', text)  # Replace multiple spaces or tabs with a single space
    text = re.sub(r'\s{2,}', ' ', text)  # Replace any long sequence of spaces with a single space
    return text.strip()  # Strip leading and trailing whitespace

def ensure_consistent_quotes(text):
    """
    Ensure that quotes are consistently used and fix common issues such as
    mismatched straight and curly quotes.
    """
    # Normalize quotes (replace curly quotes with straight quotes)
    text = text.replace("“", '"').replace("”", '"')
    text = text.replace("‘", "'").replace("’", "'")
    
    return text

def normalize_text(text):
    """
    Apply all normalization functions in sequence to clean and enhance the text
    without breaking existing content.
    """
    text = normalize_unicode(text)  # Ensure consistent unicode encoding
    text = fix_punctuation_spacing(text)  # Handle punctuation spacing
    text = remove_redundant_whitespace(text)  # Clean up redundant spaces and tabs
    text = ensure_consistent_quotes(text)  # Ensure consistent quote marks
    
    return text

def process_files(input_dir, output_dir):
    """
    Process each file in the input directory, clean the text, and save it to the output directory.
    """
    input_dir = Path(input_dir)
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Ensure the input directory exists
    if not input_dir.exists():
        logging.error(f"Input directory does not exist: {input_dir}")
        return
    
    files = list(input_dir.glob('*.txt'))  # Modify if you need other formats
    if not files:
        logging.warning(f"No files found in the input directory: {input_dir}")
        return

    for file_path in files:
        try:
            # Read the input file
            with file_path.open('r', encoding='utf-8') as f:
                text = f.read()

            # Normalize and enhance the text
            cleaned_text = normalize_text(text)

            # Write the cleaned text to the output file
            output_path = output_dir / file_path.name
            with output_path.open('w', encoding='utf-8') as f:
                f.write(cleaned_text)

            logging.info(f"Processed and cleaned: {file_path.name}")

        except Exception as e:
            logging.error(f"Error processing {file_path.name}: {e}")

if __name__ == "__main__":
    input_dir = r"C:\Users\Georg\Projects\Data_Processing\data\D_Chunked"
    output_dir = r"C:\Users\Georg\Projects\Data_Processing\data\E_Cleaned_Again"

    logging.info("Script is starting...")
    process_files(input_dir, output_dir)
    logging.info("Script has completed.")

import os
import re
import json
import sqlite3
import logging
from pathlib import Path
from collections import Counter
from math import log10

# Setup logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

# Paths
input_dir = Path(r"C:\Users\Georg\Projects\Data_Processing\data\E_Cleaned_Again")
output_metadata_dir = Path(r"C:\Users\Georg\Projects\Data_Processing\data\F_Metadata")
metadata_keywords_path = Path(r"C:\Users\Georg\Projects\Data_Processing\data\metadata_keywords.json")
db_path = Path(r'C:\Users\Georg\Desktop\Cognitive_Research\database\metadata.db')

# Ensure the output directory exists
output_metadata_dir.mkdir(parents=True, exist_ok=True)
db_path.parent.mkdir(parents=True, exist_ok=True)

# Load metadata keywords from JSON
def load_metadata_keywords():
    with open(metadata_keywords_path, 'r', encoding='utf-8') as f:
        return json.load(f)

metadata_keywords = load_metadata_keywords()

# Clean text function to simplify matching and remove unnecessary spaces or special characters
def clean_text(text):
    return re.sub(r'[^\w\s]', '', text).lower().strip()

# Function to ensure the year is a valid four-digit year
def clean_year(year):
    return year if re.match(r'^\d{4}$', year) else "Unknown"

# Function to clean and format tags by ensuring uniqueness and removing duplicates
def clean_tags(tags):
    return list(set(tags))  # Remove duplicate tags

# Function to skip initial blank/irrelevant pages (skip first 500 words)
def skip_initial_text(text, word_skip_count=500):
    words = text.split()
    if len(words) > word_skip_count:
        return ' '.join(words[word_skip_count:])
    return text  # If the file is short, return the full content

# Function to extract the **correct** unique ID (6 digits in brackets) from the filename
def extract_unique_id(filename):
    match = re.search(r'\[(\d{6})\]', filename)
    if match:
        return match.group(1)
    else:
        logging.error(f"No valid unique_id found in filename: {filename}")
        return None

# Improved function to extract a meaningful title, avoiding URLs (including "http" and "https") and non-title lines
def extract_title(file_content):
    lines = file_content.splitlines()
    for line in lines:
        # Skip empty lines or lines with URLs ("http", "https")
        if line.strip() and not re.match(r'(http|https|www)', line.strip(), re.IGNORECASE):
            # Check if line length is reasonable for a title
            if len(line.strip()) < 200:
                return line.strip()  # Return the first valid line as the title
            else:
                logging.warning(f"Line too long for title: {line.strip()[:50]}...")
                return line.strip()[:200]  # Return first 200 characters
    return "Untitled"  # Default if no valid title is found

# Calculate relevance based on the density of TAG_KEYWORDS in the remaining text using a logarithmic scale (1-100)
def calculate_relevance(cleaned_text):
    tag_counts = Counter()
    total_words = len(cleaned_text.split())

    if total_words < 100:
        return 1

    # Calculate tag keyword matches
    for tag, keywords in metadata_keywords['TAG_KEYWORDS'].items():
        for keyword in keywords:
            matches = cleaned_text.count(keyword.lower())  # Frequency of the keyword
            tag_counts[tag] += matches

    total_matches = sum(tag_counts.values())

    # Calculate keyword density (matches per 1000 words)
    keyword_density = (total_matches / total_words) * 1000

    # Apply logarithmic scaling to the relevance score, increasing the scaling factor to boost scores
    if keyword_density == 0:
        return 1
    scaling_factor = 77  # Increased scaling factor to boost relevance scores
    relevance_score = log10(1 + keyword_density) * scaling_factor

    # Bound the relevance score to a range of 1-100
    return min(100, max(1, round(relevance_score)))

# Function to extract metadata from a file's content and its filename
def extract_metadata(file_content, filename):
    metadata = {}

    # Extract the unique_id using the corrected regex method
    unique_id = extract_unique_id(filename)
    if not unique_id:
        return None  # Skip file if no valid unique_id is found

    metadata['unique_id'] = unique_id

    cleaned_content = clean_text(file_content)
    cleaned_content = skip_initial_text(cleaned_content)

    # Improved title extraction logic to avoid URLs and irrelevant lines
    metadata['title'] = extract_title(file_content)

    # Extract year from content
    year_match = re.search(r'\b(19|20)\d{2}\b', file_content)
    metadata['year'] = clean_year(year_match.group(0) if year_match else "Unknown")

    # Extract categories, tags, research_type, and sentiment_type
    metadata['category'] = extract_category(cleaned_content)
    metadata['tags'] = clean_tags(extract_tags(cleaned_content))
    metadata['research_type'] = extract_research_type(cleaned_content)
    metadata['sentiment_type'] = extract_sentiment_type(cleaned_content)

    # Calculate relevance based on tag keyword densities (1-100 with logarithmic scaling)
    metadata['relevance'] = calculate_relevance(cleaned_content)

    return metadata

# Helper functions to extract category, tags, research type, and sentiment
def extract_category(cleaned_text):
    category_counts = Counter()
    for category, keywords in metadata_keywords['CATEGORY_KEYWORDS'].items():
        for keyword in keywords:
            if keyword.lower() in cleaned_text:
                category_counts[category] += 1
    return category_counts.most_common(1)[0][0] if category_counts else "Uncategorized"

def extract_tags(cleaned_text):
    tag_counts = Counter()
    for tag, keywords in metadata_keywords['TAG_KEYWORDS'].items():
        for keyword in keywords:
            if keyword.lower() in cleaned_text:
                tag_counts[tag] += 1
    return [tag for tag, _ in tag_counts.most_common(5)]

def extract_research_type(cleaned_text):
    research_type_counts = Counter()
    for research_type, keywords in metadata_keywords['RESEARCH_TYPE_KEYWORDS'].items():
        for keyword in keywords:
            if keyword.lower() in cleaned_text:
                research_type_counts[research_type] += 1
    return research_type_counts.most_common(1)[0][0] if research_type_counts else "Unclassified"

def extract_sentiment_type(cleaned_text):
    sentiment_counts = Counter()
    for sentiment_type, keywords in metadata_keywords['SENTIMENT_TYPE'].items():
        for keyword in keywords:
            if keyword.lower() in cleaned_text:
                sentiment_counts[sentiment_type] += 1
    return sentiment_counts.most_common(1)[0][0] if sentiment_counts else "General Analysis"

# Function to save metadata to a JSON file
def save_metadata_to_json(metadata_json, output_metadata_dir):
    # Remove 'relevance' before saving to JSON as it's internal to the DB
    metadata_for_json = metadata_json.copy()
    metadata_for_json.pop('relevance', None)

    # Use unique_id as part of the JSON file name to maintain consistency
    output_file = output_metadata_dir / f"{metadata_for_json['unique_id']}.json"

    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(metadata_for_json, f, ensure_ascii=False, indent=4)

    logging.info(f"Saved metadata to JSON for {metadata_for_json['unique_id']}")

# Function to save metadata to SQLite database
def save_metadata_to_db(metadata_db, db_path):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Create table if not exists
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS metadata (
        unique_id TEXT PRIMARY KEY,
        title TEXT,
        year TEXT,
        category TEXT,
        tags TEXT,
        research_type TEXT,
        sentiment_type TEXT,
        relevance INTEGER
    )''')

    cursor.execute('''
    INSERT OR REPLACE INTO metadata (unique_id, title, year, category, tags, research_type, sentiment_type, relevance)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)''', (
        metadata_db['unique_id'],
        metadata_db['title'],
        metadata_db['year'],
        metadata_db['category'],
        ','.join(metadata_db['tags']),
        metadata_db['research_type'],
        metadata_db['sentiment_type'],
        metadata_db['relevance']
    ))

    conn.commit()
    conn.close()
    logging.info(f"Saved metadata to DB for {metadata_db['unique_id']}")

# Process each file in the input directory
def process_files(input_dir, output_metadata_dir, db_path):
    logging.info("Starting to process files for metadata extraction...")

    files = list(input_dir.glob('*.txt'))

    if not files:
        logging.warning(f"No TXT files found in {input_dir}.")
        return

    for file in files:
        try:
            with file.open('r', encoding='utf-8') as f:
                content = f.read()

            metadata = extract_metadata(content, file.name)
            if metadata:
                # Create separate copies for JSON and DB to prevent unintended modifications
                metadata_json = metadata.copy()
                metadata_db = metadata.copy()

                save_metadata_to_json(metadata_json, output_metadata_dir)
                save_metadata_to_db(metadata_db, db_path)

        except Exception as e:
            logging.error(f"Error processing file {file.name}: {e}")

if __name__ == "__main__":
    logging.info("Script is starting...")
    process_files(input_dir, output_metadata_dir, db_path)
    logging.info("Script has completed.")

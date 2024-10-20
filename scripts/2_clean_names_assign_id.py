import os
import fitz  # PyMuPDF
import random
import re

# Paths (adjust if necessary)
pdf_directory = r"C:\Users\Georg\Projects\Data_Processing\data\A_Collected"
unique_ids_log_path = r"C:\Users\Georg\Projects\Data_Processing\logs\unique_id_logs.txt"

# Load existing unique IDs
if os.path.exists(unique_ids_log_path):
    with open(unique_ids_log_path, 'r') as f:
        used_ids = set(line.strip() for line in f if line.strip())
else:
    used_ids = set()

def generate_unique_id(used_ids):
    while True:
        unique_id = str(random.randint(100000, 999999))
        if unique_id not in used_ids:
            used_ids.add(unique_id)
            with open(unique_ids_log_path, 'a') as f:
                f.write(unique_id + '\n')
            return unique_id

def extract_title(pdf_path):
    try:
        doc = fitz.open(pdf_path)

        # 1. Try to get the title from PDF metadata
        metadata_title = doc.metadata.get('title', '').strip()
        if metadata_title and len(metadata_title) > 5:
            doc.close()
            return metadata_title

        # 2. Use text with the largest font size
        max_font_size = 0
        title = ''
        for page_num in range(min(5, doc.page_count)):  # Limit to first 5 pages
            page = doc[page_num]
            blocks = page.get_text("dict")["blocks"]
            for block in blocks:
                if "lines" in block:
                    for line in block["lines"]:
                        for span in line["spans"]:
                            font_size = span["size"]
                            text = span["text"].strip()
                            if len(text) > 5 and font_size > max_font_size:
                                max_font_size = font_size
                                title = text

        if title:
            doc.close()
            return title

        # 3. As a backup, use the first line of text
        for page_num in range(min(5, doc.page_count)):
            page = doc[page_num]
            text = page.get_text().strip()
            if text:
                first_line = text.split('\n')[0]
                if len(first_line) > 5:
                    doc.close()
                    return first_line

        # 4. Default to the original filename (without extension)
        doc.close()
        return os.path.splitext(os.path.basename(pdf_path))[0]
    except Exception as e:
        print(f"Error extracting title from {pdf_path}: {e}")
        return os.path.splitext(os.path.basename(pdf_path))[0]

def clean_title(title):
    title = title.lower()
    # Remove unwanted characters
    title = re.sub(r'[^a-z0-9\s]', '', title)
    # Remove numbers that are not years (between 1900 and 2100)
    words = title.split()
    cleaned_words = []
    for word in words:
        if word.isdigit():
            year = int(word)
            if 1900 <= year <= 2100:
                cleaned_words.append(word)
        else:
            cleaned_words.append(word)
    # Join words with underscores
    cleaned_title = '_'.join(cleaned_words)
    return cleaned_title

def rename_pdf(pdf_path):
    original_filename = os.path.basename(pdf_path)
    title = extract_title(pdf_path)
    cleaned_title = clean_title(title)
    unique_id = generate_unique_id(used_ids)
    new_filename = f"{cleaned_title} [{unique_id}].pdf"
    new_path = os.path.join(os.path.dirname(pdf_path), new_filename)
    try:
        os.rename(pdf_path, new_path)
        print(f"Renamed '{original_filename}' to '{new_filename}'")
    except Exception as e:
        print(f"Error renaming {pdf_path}: {e}")

def main():
    for filename in os.listdir(pdf_directory):
        if filename.lower().endswith('.pdf'):
            pdf_path = os.path.join(pdf_directory, filename)
            rename_pdf(pdf_path)

if __name__ == "__main__":
    main()

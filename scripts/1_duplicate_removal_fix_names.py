import os
import hashlib
import re
from PyPDF2 import PdfReader

# Configuration
FOLDER_PATH = r"C:\Users\Georg\Projects\Data_Processing\data\A_Collected"
PROHIBITED_SUBSTRINGS = {'https', 'www', 'untitled', 'arxiv'}
HASH_FUNCTION = 'sha256'

def read_initial_bytes(file_path, num_bytes=1024):
    try:
        with open(file_path, 'rb') as f:
            return f.read(num_bytes)
    except Exception as e:
        print(f"Error reading initial bytes of {file_path}: {e}")
        return b''

def compute_hash(file_path, hash_function='sha256'):
    try:
        hash_func = hashlib.new(hash_function)
        with open(file_path, 'rb') as f:
            while chunk := f.read(8192):
                hash_func.update(chunk)
        return hash_func.hexdigest()
    except Exception as e:
        print(f"Error computing hash for {file_path}: {e}")
        return None

def extract_title(file_path):
    try:
        reader = PdfReader(file_path)
        # Attempt to get title from metadata
        if reader.metadata and reader.metadata.title:
            title = reader.metadata.title
        else:
            # Fallback: extract text from first page
            if len(reader.pages) > 0:
                first_page = reader.pages[0]
                text = first_page.extract_text()
                if text:
                    # Use the first non-empty line as title
                    lines = text.split('\n')
                    for line in lines:
                        clean_line = line.strip()
                        if clean_line:
                            title = clean_line
                            break
                    else:
                        title = "unknown_title"
                else:
                    title = "unknown_title"
            else:
                title = "unknown_title"
        
        # Clean the title
        title = title.lower()
        title = re.sub(r'\d+', '', title)  # Remove numbers
        title = re.sub(r'[^\w\s]', '', title)  # Remove punctuation
        title = re.sub(r'\s+', '_', title.strip())  # Replace spaces with underscores

        # Remove prohibited substrings
        for substr in PROHIBITED_SUBSTRINGS:
            if substr in title:
                title = title.replace(substr, '')
        
        # Remove leading/trailing underscores
        title = title.strip('_')

        # If title is empty after cleaning, assign a default name
        if not title:
            title = "unknown_title"
        
        return title
    except Exception as e:
        print(f"Error extracting title from {file_path}: {e}")
        return "unknown_title"

def assign_title(file_path, existing_titles):
    title = extract_title(file_path)
    original_title = title
    counter = 1
    # Ensure the title is unique to prevent overwriting
    while title in existing_titles:
        title = f"{original_title}_{counter}"
        counter += 1
    existing_titles.add(title)
    return title

def main():
    file_size_dict = {}
    # Step 1: Group files by size
    for filename in os.listdir(FOLDER_PATH):
        if filename.lower().endswith('.pdf'):
            file_path = os.path.join(FOLDER_PATH, filename)
            try:
                size = os.path.getsize(file_path)
                file_size_dict.setdefault(size, []).append(file_path)
            except Exception as e:
                print(f"Error getting size for {file_path}: {e}")

    potential_duplicates = []
    exact_duplicates = []

    # Step 2: Identify potential duplicates by initial bytes
    for size, files in file_size_dict.items():
        if len(files) > 1:
            initial_contents = {}
            for file in files:
                content = read_initial_bytes(file)
                if content in initial_contents:
                    potential_duplicates.append((file, initial_contents[content]))
                else:
                    initial_contents[content] = file

    # Step 3: Identify exact duplicates by full hash
    for size, files in file_size_dict.items():
        if len(files) > 1:
            hash_dict = {}
            for file in files:
                file_hash = compute_hash(file, HASH_FUNCTION)
                if not file_hash:
                    continue  # Skip files with hash computation issues
                if file_hash in hash_dict:
                    exact_duplicates.append((file, hash_dict[file_hash]))
                else:
                    hash_dict[file_hash] = file

    # Step 4: Remove exact duplicates
    for duplicate_pair in exact_duplicates:
        file_to_remove = duplicate_pair[0]  # Keeping the first occurrence
        try:
            os.remove(file_to_remove)
            print(f"Removed duplicate file: {file_to_remove}")
        except Exception as e:
            print(f"Error removing file {file_to_remove}: {e}")

    # Step 5: Assign titles to unique files
    # Rebuild the list of unique files after removal
    unique_files = []
    for size, files in file_size_dict.items():
        if len(files) == 1:
            unique_files.append(files[0])
        else:
            # From duplicates, keep only one file
            hashes = {}
            for file in files:
                file_hash = compute_hash(file, HASH_FUNCTION)
                if not file_hash:
                    continue
                if file_hash not in hashes:
                    hashes[file_hash] = file
            unique_files.extend(hashes.values())

    existing_titles = set()
    for file_path in unique_files:
        title = assign_title(file_path, existing_titles)
        new_filename = f"{title}.pdf"
        new_file_path = os.path.join(FOLDER_PATH, new_filename)
        try:
            os.rename(file_path, new_file_path)
            print(f"Renamed '{file_path}' to '{new_filename}'")
        except Exception as e:
            print(f"Error renaming file {file_path} to {new_filename}: {e}")

if __name__ == "__main__":
    main()

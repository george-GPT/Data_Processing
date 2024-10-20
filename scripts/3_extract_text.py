import os
import logging
import pytesseract  # Using pytesseract for OCR
import fitz  # PyMuPDF for image and table extraction
from multiprocessing import Pool, cpu_count
import re
import tabula  # Importing tabula-py
import time
import psutil  # For memory monitoring
import cv2
import numpy as np
import pdfplumber  # Additional PDF processing library as a backup
from PyPDF2 import PdfReader  # Backup for text extraction
import gc  # For memory management

# Set up logging to file and console
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


# Input and output directories
PDF_INPUT_DIR = r"C:\Users\Georg\Projects\Data_Processing\data\A_Collected"
OUTPUT_DIR = r"C:\Users\Georg\Projects\Data_Processing\data\B_Extracted"
TESSERACT_CMD = r"C:\Users\Georg\Desktop\tesseract-main\tesseract.exe"


# Ensure necessary output subdirectories
TEXT_OUTPUT_DIR = os.path.join(OUTPUT_DIR, 'text')
IMAGES_OUTPUT_DIR = os.path.join(OUTPUT_DIR, 'images')
TABLES_OUTPUT_DIR = os.path.join(OUTPUT_DIR, 'tables')

# Ensure output directories exist
for directory in [TEXT_OUTPUT_DIR, IMAGES_OUTPUT_DIR, TABLES_OUTPUT_DIR]:
    if not os.path.exists(directory):
        os.makedirs(directory)
        logging.info(f"Created directory: {directory}")

# Helper function to extract the unique ID from the filename
def get_unique_id(filename):
    # Updated regex to capture 6-digit ID inside square brackets
    match = re.search(r'\[(\d{6})\]', filename)
    if match:
        return match.group(1)
    else:
        logging.error(f"Unique identifier not found in filename: {filename}")
        return None

def clean_filename(base_filename, unique_id):
    # Ensure the unique_id is not appended a second time
    if unique_id in base_filename:
        return base_filename
    return f"{base_filename} [{unique_id}]"

def log_memory_usage():
    process = psutil.Process(os.getpid())
    logging.info(f"Current memory usage: {process.memory_info().rss / (1024 * 1024):.2f} MB")

def retry(function, retries=3, wait_time=5):
    for attempt in range(retries):
        try:
            return function()
        except Exception as e:
            logging.warning(f"Attempt {attempt + 1} failed: {e}. Retrying in {wait_time} seconds...")
            time.sleep(wait_time)
    logging.error(f"All {retries} attempts failed.")
    return False

def extract_text_from_pdf(pdf_path, output_path):
    return retry(lambda: _extract_text(pdf_path, output_path))

def _extract_text(pdf_path, output_path):
    try:
        logging.info(f"Starting text extraction for {pdf_path}")
        log_memory_usage()
        pytesseract.pytesseract.tesseract_cmd = TESSERACT_CMD
        tesseract_config = '--oem 1 --psm 6'  # LSTM OCR engine with page segmentation for block of text

        try:
            # First attempt: Try extracting text directly using PyMuPDF
            doc = fitz.open(pdf_path)
            full_text = "\n".join([page.get_text("text") for page in doc])

            # If direct extraction fails (e.g., no text is found), fallback to OCR
            if not full_text.strip():
                for page_num in range(len(doc)):
                    page = doc.load_page(page_num)
                    pix = page.get_pixmap()
                    img = np.frombuffer(pix.tobytes(), np.uint8)  # Direct in-memory pass to avoid file I/O
                    text = pytesseract.image_to_string(cv2.imdecode(img, cv2.IMREAD_COLOR), config=tesseract_config)
                    full_text += text + "\n"
            # Save extracted text
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write(full_text)

        except Exception as e:
            logging.error(f"Failed with PyMuPDF. Trying PDFPlumber for {pdf_path}: {e}")
            # Fallback to pdfplumber for text extraction
            try:
                with pdfplumber.open(pdf_path) as pdf:
                    full_text = "\n".join([page.extract_text() for page in pdf.pages if page.extract_text()])
                with open(output_path, 'w', encoding='utf-8') as f:
                    f.write(full_text)
            except Exception as e:
                logging.error(f"Failed text extraction with both methods: {e}")
                return False

        log_memory_usage()
        gc.collect()  # Explicit memory cleanup
    except Exception as e:
        logging.error(f"Failed to extract text from {pdf_path}: {e}")
        return False
    finally:
        if 'doc' in locals():
            doc.close()
    return True

def extract_images_tables(pdf_path, base_filename, unique_id):
    try:
        logging.info(f"Extracting images from {pdf_path}")
        doc = fitz.open(pdf_path)

        for page_num in range(len(doc)):
            page = doc.load_page(page_num)
            images = page.get_images(full=True)
            for img_index, img in enumerate(images):
                xref = img[0]
                base_image = doc.extract_image(xref)
                image_bytes = base_image["image"]
                image_ext = base_image["ext"]
                image_filename = f"{base_filename} [{unique_id}]_page{page_num+1}_img{img_index+1}.{image_ext}"
                image_path = os.path.join(IMAGES_OUTPUT_DIR, image_filename)

                if image_contains_relevant_content(image_bytes):
                    with open(image_path, 'wb') as img_file:
                        img_file.write(image_bytes)
                    logging.info(f"Extracted relevant image to {image_path}")
                else:
                    logging.info(f"Skipped irrelevant image on page {page_num+1} of {pdf_path}")

        logging.info(f"Extracting tables from {pdf_path}")
        tables = tabula.read_pdf(pdf_path, pages='all', multiple_tables=True, lattice=True)
        for i, table in enumerate(tables):
            table_filename = f"{base_filename} [{unique_id}]_table_{i+1}.csv"
            table_path = os.path.join(TABLES_OUTPUT_DIR, table_filename)
            table.to_csv(table_path, index=False)
            logging.info(f"Extracted table to {table_path}")

        doc.close()
    except Exception as e:
        logging.error(f"Failed to extract images/tables from {pdf_path}: {e}")
        return False
    gc.collect()  # Explicit memory cleanup after processing large files
    return True

# Added compute_colorfulness function
def compute_colorfulness(image):
    # Split the image into its respective RGB components
    (B, G, R) = cv2.split(image.astype("float"))

    # Compute rg = R - G
    rg = np.absolute(R - G)

    # Compute yb = 0.5 * (R + G) - B
    yb = np.absolute(0.5 * (R + G) - B)

    # Compute the mean and standard deviation of both 'rg' and 'yb'
    (rbMean, rbStd) = (np.mean(rg), np.std(rg))
    (ybMean, ybStd) = (np.mean(yb), np.std(yb))

    # Combine the mean and standard deviations
    stdRoot = np.sqrt((rbStd ** 2) + (ybStd ** 2))
    meanRoot = np.sqrt((rbMean ** 2) + (ybMean ** 2))

    # Compute the colorfulness metric and return it
    return stdRoot + (0.3 * meanRoot)

# Adjusted image_contains_relevant_content function
def image_contains_relevant_content(image_bytes):
    np_arr = np.frombuffer(image_bytes, np.uint8)
    img = cv2.imdecode(np_arr, cv2.IMREAD_COLOR)

    # Compute colorfulness
    colorfulness = compute_colorfulness(img)

    if colorfulness < 10:
        # Low colorfulness, possibly text image
        # Convert image to RGB (pytesseract expects RGB)
        img_rgb = cv2.cvtColor(img, cv2.COLOR_BGR2RGB)

        # Use Tesseract to detect text in the image
        custom_config = '--oem 3 --psm 6'  # LSTM OCR engine, assume a single uniform block of text
        data = pytesseract.image_to_data(img_rgb, output_type=pytesseract.Output.DICT, config=custom_config)
        num_words = len([word for word in data['text'] if word.strip() != ''])

        # If significant text is detected, consider it as text content and skip the image
        if num_words > 10:
            return False  # Skip image containing text content
        else:
            return True  # Keep image
    else:
        # Image is colorful, likely to be a picture
        return True

def process_pdf(pdf_path):
    base_filename = os.path.splitext(os.path.basename(pdf_path))[0]
    unique_id = get_unique_id(base_filename)

    if not unique_id:
        logging.error(f"Skipping file {pdf_path} due to missing unique ID.")
        return

    base_filename = clean_filename(base_filename, unique_id)
    text_output_path = os.path.join(TEXT_OUTPUT_DIR, f"{base_filename}.txt")

    if extract_text_from_pdf(pdf_path, text_output_path):
        logging.info(f"Text extraction completed for {pdf_path}")
    else:
        logging.warning(f"Text extraction failed for {pdf_path}")

    if extract_images_tables(pdf_path, base_filename, unique_id):
        logging.info(f"Images and tables extraction completed for {pdf_path}")
    else:
        logging.warning(f"Images and tables extraction failed for {pdf_path}")

def main():
    pdf_files = [f for f in os.listdir(PDF_INPUT_DIR) if f.lower().endswith('.pdf')]

    if not pdf_files:
        logging.warning(f"No PDF files found in {PDF_INPUT_DIR}.")
        return

    num_workers = min(cpu_count(), len(pdf_files))
    with Pool(processes=num_workers) as pool:
        pool.map(process_pdf, [os.path.join(PDF_INPUT_DIR, pdf_file) for pdf_file in pdf_files])

if __name__ == "__main__":
    logging.info("Starting PDF processing workflow...")
    main()
    logging.info("PDF processing completed.")

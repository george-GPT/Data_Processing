import os
import logging
import tiktoken
from multiprocessing import Pool, cpu_count, Lock
import re
import psutil
import time
import spacy

# Lock for multiprocessing safety
lock = Lock()

# Paths for input and output
INPUT_DIR = r"C:\Users\Georg\Projects\Data_Processing\data\C_Cleaned"
OUTPUT_DIR = r"C:\Users\Georg\Projects\Data_Processing\data\D_Chunked"

# Ensure the output directory exists
if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)

def setup_logging(log_file_name='segment_text.log'):
    logger = logging.getLogger()
    if not logger.hasHandlers():
        logger.setLevel(logging.INFO)
        fh = logging.FileHandler(log_file_name)
        fh.setLevel(logging.INFO)
        file_formatter = logging.Formatter('%(asctime)s:%(levelname)s:%(message)s')
        fh.setFormatter(file_formatter)
        logger.addHandler(fh)

        ch = logging.StreamHandler()
        ch.setLevel(logging.WARNING)
        console_formatter = logging.Formatter('%(asctime)s:%(levelname)s:%(message)s')
        ch.setFormatter(console_formatter)
        logger.addHandler(ch)

    return logger

def throttle_if_needed(threshold=85, sleep_time=5):
    cpu_usage = psutil.cpu_percent()
    if cpu_usage > threshold:
        logging.warning(f"High CPU usage detected: {cpu_usage}%. Throttling processing for {sleep_time} seconds.")
        time.sleep(sleep_time)

def preprocess_text(text):
    segments = []
    code_pattern = re.compile(r'```[\s\S]*?```', re.DOTALL)
    last_idx = 0

    for match in code_pattern.finditer(text):
        if last_idx < match.start():
            segments.append(('text', text[last_idx:match.start()]))
        segments.append(('code', match.group()))
        last_idx = match.end()

    if last_idx < len(text):
        segments.append(('text', text[last_idx:]))

    return segments

def split_into_sentences(text, max_length=None):
    if nlp is None:
        logging.error("NLP model is not initialized.")
        return []

    sentences = []
    current_start = 0
    text_length = len(text)
    max_length = max_length or 1000000

    while current_start < text_length:
        current_end = min(current_start + max_length, text_length)
        sub_text = text[current_start:current_end]

        if current_end < text_length:
            last_space = sub_text.rfind(' ')
            if last_space != -1:
                current_end = current_start + last_space
                sub_text = text[current_start:current_end]

        doc = nlp(sub_text)
        sub_sentences = [sent.text.strip() for sent in doc.sents]
        sentences.extend(sub_sentences)

        current_start = current_end

    return sentences

def chunk_segments(segments, max_tokens, overlap_tokens):
    chunks = []
    current_chunk = ""
    current_tokens = 0
    overlap_text = ""

    for segment_type, segment in segments:
        if segment_type == 'code':
            segment_tokens = len(tokenizer.encode(segment))
            if current_tokens + segment_tokens > max_tokens:
                if current_chunk:
                    chunks.append({
                        "text": current_chunk.strip(),
                        "token_count": current_tokens
                    })
                    current_chunk = ""
                    current_tokens = 0

            current_chunk += "\n" + segment
            current_tokens += segment_tokens
        else:
            sentences = split_into_sentences(segment)
            for sentence in sentences:
                sentence_tokens = len(tokenizer.encode(sentence))
                if current_tokens + sentence_tokens > max_tokens:
                    if current_chunk:
                        if overlap_tokens > 0:
                            encoded_current = tokenizer.encode(current_chunk)
                            overlap_tokens_actual = min(overlap_tokens, len(encoded_current))
                            overlap_text = tokenizer.decode(encoded_current[-overlap_tokens_actual:])
                        else:
                            overlap_text = ""
                        chunks.append({
                            "text": current_chunk.strip(),
                            "token_count": current_tokens
                        })
                        current_chunk = overlap_text
                        current_tokens = len(tokenizer.encode(current_chunk))

                current_chunk += " " + sentence
                current_tokens += sentence_tokens

    if current_chunk.strip():
        chunks.append({
            "text": current_chunk.strip(),
            "token_count": current_tokens
        })
    
    return chunks

def segment_text_file(input_path, output_dir):
    try:
        throttle_if_needed()

        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
            logging.info(f"Created directory {output_dir}")

        with open(input_path, 'r', encoding='utf-8') as f:
            text = f.read()

        logging.info(f"Starting segmentation for file: {input_path}")

        max_tokens = 3000  # Set the maximum tokens per chunk
        overlap_tokens = 200  # Set the overlap tokens for chunking

        segments = preprocess_text(text)
        chunks = chunk_segments(segments, max_tokens, overlap_tokens)
        logging.info(f"Total chunks created: {len(chunks)} for file {input_path}.")

        base_filename = os.path.splitext(os.path.basename(input_path))[0]
        for i, chunk in enumerate(chunks):
            chunk_filename = f"{base_filename}_chunk_{i+1}.txt"
            chunk_path = os.path.join(output_dir, chunk_filename)

            if os.path.exists(chunk_path):
                logging.info(f"Chunk {chunk_filename} already exists. Skipping.")
                continue

            with open(chunk_path, 'w', encoding='utf-8') as cf:
                cf.write(chunk['text'])
            logging.info(f"Created chunk {chunk_filename} with {chunk['token_count']} tokens.")
        
        if chunks:
            #os.remove(input_path)
            #logging.info(f"Removed original file {input_path} after successful segmentation.")
        #else:
            logging.warning(f"No chunks created for {input_path}. Original file retained.")

    except FileNotFoundError as e:
        logging.error(f"File not found: {e}")
    except Exception as e:
        logging.error(f"Failed to segment text from {input_path}: {e}", exc_info=True)

def worker_init():
    global nlp, tokenizer
    try:
        nlp = spacy.load("en_core_web_sm", disable=["parser", "ner"])
        if "sentencizer" not in nlp.pipe_names:
            nlp.add_pipe("sentencizer")
        tokenizer = tiktoken.get_encoding("cl100k_base")
    except OSError:
        logging.info("spaCy model not found. Downloading 'en_core_web_sm'...")
        from spacy.cli import download
        download("en_core_web_sm")
        nlp = spacy.load("en_core_web_sm", disable=["parser", "ner"])
        nlp.add_pipe("sentencizer")
        tokenizer = tiktoken.get_encoding("cl100k_base")
        logging.info("spaCy model 'en_core_web_sm' downloaded and loaded successfully.")

def main():
    setup_logging()

    if not os.path.exists(INPUT_DIR):
        logging.error(f"Input directory not found: {INPUT_DIR}")
        return

    txt_files = [f for f in os.listdir(INPUT_DIR) if f.lower().endswith('.txt')]
    if not txt_files:
        logging.warning(f"No TXT files found in {INPUT_DIR} for segmentation.")
        return

    tasks = [(os.path.join(INPUT_DIR, txt_file), OUTPUT_DIR) for txt_file in txt_files]

    num_workers = min(cpu_count(), len(tasks))

    with Pool(processes=num_workers, initializer=worker_init) as pool:
        pool.starmap(segment_text_file, tasks)

    logging.info("Text Segmentation Workflow Completed Successfully.")

if __name__ == "__main__":
    main()

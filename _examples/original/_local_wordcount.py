import os
import re
import time
import hashlib
import json

def read_text_file(file_path: str) -> str:
    with open(file_path, 'r', encoding='utf-8') as file:
        return file.read()

def count_words(text: str) -> dict[str, int]:
    words = re.findall(r'\b\w+\b', text.lower())
    word_counts = {}
    for word in words:
        word_counts[word] = word_counts.get(word, 0) + 1
    return word_counts

def hash_dict(d):
    """
    Create a consistent hash of a dictionary.
    
    Args:
        d (dict): Dictionary to hash
    
    Returns:
        str: SHA-256 hash of the sorted dictionary
    """
    sorted_dict = dict(sorted(d.items()))
    json_string = json.dumps(sorted_dict, sort_keys=True)
    return hashlib.sha256(json_string.encode()).hexdigest()

def main():
    # File paths
    INPUT_FILE = os.path.join("..", "_inputs", "shakespeare.txt")
    OUTPUT_FILE = os.path.join("..", "_outputs", "correct_word_frequencies.txt")

    start_time = time.time()

    text = read_text_file(INPUT_FILE)
    word_counts = count_words(text)

    with open(OUTPUT_FILE, 'w', encoding='utf-8') as outfile:
        outfile.write(str(word_counts))

    makespan = time.time() - start_time

    print(f"Wordcount Hash: {hash_dict(word_counts)} | Makespan: {makespan}s")

    # Print top 10 words for verification
    # top_10 = sorted(word_counts.items(), key=lambda x: x[1], reverse=True)[:10]
    # print("\nTop 10 Words:")
    # for word, count in top_10:
    #     print(f"{word}: {count}")

    return word_counts

if __name__ == "__main__":
    main()
import os
import sys
import time
from collections import Counter
from typing import List, Dict, Any

# Add parent directory to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from src.dag_task_node import DAGTask

# Import centralized configuration
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from common.config import WORKER_CONFIG

def _read_file(path: str) -> str:
    with open(path, "r", encoding="utf-8") as f:
        return f.read()

# --- DAG Tasks ---

# --- Fan-out group 1 (same task type: word counts on chunks) ---

@DAGTask
def word_count_chunk(text: str, start: int, end: int) -> int:
    words = text.split()
    return len(words[start:end])


@DAGTask
def merge_word_counts(counts: List[int]) -> int:
    return sum(counts)


# --- Fan-out group 2 (different tasks on same text) ---

@DAGTask
def sentence_count(text: str) -> int:
    return len([s for s in text.split('.') if s.strip()])


@DAGTask
def avg_word_length(text: str) -> float:
    words = [w for w in text.split() if w.strip()]
    return sum(len(w) for w in words) / len(words) if words else 0.0


@DAGTask
def most_frequent_word(text: str) -> str:
    words = [w for w in text.split() if w.strip()]
    return Counter(words).most_common(1)[0][0] if words else ""


@DAGTask
def merge_features(sentence_count: int, avg_word_len: float, top_word: str) -> Dict[str, Any]:
    return {
        "sentence_count": sentence_count,
        "avg_word_length": avg_word_len,
        "most_frequent_word": top_word,
    }


# --- Branch A (2 tasks) ---

@DAGTask
def scale_word_count(total_count: int) -> float:
    """Normalize by 1000 words just as a demo metric."""
    return total_count / 1000.0


@DAGTask
def report_word_stats(scaled_count: float) -> Dict[str, Any]:
    return {"scaled_word_count": scaled_count}


# --- Branch B (4 tasks) ---

@DAGTask
def normalize_features(features: Dict[str, Any]) -> Dict[str, Any]:
    """Apply toy normalization."""
    f = dict(features)
    f["avg_word_length"] = f["avg_word_length"] / 10.0
    return f


@DAGTask
def score_features(features: Dict[str, Any]) -> Dict[str, Any]:
    """Assign a toy score."""
    f = dict(features)
    f["score"] = f["sentence_count"] * f["avg_word_length"]
    return f


@DAGTask
def rank_features(features: Dict[str, Any]) -> Dict[str, Any]:
    """Rank the top word by fake criteria."""
    f = dict(features)
    f["rank"] = 1 if f.get("most_frequent_word") else 0
    return f


@DAGTask
def report_features(features: Dict[str, Any]) -> Dict[str, Any]:
    return {"features_report": features}


# --- Final sink ---

@DAGTask
def final_report(word_stats: Dict[str, Any], feature_report: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "word_stats": word_stats,
        "feature_analysis": feature_report,
    }


# --- Define Workflow ---

input_file = "../_inputs/shakespeare.txt"
text = _read_file(input_file)

# fan-out group 1 (3 same-type tasks)
wc1 = word_count_chunk(text, 0, 50)
wc2 = word_count_chunk(text, 50, 100)
wc3 = word_count_chunk(text, 100, 150)
total_wc = merge_word_counts([wc1, wc2, wc3])

# fan-out group 2 (3 different tasks)
sc = sentence_count(text)
awl = avg_word_length(text)
top = most_frequent_word(text)
features = merge_features(sc, awl, top)

# branch A (2 tasks in sequence)
scaled = scale_word_count(total_wc)
word_stats = report_word_stats(scaled)

# branch B (4 tasks in sequence)
norm = normalize_features(features)
scored = score_features(norm)
ranked = rank_features(scored)
feature_report = report_features(ranked)

# final sink
report = final_report(word_stats, feature_report)


# --- Run workflow ---
# report.visualize_dag(output_file=os.path.join("..", "_dag_visualization", "text_analysis_complex"), open_after=True)

start_time = time.time()
result = report.compute(dag_name="text_analysis", config=WORKER_CONFIG)
print(f"Final Text Analysis Report: {result} | User waited: {time.time() - start_time:.3f}s")

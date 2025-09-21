import os
import sys
import time
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

# --- Initial fan-out group (word counts on chunks) ---

@DAGTask
def word_count_chunk(text: str, start: int, end: int) -> int:
    words = text.split()
    return len(words[start:end])

@DAGTask
def merge_word_counts(counts: List[int]) -> int:
    return sum(counts)

# --- Single processing task that creates intermediate result ---

@DAGTask
def create_text_segments(text: str, total_word_count: int) -> List[str]:
    """Create 16 text segments for further analysis"""
    words = text.split()
    segment_size = len(words) // 16

    segments = []
    for i in range(16):
        start_idx = i * segment_size
        end_idx = len(words) if i == 15 else (i + 1) * segment_size
        segments.append(" ".join(words[start_idx:end_idx]))
    return segments


@DAGTask
def analyze_segment(segments: List[str], segment_id: int) -> Dict[str, Any]:
    text = segments[segment_id]
    words = text.split()
    return {
        "word_count": len(words),
        "avg_word_length": sum(len(w) for w in words) / len(words) if words else 0,
        "sentence_count": len([s for s in text.split('.') if s.strip()])
    }


@DAGTask
def merge_segment_analyses(*segments: Dict[str, Any]) -> Dict[str, Any]:
    """Merge all 16 segment analyses"""
    total_words = sum(s["word_count"] for s in segments)
    avg_word_length = (
        sum(s["avg_word_length"] * s["word_count"] for s in segments) / total_words
        if total_words > 0 else 0
    )
    total_sentences = sum(s["sentence_count"] for s in segments)
    
    return {
        "total_segments": len(segments),
        "total_words": total_words,
        "overall_avg_word_length": avg_word_length,
        "total_sentences": total_sentences,
        "segment_details": segments
    }


# --- Additional processing tasks (creating another branch) ---

@DAGTask
def calculate_text_metrics(merged_analysis: Dict[str, Any]) -> Dict[str, Any]:
    """Calculate additional text metrics"""
    return {
        "words_per_sentence": merged_analysis["total_words"] / merged_analysis["total_sentences"] if merged_analysis["total_sentences"] > 0 else 0,
        "segment_balance_score": min(s["word_count"] for s in merged_analysis["segment_details"]) / max(s["word_count"] for s in merged_analysis["segment_details"]) if merged_analysis["segment_details"] else 0,
        "complexity_score": merged_analysis["overall_avg_word_length"] * merged_analysis["total_sentences"] / 100
    }

@DAGTask
def generate_text_summary(merged_analysis: Dict[str, Any]) -> Dict[str, Any]:
    """Generate a summary of the text"""
    return {
        "summary": f"Text contains {merged_analysis['total_words']} words across {merged_analysis['total_segments']} segments",
        "readability": "high" if merged_analysis["overall_avg_word_length"] < 5 else "medium" if merged_analysis["overall_avg_word_length"] < 7 else "low"
    }

# --- Final convergence ---

@DAGTask
def final_comprehensive_report(metrics: Dict[str, Any], summary: Dict[str, Any], 
                              merged_analysis: Dict[str, Any]) -> Dict[str, Any]:
    """Create final comprehensive report"""
    return {
        "analysis_metrics": metrics,
        "text_summary": summary,
        "detailed_analysis": merged_analysis
    }

# --- Define Workflow ---

input_file = "../_inputs/shakespeare.txt"
text = _read_file(input_file)

# Initial fan-out group (3 word count tasks)
chunk_size = 200
num_chunks = 10
word_counts = []

for i in range(num_chunks):
    start = i * chunk_size
    end = (i + 1) * chunk_size
    wc = word_count_chunk(text, start, end)
    word_counts.append(wc)

total_wc = merge_word_counts(word_counts)

# Single task that prepares data for middle fan-out
segments_data = create_text_segments(text, total_wc)

# MIDDLE FAN-OUT: generate 16 analyses in a loop
segment_analyses = [analyze_segment(segments_data, i) for i in range(16)]

# Fan-in: Merge all 16 segment analyses
merged_analysis = merge_segment_analyses(*segment_analyses)

# Create two branches from the merged analysis
metrics = calculate_text_metrics(merged_analysis)
summary = generate_text_summary(merged_analysis)

final_report = final_comprehensive_report(metrics, summary, merged_analysis)
# final_report.visualize_dag(output_file=os.path.join("_dag_visualization", "text_analysis"), open_after=False)

# --- Run workflow ---
start_time = time.time()
result = final_report.compute(dag_name="text_analysis", config=WORKER_CONFIG)
print(f"Result: {result} | User waited: {time.time() - start_time:.3f}s")
print(f"Analysis complete - processed {result['detailed_analysis']['total_words']} words in {result['detailed_analysis']['total_segments']} segments")
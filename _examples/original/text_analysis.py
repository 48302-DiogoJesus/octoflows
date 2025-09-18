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
def create_text_segments(text: str, total_word_count: int) -> Dict[str, Any]:
    """Create segments of text for further analysis"""
    words = text.split()
    segment_size = len(words) // 16  # Split into 16 segments
    
    segments = {}
    for i in range(16):
        start_idx = i * segment_size
        if i == 15:  # Last segment gets remaining words
            end_idx = len(words)
        else:
            end_idx = (i + 1) * segment_size
        segments[f"segment_{i+1}"] = " ".join(words[start_idx:end_idx])
    
    segments["total_word_count"] = total_word_count
    return segments

# --- MIDDLE FAN-OUT: Fixed 16-way fan-out for segment analysis ---

@DAGTask
def analyze_segment_1(segments_data: Dict[str, Any]) -> Dict[str, Any]:
    text = segments_data["segment_1"]
    words = text.split()
    return {
        "segment_id": 1,
        "word_count": len(words),
        "avg_word_length": sum(len(w) for w in words) / len(words) if words else 0,
        "sentence_count": len([s for s in text.split('.') if s.strip()])
    }

@DAGTask
def analyze_segment_2(segments_data: Dict[str, Any]) -> Dict[str, Any]:
    text = segments_data["segment_2"]
    words = text.split()
    return {
        "segment_id": 2,
        "word_count": len(words),
        "avg_word_length": sum(len(w) for w in words) / len(words) if words else 0,
        "sentence_count": len([s for s in text.split('.') if s.strip()])
    }

@DAGTask
def analyze_segment_3(segments_data: Dict[str, Any]) -> Dict[str, Any]:
    text = segments_data["segment_3"]
    words = text.split()
    return {
        "segment_id": 3,
        "word_count": len(words),
        "avg_word_length": sum(len(w) for w in words) / len(words) if words else 0,
        "sentence_count": len([s for s in text.split('.') if s.strip()])
    }

@DAGTask
def analyze_segment_4(segments_data: Dict[str, Any]) -> Dict[str, Any]:
    text = segments_data["segment_4"]
    words = text.split()
    return {
        "segment_id": 4,
        "word_count": len(words),
        "avg_word_length": sum(len(w) for w in words) / len(words) if words else 0,
        "sentence_count": len([s for s in text.split('.') if s.strip()])
    }

@DAGTask
def analyze_segment_5(segments_data: Dict[str, Any]) -> Dict[str, Any]:
    text = segments_data["segment_5"]
    words = text.split()
    return {
        "segment_id": 5,
        "word_count": len(words),
        "avg_word_length": sum(len(w) for w in words) / len(words) if words else 0,
        "sentence_count": len([s for s in text.split('.') if s.strip()])
    }

@DAGTask
def analyze_segment_6(segments_data: Dict[str, Any]) -> Dict[str, Any]:
    text = segments_data["segment_6"]
    words = text.split()
    return {
        "segment_id": 6,
        "word_count": len(words),
        "avg_word_length": sum(len(w) for w in words) / len(words) if words else 0,
        "sentence_count": len([s for s in text.split('.') if s.strip()])
    }

@DAGTask
def analyze_segment_7(segments_data: Dict[str, Any]) -> Dict[str, Any]:
    text = segments_data["segment_7"]
    words = text.split()
    return {
        "segment_id": 7,
        "word_count": len(words),
        "avg_word_length": sum(len(w) for w in words) / len(words) if words else 0,
        "sentence_count": len([s for s in text.split('.') if s.strip()])
    }

@DAGTask
def analyze_segment_8(segments_data: Dict[str, Any]) -> Dict[str, Any]:
    text = segments_data["segment_8"]
    words = text.split()
    return {
        "segment_id": 8,
        "word_count": len(words),
        "avg_word_length": sum(len(w) for w in words) / len(words) if words else 0,
        "sentence_count": len([s for s in text.split('.') if s.strip()])
    }

@DAGTask
def analyze_segment_9(segments_data: Dict[str, Any]) -> Dict[str, Any]:
    text = segments_data["segment_9"]
    words = text.split()
    return {
        "segment_id": 9,
        "word_count": len(words),
        "avg_word_length": sum(len(w) for w in words) / len(words) if words else 0,
        "sentence_count": len([s for s in text.split('.') if s.strip()])
    }

@DAGTask
def analyze_segment_10(segments_data: Dict[str, Any]) -> Dict[str, Any]:
    text = segments_data["segment_10"]
    words = text.split()
    return {
        "segment_id": 10,
        "word_count": len(words),
        "avg_word_length": sum(len(w) for w in words) / len(words) if words else 0,
        "sentence_count": len([s for s in text.split('.') if s.strip()])
    }

@DAGTask
def analyze_segment_11(segments_data: Dict[str, Any]) -> Dict[str, Any]:
    text = segments_data["segment_11"]
    words = text.split()
    return {
        "segment_id": 11,
        "word_count": len(words),
        "avg_word_length": sum(len(w) for w in words) / len(words) if words else 0,
        "sentence_count": len([s for s in text.split('.') if s.strip()])
    }

@DAGTask
def analyze_segment_12(segments_data: Dict[str, Any]) -> Dict[str, Any]:
    text = segments_data["segment_12"]
    words = text.split()
    return {
        "segment_id": 12,
        "word_count": len(words),
        "avg_word_length": sum(len(w) for w in words) / len(words) if words else 0,
        "sentence_count": len([s for s in text.split('.') if s.strip()])
    }

@DAGTask
def analyze_segment_13(segments_data: Dict[str, Any]) -> Dict[str, Any]:
    text = segments_data["segment_13"]
    words = text.split()
    return {
        "segment_id": 13,
        "word_count": len(words),
        "avg_word_length": sum(len(w) for w in words) / len(words) if words else 0,
        "sentence_count": len([s for s in text.split('.') if s.strip()])
    }

@DAGTask
def analyze_segment_14(segments_data: Dict[str, Any]) -> Dict[str, Any]:
    text = segments_data["segment_14"]
    words = text.split()
    return {
        "segment_id": 14,
        "word_count": len(words),
        "avg_word_length": sum(len(w) for w in words) / len(words) if words else 0,
        "sentence_count": len([s for s in text.split('.') if s.strip()])
    }

@DAGTask
def analyze_segment_15(segments_data: Dict[str, Any]) -> Dict[str, Any]:
    text = segments_data["segment_15"]
    words = text.split()
    return {
        "segment_id": 15,
        "word_count": len(words),
        "avg_word_length": sum(len(w) for w in words) / len(words) if words else 0,
        "sentence_count": len([s for s in text.split('.') if s.strip()])
    }

@DAGTask
def analyze_segment_16(segments_data: Dict[str, Any]) -> Dict[str, Any]:
    text = segments_data["segment_16"]
    words = text.split()
    return {
        "segment_id": 16,
        "word_count": len(words),
        "avg_word_length": sum(len(w) for w in words) / len(words) if words else 0,
        "sentence_count": len([s for s in text.split('.') if s.strip()])
    }

# --- Fan-in: Merge segment analyses ---

@DAGTask
def merge_segment_analyses(seg1: Dict[str, Any], seg2: Dict[str, Any], seg3: Dict[str, Any], seg4: Dict[str, Any],
                          seg5: Dict[str, Any], seg6: Dict[str, Any], seg7: Dict[str, Any], seg8: Dict[str, Any],
                          seg9: Dict[str, Any], seg10: Dict[str, Any], seg11: Dict[str, Any], seg12: Dict[str, Any],
                          seg13: Dict[str, Any], seg14: Dict[str, Any], seg15: Dict[str, Any], seg16: Dict[str, Any]) -> Dict[str, Any]:
    """Merge all 16 segment analyses"""
    segments = [seg1, seg2, seg3, seg4, seg5, seg6, seg7, seg8, 
                seg9, seg10, seg11, seg12, seg13, seg14, seg15, seg16]
    
    total_words = sum(s["word_count"] for s in segments)
    avg_word_length = sum(s["avg_word_length"] * s["word_count"] for s in segments) / total_words if total_words > 0 else 0
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
        "detailed_analysis": merged_analysis,
        "processing_complete": True
    }

# --- Define Workflow ---

input_file = "../_inputs/shakespeare.txt"
text = _read_file(input_file)

# Initial fan-out group (3 word count tasks)
wc1 = word_count_chunk(text, 0, 100)
wc2 = word_count_chunk(text, 100, 200)
wc3 = word_count_chunk(text, 200, 300)
total_wc = merge_word_counts([wc1, wc2, wc3])

# Single task that prepares data for middle fan-out
segments_data = create_text_segments(text, total_wc)

# MIDDLE FAN-OUT: Fixed 16-way fan-out for segment analysis
seg1_analysis = analyze_segment_1(segments_data)
seg2_analysis = analyze_segment_2(segments_data)
seg3_analysis = analyze_segment_3(segments_data)
seg4_analysis = analyze_segment_4(segments_data)
seg5_analysis = analyze_segment_5(segments_data)
seg6_analysis = analyze_segment_6(segments_data)
seg7_analysis = analyze_segment_7(segments_data)
seg8_analysis = analyze_segment_8(segments_data)
seg9_analysis = analyze_segment_9(segments_data)
seg10_analysis = analyze_segment_10(segments_data)
seg11_analysis = analyze_segment_11(segments_data)
seg12_analysis = analyze_segment_12(segments_data)
seg13_analysis = analyze_segment_13(segments_data)
seg14_analysis = analyze_segment_14(segments_data)
seg15_analysis = analyze_segment_15(segments_data)
seg16_analysis = analyze_segment_16(segments_data)

# Fan-in: Merge all 16 segment analyses
merged_analysis = merge_segment_analyses(seg1_analysis, seg2_analysis, seg3_analysis, seg4_analysis,
                                       seg5_analysis, seg6_analysis, seg7_analysis, seg8_analysis,
                                       seg9_analysis, seg10_analysis, seg11_analysis, seg12_analysis,
                                       seg13_analysis, seg14_analysis, seg15_analysis, seg16_analysis)

# Create two branches from the merged analysis
metrics = calculate_text_metrics(merged_analysis)
summary = generate_text_summary(merged_analysis)

# Final convergence
final_report = final_comprehensive_report(metrics, summary, merged_analysis)

# --- Run workflow ---
start_time = time.time()
result = final_report.compute(dag_name="text_analysis_middle_fanout", config=WORKER_CONFIG)
print(f"Result keys: {list(result.keys())} | User waited: {time.time() - start_time:.3f}s")
print(f"Analysis complete - processed {result['detailed_analysis']['total_words']} words in {result['detailed_analysis']['total_segments']} segments")
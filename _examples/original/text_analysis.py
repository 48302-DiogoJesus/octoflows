import os
import sys
import time
from typing import List, Dict, Any
import random
from collections import Counter

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
def merge_word_counts(text: str, counts: List[int]) -> tuple[int, str]:
    time.sleep(2)
    return sum(counts), text

# --- Single processing task that creates intermediate result ---

@DAGTask
def create_text_segments(res: tuple[int, str]) -> List[str]:
    """Create 8 text segments for further analysis"""
    _, text = res

    words = text.split()
    segment_size = len(words) // 8

    segments = []
    for i in range(8):
        start_idx = i * segment_size
        end_idx = len(words) if i == 7 else (i + 1) * segment_size
        segments.append(" ".join(words[start_idx:end_idx]))
    return segments

@DAGTask
def compute_text_statistics(res: tuple[int, str]) -> Dict[str, Any]:
    """Heavy computational task - compute comprehensive text statistics"""
    import time
    
    _, text = res

    # Simulate heavy computation with simple but time-consuming operations
    words = text.split()
    
    # Simplified character counting (only vowels and consonants)
    vowel_count = 0
    consonant_count = 0
    for char in text.lower():
        if char in 'aeiou':
            vowel_count += 1
        elif char.isalpha():
            consonant_count += 1
    
    # Word length analysis with reduced artificial delay
    word_lengths = []
    for i, word in enumerate(words):
        clean_word = word.strip('.,!?;:"()')
        word_lengths.append(len(clean_word))
        # Reduced delay - only every 2000 words instead of 1000
        if i % 2000 == 0 and i > 0:
            time.sleep(0.001)  # 1ms delay every 2000 words
    
    # Simple sentence counting
    sentence_count = text.count('.')
    
    # Basic word frequency for top 10 words only (reduced from 20)
    word_freq = {}
    for word in words:
        clean_word = word.lower().strip('.,!?;:"()')
        if len(clean_word) > 4:  # Only count words longer than 4 chars (reduced work)
            word_freq[clean_word] = word_freq.get(clean_word, 0) + 1
    
    # Get top 10 most common words only
    most_common = sorted(word_freq.items(), key=lambda x: x[1], reverse=True)[:10]
    
    # Basic calculations
    avg_word_length = sum(word_lengths) / len(word_lengths) if word_lengths else 0
    avg_sentence_length = len(words) / sentence_count if sentence_count > 0 else 0
    simple_readability = avg_word_length + (avg_sentence_length / 10)
    
    return {
        "vowel_count": vowel_count,
        "consonant_count": consonant_count,
        "avg_word_length": avg_word_length,
        "sentence_count": sentence_count,
        "avg_sentence_length": avg_sentence_length,
        "most_common_words": most_common,
        "unique_word_count": len(word_freq),
        "simple_readability_score": simple_readability,
        "processing_time_simulation": "Heavy computation completed"
    }


@DAGTask
def analyze_segment(segments: List[str], segment_id: int) -> Dict[str, Any]:
    """Analyze a specific segment"""
    text = segments[segment_id]
    words = text.split()
    sentences = [s.strip() for s in text.split('.') if s.strip()]
    
    return {
        "segment_id": segment_id,
        "text": text,
        "word_count": len(words),
        "avg_word_length": sum(len(w) for w in words) / len(words) if words else 0,
        "sentence_count": len(sentences),
        "unique_words": len(set(word.lower().strip('.,!?;:"()') for word in words))
    }

# --- New processing functions that work on the overall segments data ---

@DAGTask  
def extract_overall_keywords(segments: List[str], text_stats: Dict[str, Any]) -> Dict[str, Any]:
    MAX_SAMPLE_SIZE = 50
    MAX_WORDS_PER_SEGMENT = 100
    
    if len(segments) > MAX_SAMPLE_SIZE:
        sample_segments = random.sample(segments, MAX_SAMPLE_SIZE)
    else:
        sample_segments = segments
    
    common_words = {word for word, _ in text_stats["most_common_words"][:50]}  # Use fewer common words
    keyword_counter = Counter()
    total_processed = 0
    
    for segment in sample_segments:
        words = segment.lower().split()[:MAX_WORDS_PER_SEGMENT]  # Limit words per segment
        total_processed += len(words)
        
        for word in words:
            clean_word = word.strip('.,!?;:"()')
            if 5 < len(clean_word) < 20 and clean_word not in common_words:  # Add max length limit
                keyword_counter[clean_word] += 1
    
    return {
        "type": "overall_keywords", 
        "top_keywords": keyword_counter.most_common(10),  # Return fewer keywords
        "total_keywords": len(keyword_counter),
        "keyword_density": len(keyword_counter) / total_processed if total_processed > 0 else 0,
        "avg_word_length_context": text_stats["avg_word_length"],
        "is_sample": True,
        "sample_size": len(sample_segments)
    }

@DAGTask
def analyze_overall_punctuation(segments: List[str], text_stats: Dict[str, Any]) -> Dict[str, Any]:
    """Analyze punctuation patterns across all segments, enhanced with character frequency data"""
    all_text = " ".join(segments)
    
    punctuation_counts = {
        "periods": all_text.count('.'),
        "commas": all_text.count(','),
        "exclamations": all_text.count('!'),
        "questions": all_text.count('?'),
        "semicolons": all_text.count(';'),
        "colons": all_text.count(':'),
        "quotations": all_text.count('"') + all_text.count("'")
    }
    
    total_punct = sum(punctuation_counts.values())
    
    return {
        "type": "overall_punctuation",
        "punctuation_counts": punctuation_counts,
        "total_punctuation": total_punct,
        "punctuation_density": total_punct / len(all_text) if all_text else 0,
        "most_common_punct": max(punctuation_counts.items(), key=lambda x: x[1])[0] if punctuation_counts else "none",
        "vowel_context": text_stats["vowel_count"],
        "consonant_context": text_stats["consonant_count"]
    }

@DAGTask
def calculate_overall_readability(segments: List[str], text_stats: Dict[str, Any]) -> Dict[str, Any]:
    """Calculate readability metrics for the entire text, enhanced with statistics"""
    all_text = " ".join(segments)
    
    # Use pre-computed statistics for enhanced readability
    enhanced_score = text_stats["simple_readability_score"] + (text_stats["avg_sentence_length"] * 0.1)
    
    return {
        "type": "overall_readability",
        "simple_readability_score": text_stats["simple_readability_score"],
        "enhanced_readability_score": enhanced_score,
        "avg_word_length": text_stats["avg_word_length"],
        "avg_sentence_length": text_stats["avg_sentence_length"],
        "complexity_level": "simple" if enhanced_score < 8 else "medium" if enhanced_score < 12 else "complex",
        "sentence_context": text_stats["sentence_count"]
    }

@DAGTask
def detect_overall_patterns(segments: List[str], text_stats: Dict[str, Any]) -> Dict[str, Any]:
    """Detect linguistic patterns across all segments, enhanced with statistics"""
    all_text = " ".join(segments)
    words = all_text.split()
    
    patterns = {
        "total_words": len(words),
        "unique_words": text_stats["unique_word_count"],
        "avg_word_length": text_stats["avg_word_length"],
        "segment_count": len(segments),
        "vowel_count": text_stats["vowel_count"]
    }
    
    vocabulary_richness = patterns["unique_words"] / patterns["total_words"] if patterns["total_words"] > 0 else 0
    
    return {
        "type": "overall_patterns",
        "patterns": patterns,
        "vocabulary_richness": vocabulary_richness,
        "lexical_diversity": "high" if vocabulary_richness > 0.7 else "medium" if vocabulary_richness > 0.4 else "low",
        "readability_context": text_stats["simple_readability_score"]
    }

# --- Updated merge function to handle all 20 results (8 segments + 12 processing results) ---

@DAGTask
def merge_segment_analyses(segment_analyses: List[Dict[str, Any]], 
                          overall_keywords: Dict[str, Any],
                          overall_punctuation: Dict[str, Any], 
                          overall_readability: Dict[str, Any], 
                          overall_patterns: Dict[str, Any]) -> Dict[str, Any]:
    """Merge all 12 analyses: 8 segments + 4 overall processing results"""
    
    # Aggregate segment data
    total_words = sum(s["word_count"] for s in segment_analyses)
    total_sentences = sum(s["sentence_count"] for s in segment_analyses)
    avg_word_length = (
        sum(s["avg_word_length"] * s["word_count"] for s in segment_analyses) / total_words
        if total_words > 0 else 0
    )
    total_unique_words = sum(s["unique_words"] for s in segment_analyses)
    
    return {
        "total_segments": len(segment_analyses),
        "total_words": total_words,
        "total_sentences": total_sentences,
        "overall_avg_word_length": avg_word_length,
        "total_unique_words": total_unique_words,
        
        # Data from the 4 overall processing functions
        "keywords_analysis": overall_keywords,
        "punctuation_analysis": overall_punctuation,
        "readability_analysis": overall_readability,
        "patterns_analysis": overall_patterns,
        
        # Detailed segment data
        "segment_details": segment_analyses
    }

# --- Additional processing tasks (creating another branch) ---

@DAGTask
def calculate_text_metrics(merged_analysis: Dict[str, Any]) -> Dict[str, Any]:
    """Calculate additional text metrics"""
    return {
        "words_per_sentence": merged_analysis["total_words"] / merged_analysis["total_sentences"] if merged_analysis["total_sentences"] > 0 else 0,
        "vocabulary_richness": merged_analysis["patterns_analysis"]["vocabulary_richness"],
        "complexity_score": merged_analysis["overall_avg_word_length"] * merged_analysis["total_sentences"] / 100,
        "keyword_density": merged_analysis["keywords_analysis"]["keyword_density"]
    }

@DAGTask
def generate_text_summary(merged_analysis: Dict[str, Any]) -> Dict[str, Any]:
    """Generate a summary of the text"""
    return {
        "summary": f"Text contains {merged_analysis['total_words']} words across {merged_analysis['total_segments']} segments",
        "readability": merged_analysis["readability_analysis"]["complexity_level"],
        "lexical_diversity": merged_analysis["patterns_analysis"]["lexical_diversity"],
        "top_keywords": merged_analysis["keywords_analysis"]["top_keywords"][:5],
        "punctuation_style": merged_analysis["punctuation_analysis"]["most_common_punct"]
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

# Initial fan-out group (word count tasks)
chunk_size = 200
num_chunks = 10
word_counts = []

for i in range(num_chunks):
    start = i * chunk_size
    end = (i + 1) * chunk_size
    wc = word_count_chunk(text, start, end)
    word_counts.append(wc)

merge_wc_result = merge_word_counts(text, word_counts)

# Single task that prepares data for middle fan-out
segments_data = create_text_segments(merge_wc_result)

# Heavy computational task that also fans out from merge_word_counts
text_statistics = compute_text_statistics(merge_wc_result)

# FAN-OUT: Generate 8 segment analyses + 4 direct processing functions (12 total tasks)
segment_analyses = [analyze_segment(segments_data, i) for i in range(8)]

# 4 additional processing functions that work on segments data + text statistics
overall_keywords = extract_overall_keywords(segments_data, text_statistics)
overall_punctuation = analyze_overall_punctuation(segments_data, text_statistics)
overall_readability = calculate_overall_readability(segments_data, text_statistics)
overall_patterns = detect_overall_patterns(segments_data, text_statistics)

# Fan-in: Merge all 12 analyses (8 segments + 4 overall processing results)
merged_analysis = merge_segment_analyses(
    segment_analyses, 
    overall_keywords, 
    overall_punctuation, 
    overall_readability, 
    overall_patterns
)

# Create two branches from the merged analysis
metrics = calculate_text_metrics(merged_analysis)
summary = generate_text_summary(merged_analysis)

final_report = final_comprehensive_report(metrics, summary, merged_analysis)
# final_report.visualize_dag(open_after=True)
# exit()

# --- Run workflow ---
start_time = time.time()
result = final_report.compute(dag_name="text_analysis", config=WORKER_CONFIG, open_dashboard=False)
print(f"Result keys: {list(result.keys())} | User waited: {time.time() - start_time:.3f}s")
print(f"Analysis complete - processed {result['detailed_analysis']['total_words']} words in {result['detailed_analysis']['total_segments']} segments")
print(f"Found {result['detailed_analysis']['keywords_analysis']['total_keywords']} unique keywords")
print(f"Overall readability: {result['detailed_analysis']['readability_analysis']['complexity_level']}")
print(f"Vocabulary richness: {result['detailed_analysis']['patterns_analysis']['lexical_diversity']}")
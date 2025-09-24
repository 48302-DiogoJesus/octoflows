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
    """Create 8 text segments for further analysis"""
    words = text.split()
    segment_size = len(words) // 8

    segments = []
    for i in range(8):
        start_idx = i * segment_size
        end_idx = len(words) if i == 7 else (i + 1) * segment_size
        segments.append(" ".join(words[start_idx:end_idx]))
    return segments

@DAGTask
def compute_text_statistics(text: str, total_word_count: int) -> Dict[str, Any]:
    """Heavy computational task - compute comprehensive text statistics"""
    import re
    from collections import Counter
    
    # Simulate heavy computation with detailed analysis
    words = text.split()
    
    # Character-level analysis
    char_freq = Counter(text.lower())
    vowels = sum(char_freq[v] for v in 'aeiou')
    consonants = sum(char_freq[c] for c in 'bcdfghjklmnpqrstvwxyz')
    
    # Word length distribution
    word_lengths = [len(w.strip('.,!?;:"()')) for w in words]
    length_distribution = Counter(word_lengths)
    
    # Sentence analysis (heavy regex processing)
    sentences = re.split(r'[.!?]+', text)
    sentences = [s.strip() for s in sentences if s.strip()]
    
    # Word frequency analysis (computationally intensive)
    clean_words = [w.lower().strip('.,!?;:"()') for w in words]
    word_freq = Counter(clean_words)
    
    # Language pattern analysis
    capitalized_words = [w for w in words if w[0].isupper() and len(w) > 1]
    
    # Syllable estimation (heavy computation)
    def estimate_syllables(word):
        word = word.lower()
        vowel_groups = re.findall(r'[aeiouy]+', word)
        syllables = len(vowel_groups)
        if word.endswith('e') and syllables > 1:
            syllables -= 1
        return max(1, syllables)
    
    total_syllables = sum(estimate_syllables(w.strip('.,!?;:"()')) for w in words)
    
    # Advanced readability metrics
    flesch_reading_ease = 206.835 - (1.015 * (len(words) / len(sentences))) - (84.6 * (total_syllables / len(words))) if sentences and words else 0
    
    return {
        "char_frequency": dict(char_freq.most_common(10)),
        "vowel_count": vowels,
        "consonant_count": consonants,
        "word_length_distribution": dict(length_distribution),
        "avg_word_length": sum(word_lengths) / len(word_lengths) if word_lengths else 0,
        "sentence_count": len(sentences),
        "avg_sentence_length": len(words) / len(sentences) if sentences else 0,
        "most_common_words": word_freq.most_common(20),
        "hapax_legomena": len([w for w, count in word_freq.items() if count == 1]),
        "vocabulary_size": len(word_freq),
        "capitalized_word_count": len(capitalized_words),
        "total_syllables": total_syllables,
        "flesch_reading_ease": flesch_reading_ease,
        "type_token_ratio": len(word_freq) / len(clean_words) if clean_words else 0
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
    """Extract keywords from all segments combined, enhanced with text statistics"""
    all_text = " ".join(segments)
    words = [w.lower().strip('.,!?;:"()') for w in all_text.split()]
    
    # Use text statistics to inform keyword extraction
    common_words = {word for word, _ in text_stats["most_common_words"][:50]}
    
    # Enhanced keyword extraction using statistical data
    keywords = []
    for w in words:
        if (len(w) > 5 and w not in common_words and 
            text_stats["word_length_distribution"].get(len(w), 0) < len(words) * 0.1):
            keywords.append(w)
    
    keyword_freq = {}
    for word in keywords:
        keyword_freq[word] = keyword_freq.get(word, 0) + 1
    
    # Get top keywords by frequency, filtered by statistical significance
    sorted_keywords = sorted(keyword_freq.items(), key=lambda x: x[1], reverse=True)
    
    return {
        "type": "overall_keywords",
        "top_keywords": sorted_keywords[:20],
        "total_keywords": len(keyword_freq),
        "keyword_density": len(keywords) / len(words) if words else 0,
        "statistical_enhancement": f"Filtered using {len(common_words)} common words from text statistics"
    }

@DAGTask
def analyze_overall_punctuation(segments: List[str], text_stats: Dict[str, Any]) -> Dict[str, Any]:
    """Analyze punctuation patterns across all segments, enhanced with character frequency data"""
    all_text = " ".join(segments)
    
    # Enhanced punctuation analysis using character frequency data
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
    
    # Use character frequency data for additional insights
    total_chars = sum(text_stats["char_frequency"].values()) if text_stats["char_frequency"] else len(all_text)
    
    return {
        "type": "overall_punctuation",
        "punctuation_counts": punctuation_counts,
        "total_punctuation": total_punct,
        "punctuation_density": total_punct / len(all_text) if all_text else 0,
        "punctuation_to_char_ratio": total_punct / total_chars if total_chars > 0 else 0,
        "most_common_punct": max(punctuation_counts.items(), key=lambda x: x[1])[0] if punctuation_counts else "none",
        "statistical_enhancement": f"Enhanced with character frequency analysis from {len(text_stats['char_frequency'])} character types"
    }

@DAGTask
def calculate_overall_readability(segments: List[str], text_stats: Dict[str, Any]) -> Dict[str, Any]:
    """Calculate readability metrics for the entire text, enhanced with advanced statistics"""
    all_text = " ".join(segments)
    words = all_text.split()
    
    # Use pre-computed statistics for enhanced readability calculation
    flesch_score = text_stats["flesch_reading_ease"]
    type_token_ratio = text_stats["type_token_ratio"]
    avg_syllables_per_word = text_stats["total_syllables"] / len(words) if words else 0
    
    # Enhanced readability metrics
    enhanced_readability_score = (
        text_stats["avg_word_length"] * 1.5 +
        text_stats["avg_sentence_length"] * 0.3 +
        avg_syllables_per_word * 2.0 +
        (1 - type_token_ratio) * 5.0  # Lower vocabulary diversity increases complexity
    )
    
    return {
        "type": "overall_readability",
        "flesch_reading_ease": flesch_score,
        "enhanced_readability_score": enhanced_readability_score,
        "type_token_ratio": type_token_ratio,
        "avg_syllables_per_word": avg_syllables_per_word,
        "complexity_level": "simple" if enhanced_readability_score < 8 else "medium" if enhanced_readability_score < 12 else "complex",
        "flesch_level": "very_easy" if flesch_score >= 90 else "easy" if flesch_score >= 80 else "fairly_easy" if flesch_score >= 70 else "standard" if flesch_score >= 60 else "fairly_difficult" if flesch_score >= 50 else "difficult",
        "statistical_enhancement": f"Enhanced with syllable count ({text_stats['total_syllables']}) and vocabulary analysis"
    }

@DAGTask
def detect_overall_patterns(segments: List[str], text_stats: Dict[str, Any]) -> Dict[str, Any]:
    """Detect linguistic patterns across all segments, enhanced with comprehensive statistics"""
    all_text = " ".join(segments)
    words = all_text.split()
    
    # Enhanced pattern detection using pre-computed statistics
    patterns = {
        "total_words": len(words),
        "unique_words": text_stats["vocabulary_size"],
        "hapax_legomena": text_stats["hapax_legomena"],  # Words that appear only once
        "avg_word_length": text_stats["avg_word_length"],
        "long_words": sum(count for length, count in text_stats["word_length_distribution"].items() if length > 7),
        "short_words": sum(count for length, count in text_stats["word_length_distribution"].items() if 1 <= length <= 3),
        "segment_count": len(segments),
        "vowel_consonant_ratio": text_stats["vowel_count"] / text_stats["consonant_count"] if text_stats["consonant_count"] > 0 else 0,
        "capitalized_words": text_stats["capitalized_word_count"]
    }
    
    # Advanced vocabulary richness using multiple metrics
    vocabulary_richness = text_stats["type_token_ratio"]
    lexical_sophistication = patterns["long_words"] / patterns["total_words"] if patterns["total_words"] > 0 else 0
    
    return {
        "type": "overall_patterns",
        "patterns": patterns,
        "vocabulary_richness": vocabulary_richness,
        "lexical_sophistication": lexical_sophistication,
        "lexical_diversity": "high" if vocabulary_richness > 0.7 else "medium" if vocabulary_richness > 0.4 else "low",
        "writing_complexity": "sophisticated" if lexical_sophistication > 0.15 else "moderate" if lexical_sophistication > 0.08 else "simple",
        "statistical_enhancement": f"Enhanced with {len(text_stats['word_length_distribution'])} word length categories and hapax legomena analysis"
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

total_wc = merge_word_counts(word_counts)

# Single task that prepares data for middle fan-out
segments_data = create_text_segments(text, total_wc)

# Heavy computational task that also fans out from merge_word_counts
text_statistics = compute_text_statistics(text, total_wc)

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

# --- Run workflow ---
start_time = time.time()
result = final_report.compute(dag_name="text_analysis", config=WORKER_CONFIG)
print(f"Result keys: {list(result.keys())} | User waited: {time.time() - start_time:.3f}s")
print(f"Analysis complete - processed {result['detailed_analysis']['total_words']} words in {result['detailed_analysis']['total_segments']} segments")
print(f"Found {result['detailed_analysis']['keywords_analysis']['total_keywords']} unique keywords")
print(f"Overall readability: {result['detailed_analysis']['readability_analysis']['complexity_level']}")
print(f"Vocabulary richness: {result['detailed_analysis']['patterns_analysis']['lexical_diversity']}")
# Automated Evals - Recommendations for the Development Team

These evals can be implemented by the development team without involving testers. They complement the human evals and measure objective aspects of system quality.

---

## 1. Red Team Pass Rate (already present in code)

### What already exists
Both **MFI Drafter** and **Market Monitor** include a `red_team` node that performs automated fact-checking on generated output. The red team produces `SkepticFlag` entries with issue_type and severity.

### Eval to implement

**Metric**: Rate of high-severity flags per run

```python
# Pseudo-code
red_team_flags = result["skeptic_flags"]
high_severity = [f for f in red_team_flags if f["severity"] == "high"]
pass_rate = 1 if len(high_severity) == 0 else 0
```

**What to track**:
- % of runs with zero "high" severity flags (target: >80%)
- Distribution of the most frequent issue types (to prioritize fixes)
- Trend over time (does it improve after prompt updates?)

**Issue types to monitor by agent**:

| MFI Drafter | Market Monitor |
|-------------|----------------|
| `score_mismatch` | `numeracy_error` |
| `interpretation_error` | `unsupported_speculation` |
| `missing_content` | `context_data_conflict` |
| | `terminology_misuse` |
| | `internal_contradiction` |

**Implementation**: Add structured logging to the `red_team` node that writes flags in an analyzable format (JSON lines in a file, or a dedicated field in `RunRecord`).

---

## 2. Numeracy Accuracy (Market Monitor)

### What can be checked
Market Monitor computes statistics (MoM%, YoY%, food basket cost) from the CSV and passes them to the LLM narrative. We can verify whether numbers in generated text match the data.

### Eval to implement

**Metric**: Rate of numeric errors in generated text

```python
# Pseudo-code
data_stats = result["data_statistics"]  # ground truth
report_text = result["report_sections"]["HIGHLIGHTS"]

# Extract numbers from text with regex
numbers_in_text = extract_numbers(report_text)

# Compare with ground truth
for number in numbers_in_text:
    if not matches_any_stat(number, data_stats, tolerance=0.5):
        flag_numeracy_error(number)
```

**What to track**:
- % of runs with zero numeric errors (target: >90%)
- Most frequent error type (MoM% vs YoY% vs absolute price)

**Note**: Partially covered by the red team, but a deterministic check (regex + numeric comparison) is more reliable than an LLM grading itself.

---

## 3. Terminology Compliance (Market Monitor)

### What already exists
The system has hardcoded `TERMINOLOGY_THRESHOLDS` (hyperinflation: monthly >50%, severe_depreciation: YoY >30%, etc.) and a validation in the highlights node that produces warnings if drivers contradict exchange-rate data.

### Eval to implement

**Metric**: Terminology violation rate

```python
# Pseudo-code
thresholds = TERMINOLOGY_THRESHOLDS
report_text = full_report_text

for term, criteria in thresholds.items():
    if term_appears_in_text(term, report_text):
        if not data_meets_threshold(data_stats, criteria):
            flag_terminology_violation(term, criteria, actual_values)
```

**What to track**:
- Number of terminology violations per run (target: 0)
- Most frequently violated term (signals where prompts are not restrictive enough)

**Implementation**: Create a deterministic (non-LLM) function that scans the final text for threshold terms and validates against quantitative data. Run post-generation.

---

## 4. Sub-Score Inversion Detection (MFI Drafter)

### The problem
MFI Drafter prompts explicitly warn that sub-scores (e.g., `scarce_cereals_pct=0.80`) must be interpreted as "80% scarcity" (negative), not "80% availability" (positive). This is a known error documented in the prompts.

### Eval to implement

**Metric**: Rate of inversions in findings

```python
# Pseudo-code
for dimension in ["Availability", "Price"]:
    sub_scores = get_sub_scores(dimension)
    findings_text = result["dimension_findings"][dimension]["key_findings"]

    for score_name, score_value in sub_scores.items():
        if is_negative_indicator(score_name):  # scarce_*, runout_*, increase_*
            if score_value > 0.5 and text_describes_positive(findings_text, score_name):
                flag_inversion(dimension, score_name, score_value)
```

**What to track**:
- % of runs with inversions (target: 0%)
- Most problematic dimensions and sub-scores

**Implementation**: Requires lightweight NLP or a dedicated LLM grader that compares the finding text with the numeric value and expected direction.

---

## 5. Product Classification Accuracy (Price Validator)

### What can be checked
The Price Validator classifies products against the WFP list (~200 products) and assigns a 0-1 confidence score. Classifications with confidence < 0.5 are already flagged as "low_confidence_matches".

### Eval to implement

**Metric**: Confidence distribution and classification rate

```python
# Pseudo-code
classifications = result["product_classifications"]
total = len(classifications)
high_conf = len([c for c in classifications if c["confidence"] >= 0.8])
low_conf = len([c for c in classifications if c["confidence"] < 0.5])
unmatched = len([c for c in classifications if c["matched_name"] is None])

metrics = {
   "classification_rate": (total - unmatched) / total,
   "high_confidence_rate": high_conf / total,
   "low_confidence_rate": low_conf / total,
}
```

**What to track**:
- % of products classified with confidence >= 0.8 (target: >70%)
- % of products not classified (target: <10%)
- Most frequently misclassified products (to enrich the dictionary)

**Implementation**: Almost all data is already present in the code. Aggregate across runs.

---

## 6. Context Retrieval Quality (MFI Drafter and Market Monitor)

### What can be checked
Both drafters retrieve context from Seerist and ReliefWeb. The MFI Drafter code already filters "disclaimer" responses ("cannot be extracted", "insufficient information"). We can measure retrieval usefulness.

### Eval to implement

**Metric**: Useful retrieval rate

```python
# Pseudo-code
docs_retrieved = result["metadata"]["context_counts"]  # e.g., {"Seerist": 45, "ReliefWeb": 12}
context_extracted = result["country_context"]  # extracted text

metrics = {
   "docs_retrieved_total": docs_retrieved["Seerist"] + docs_retrieved["ReliefWeb"],
   "context_non_empty": len(context_extracted.strip()) > 0,
   "context_is_disclaimer": is_disclaimer(context_extracted),  # uses filters already in code
}
```

**What to track**:
- % of runs with non-empty, non-disclaimer context (target: >80%)
- Average number of documents retrieved per country
- Countries where retrieval consistently fails

---

## 7. Performance Metrics (all agents)

### What can be measured with no extra effort

The async system already tracks `created_at`, `updated_at`, `progress_pct`, and `current_node`.

### Eval to implement

**Metrics**:

```python
metrics = {
   "total_duration_seconds": updated_at - created_at,
   "llm_call_count": result.get("llm_call_count", 0),
   "node_durations": {node: duration for each node},  # requires extra logging
}
```

**What to track**:
- Average run duration per agent (for infrastructure sizing)
- Distribution: % of runs that complete in < 2 min, < 5 min, < 10 min
- Failure rate (runs with status "failed") per agent
- Slowest node (for optimization)

**Implementation**: Add timestamps for each node transition in `on_step_callback`. Minimal development effort.

---

## 8. Validation Layer Pass Rates (Validators)

### What can be measured

Validators produce results per layer (0-4). These can be aggregated.

### Eval to implement

```python
for layer in result["layer_results"]:
    layer_name = layer["layer"]
    passed = layer["passed"]
    issues_count = len(layer.get("issues", []))

    # Aggregate across runs
    layer_pass_rates[layer_name].append(passed)
    layer_issue_counts[layer_name].append(issues_count)
```

**What to track**:
- Pass rate per layer (which layers fail most often)
- Average issue count per layer

---

## Implementation priority

| # | Eval | Effort | Value | Priority |
|---|------|--------|--------|-----------|
| 1 | Red Team Pass Rate | Low (logging) | High | **P0** |
| 2 | Performance Metrics | Low (timestamps) | High | **P0** |
| 3 | Terminology Compliance | Medium (regex+data) | High | **P1** |
| 4 | Numeracy Accuracy | Medium (regex+comparison) | High | **P1** |
| 5 | Product Classification Stats | Low (aggregation) | Medium | **P1** |
| 6 | Sub-Score Inversion | High (NLP/LLM grader) | High | **P2** |
| 7 | Context Retrieval Quality | Low (counts) | Medium | **P2** |
| 8 | Validation Layer Pass Rates | Low (aggregation) | Medium | **P2** |

**P0**: Implement before the alpha test (helps interpret results)
**P1**: Implement during the alpha test (complements human evals)
**P2**: Implement after the alpha test (for continuous monitoring)

---

## Suggested infrastructure

For all these evals, two components are needed:

1. **Structured logging**: Each run should produce a JSON record with all required data (timings, red team flags, classification metrics, counts). Modify `RunRecord` or create a separate log.

2. **Aggregation dashboard**: A Google Sheet, Looker Studio, or Python script that reads the logs and calculates aggregated metrics. No enterprise platform is needed - a Jupyter notebook or script producing a metrics CSV is sufficient for alpha.

# Time Savings Methodology

---

## 1. Selected approach: Paired Comparison with Retrospective Baseline

### Why this approach

We evaluated 4 methods and chose a combination of two:

| Method | Pros | Cons | Decision |
|--------|------|------|----------|
| **Diary method** (log every task) | Granular data | Too burdensome for testers | Rejected as primary method |
| **Paired comparison** (same task with and without the tool) | Gold standard | Requires double work, unrealistic for 1 month | Used in a light version |
| **Retrospective baseline** (manual time estimate pre-test) | Simple, zero overhead | Self-report bias, imprecise estimates | Used as a complement |
| **Task-specific benchmarks** (timed predefined tasks) | Controlled | Artificial, not representative | Rejected |

**Final approach**: Combination of (A) a retrospective baseline collected pre-test + (B) a quick post-task log that captures actual time with the tool and an estimated equivalent manual time.

This combination balances:
- **Practicality**: the post-task log takes 1-2 minutes
- **Credibility**: the "with tool" estimate is observed (not retrospective), while the "without tool" estimate is informed by the immediate task experience
- **Comparability**: we have both a general baseline (pre-test) and task-specific comparisons

---

## 2. Phase 1: Baseline Collection (pre-test, during onboarding)

### "Manual Time Baseline" template

To be completed ONCE during onboarding. Google Form or the first section of a Google Sheet.

| Question | Type | Options |
|---------|------|---------|
| Name/Initials | Text | - |
| **How long do you normally take to validate an MFI dataset manually?** | Choice | < 30 min / 30-60 min / 1-2 hours / 2-4 hours / > 4 hours / I do not do this task |
| **How long do you take to validate and classify a price dataset manually?** | Choice | < 30 min / 30-60 min / 1-2 hours / 2-4 hours / > 4 hours / I do not do this task |
| **How long do you take to write a full MFI report?** | Choice | < 1 hour / 1-2 hours / 2-4 hours / 4-8 hours / > 1 day / I do not do this task |
| **How long do you take to write a full price bulletin?** | Choice | < 1 hour / 1-2 hours / 2-4 hours / 4-8 hours / > 1 day / I do not do this task |
| **How often do you perform each of these tasks?** (for each) | Choice | Weekly / Bi-weekly / Monthly / Quarterly / Rarely |

---

## 3. Phase 2: Post-Task Time Tracking (continuous)

### Google Sheet template: "VAM LLM Time Tracker"

Each tester has a personal tab (or uses a Google Form that feeds a shared sheet).

**Tester instructions**: "After each use of the tool, fill in one row. It takes less than 2 minutes."

#### Sheet columns

| Column | Type | Description | Example |
|---------|------|-------------|---------|
| **Date** | Date | When you used the tool | 2025-06-15 |
| **Tester** | Text | Your initials | MR |
| **Agent** | Dropdown | Which agent you used | MFI Drafter |
| **Country** | Text | Country of analyzed data | Sudan |
| **Task** | Dropdown | What you did | Generate full report |
| **Time with tool** | Dropdown | Total time (including upload, waiting, checking output, correcting) | 30-45 min |
| **Estimated time without tool** | Dropdown | How long the same task would take manually, in your experience | 2-4 hours |
| **Output usable?** | Dropdown | The output was... | Usable with small edits |
| **Review time** | Dropdown | Time spent reviewing/fixing output | < 15 min |
| **Notes** | Free text | Any useful detail | Large file (2000 rows), first use |

#### Dropdown options

**Agent**:
- MFI Validator
- Price Validator
- MFI Drafter
- Market Monitor

**Task** (depends on agent):
- For Validators: Validate a dataset / Validate and correct a dataset
- For Drafters: Generate full report / Generate specific sections / Review and finalize generated report

**Time with tool**:
- < 5 min
- 5-15 min
- 15-30 min
- 30-45 min
- 45-60 min
- 1-2 hours
- > 2 hours

**Estimated time without tool** (same options +):
- < 30 min
- 30-60 min
- 1-2 hours
- 2-4 hours
- 4-8 hours
- > 1 day

**Output usable?**:
- Yes, without edits
- Yes, with small edits (< 15 min)
- Yes, with significant edits (15-30 min)
- Requires substantial rewrite (> 30 min)
- Not usable

**Review time**:
- No review needed
- < 15 min
- 15-30 min
- 30-60 min
- > 1 hour

---

## 4. Metrics to calculate at the end of the test

### Primary metrics

#### 4.1 Time savings per task (%)

```
Savings % = (Manual_time - Tool_time) / Manual_time * 100
```

Where:
- `Tool_time` = actual time (including review)
- `Manual_time` = "without tool" estimate provided by the tester

Calculate:
- **Average** per agent (with 95% confidence interval)
- **Median** per agent (more robust to outliers)
- **Distribution** (% of tasks with savings >50%, >30%, <0%)

#### 4.2 Total effective time with the tool (including review)

```
Total_time = Tool_time + Review_time
```

This is the most honest figure: it includes the cost of verifying and correcting output.

#### 4.3 Quality/time ratio

Cross savings with output quality:

| Output usable without edits | Net savings = Full savings |
| Output with small edits | Net savings = Savings - editing time |
| Output with significant edits | Net savings = Savings - editing time (potentially negative) |
| Not usable | Net savings = negative (time lost) |

#### 4.4 Annualized estimate

```
Annual_savings_per_analyst = Average_savings_per_task * Annual_task_frequency
```

Where `Annual_task_frequency` comes from the pre-test baseline (e.g., "monthly" = 12 times/year).

Calculate per agent and sum.

### Secondary metrics

#### 4.5 Learning curve

Compare `Tool_time` for the first 3 sessions of each tester vs the last 3.
If there is a significant improvement, the learning effect is relevant for future projections.

#### 4.6 Actual usage rate

```
Rate = Recorded_sessions / Estimated_relevant_tasks
```

If testers have relevant tasks but do not use the tool, this is a signal.

---

## 5. Google Sheet template: full structure

### Tab 1: "Baseline" (filled once)
| Tester | Location | Years of experience | MFI validation time | Price validation time | MFI report time | Price bulletin time | MFI validation frequency | Price validation frequency | MFI report frequency | Price bulletin frequency |
|--------|----------|---------------------|---------------------|------------------------|-----------------|---------------------|--------------------------|---------------------------|---------------------|-------------------------|
| MR | HQ Rome | 3-5 years | 1-2 hours | 30-60 min | 4-8 hours | 2-4 hours | Monthly | Weekly | Monthly | Monthly |

### Tab 2: "Time Log" (filled after each use)
| Date | Tester | Agent | Country | Task | Time with tool | Time without tool (estimate) | Output usable? | Review time | Notes |
|------|--------|-------|---------|------|----------------|-----------------------------|---------------|-------------|------|
| 2025-06-15 | MR | MFI Drafter | Sudan | Generate report | 30-45 min | 4-8 hours | With small edits | < 15 min | - |

### Tab 3: "Dashboard" (automatic formulas)
Create a tab with formulas that automatically calculate:
- Total number of sessions per agent
- Average and median savings % per agent
- Distribution of output quality per agent
- Time trend (savings in the first 2 weeks vs last 2)

---

## 6. Methodological disclaimers

These disclaimers should be included in the final report. They matter for intellectual honesty and to avoid overestimation.

### 6.1 Self-report bias
"Time without tool" estimates are self-reported and tend to overestimate manual time (and therefore savings). Testers may unconsciously inflate savings to "justify" the project. **Mitigation**: the pre-test baseline (collected before using the tool) provides an independent reference. If post-task estimates diverge significantly from the baseline, flag the discrepancy.

### 6.2 Novelty effect
In the first weeks, testers may be more patient with the tool (enthusiasm) or slower (learning). The realistic data is from weeks 3-4. **Mitigation**: analyze data by week and report the trend.

### 6.3 Learning curve
Time with the tool includes the learning curve. With regular use, times should improve. Annualized projections based on 1 month of testing underestimate steady-state savings. **Mitigation**: use the last 2 weeks of data for projections.

### 6.4 Task selection bias
Testers might use the tool only for "easy" tasks where it works well, avoiding complex ones. This overstates average savings. **Mitigation**: cross-check with usage frequency and ask in the survey if there are tasks they chose NOT to use the tool for.

### 6.5 Uncaptured quality
Time savings alone does not capture whether output quality is higher, equal, or lower than manual work. A report generated in 30 minutes but with lower quality is not a true saving. **Mitigation**: always cross time data with quality data ("Output usable?" field and holistic survey).

### 6.6 Small sample size
With 10-20 testers and 1 month of testing, the sample is not statistically significant for definitive conclusions. Results are indicative, not final. **Mitigation**: present data with confidence intervals and use it as a directional signal, not proof.

---

## 7. Final time savings report format

The final report should include:

```
1. Summary
   - Overall average savings: X% (range: Y%-Z%)
   - Agent with highest savings: [name] (X%)
   - Agent with lowest savings: [name] (X%)

2. Per agent
   For each of the 4 agents:
   - Number of recorded sessions
   - Average savings (with 95% CI)
   - Median savings
   - Output quality distribution (% per category)
   - Average time with tool (including review)
   - Time trend (weeks 1-2 vs 3-4)

3. Annualized estimate
   - For a "typical" analyst (based on median frequencies)
   - For the team (based on number of analysts and aggregated frequencies)
   - With explicit caveats (see disclaimers above)

4. Qualitative analysis
   - Tasks where the tool performs best
   - Tasks where the tool performs worst
   - Factors influencing savings (file size, country, complexity)
```

# Holistic Feedback Survey - Full Specification

## Administration: twice (end of week 2 and end of week 4)
## Implementation: Google Form / Microsoft Form with conditional logic
## Estimated completion time: 15-20 minutes

---

## SECTION A: General information

### A1. Your name or initials
- **Type**: Short text
- **Required**: Yes

### A2. Where do you work?
- **Type**: Single choice
- **Options**: HQ Rome / Country Office (specify country) / Regional Bureau / Other
- **Required**: Yes

### A3. How many years have you worked as a VAM analyst or in similar roles?
- **Type**: Single choice
- **Options**: Less than 1 year / 1-3 years / 3-5 years / More than 5 years
- **Required**: Yes

### A4. Which agents did you use during the test? (select all that apply)
- **Type**: Checkbox (multiple choice)
- **Options**:
  - MFI Validator (MFI dataset validation)
  - Price Validator (price data validation)
  - MFI Drafter (MFI report generation)
  - Market Monitor (price bulletin generation)
- **Required**: Yes
- **Implementation note**: This field controls routing to agent-specific sections (D, E, F, G)

### A5. About how many times did you use the tool in total?
- **Type**: Single choice
- **Options**: 1-2 times / 3-5 times / 6-10 times / More than 10 times
- **Required**: Yes

---

## SECTION B: Overall evaluation

### B1. How useful is the VAM LLM tool for your day-to-day work?
- **Type**: 1-5 scale
- **Scale**: 1 = Not useful at all / 2 = Slightly useful / 3 = Moderately useful / 4 = Very useful / 5 = Indispensable
- **Required**: Yes

### B2. How much do you trust the output produced by the tool?
- **Type**: 1-5 scale
- **Scale**: 1 = I do not trust it, I verify everything / 2 = I trust it a little, I verify most items / 3 = I trust it enough, I verify key points / 4 = I trust it a lot, I spot-check only / 5 = I fully trust it
- **Required**: Yes

### B3. How would you describe the quality of the outputs relative to your expectations?
- **Type**: 1-5 scale
- **Scale**: 1 = Far below expectations / 2 = Below expectations / 3 = Meets expectations / 4 = Above expectations / 5 = Far above expectations
- **Required**: Yes

### B4. In general, the tool output is...
- **Type**: Single choice
- **Options**:
  - Usable as-is, without edits
  - Usable with small edits (< 15 min of editing)
  - Requires significant edits (15-30 min of editing)
  - Requires substantial rewrite (> 30 min)
  - Not usable, I need to redo from scratch
- **Required**: Yes

### B5. Compared to your current manual process, the VAM LLM tool is...
- **Type**: 1-5 scale
- **Scale**: 1 = Much slower / 2 = Slightly slower / 3 = About the same / 4 = Slightly faster / 5 = Much faster
- **Required**: Yes

### B6. How easy is the tool to use?
- **Type**: 1-5 scale
- **Scale**: 1 = Very difficult / 2 = Difficult / 3 = Neither easy nor difficult / 4 = Easy / 5 = Very easy
- **Required**: Yes

### B7. How likely are you to recommend the tool to a colleague?
- **Type**: 0-10 scale (standard NPS)
- **Scale**: 0 = Not at all likely ... 10 = Extremely likely
- **Required**: Yes
- **Analysis note**: NPS = % Promoters (9-10) - % Detractors (0-6)

### B8. If the tool were available tomorrow in its current version, would you use it regularly?
- **Type**: 1-5 scale
- **Scale**: 1 = Definitely not / 2 = Probably not / 3 = Maybe / 4 = Probably yes / 5 = Definitely yes
- **Required**: Yes

---

## SECTION C: Accuracy and completeness (all testers)

### C1. Did you find errors in numeric data (scores, percentages, prices) in the tool outputs?
- **Type**: Single choice
- **Options**: Never / Rarely (1-2 times) / Sometimes (3-5 times) / Often (almost every time)
- **Required**: Yes

### C2. Did you find information that seemed invented or unsupported by data?
- **Type**: Single choice
- **Options**: Never / Rarely (1-2 times) / Sometimes (3-5 times) / Often (almost every time)
- **Required**: Yes

### C3. Were important pieces of information missing that you expected in the output?
- **Type**: Single choice
- **Options**: Never / Rarely / Sometimes / Often
- **Required**: Yes

### C4. When the output contained errors, how severe were they?
- **Type**: Single choice
- **Options**:
  - I did not find errors
  - Minor errors, easy to fix
  - Medium errors, required verification and correction
  - Severe errors, made the output unreliable
- **Required**: Yes

---

## SECTION D: MFI Validator (only if selected in A4)

*Routing: show this section only if the tester selected "MFI Validator" in question A4*

### D1. Did the validator correctly identify real issues in your datasets?
- **Type**: 1-5 scale
- **Scale**: 1 = Almost never / 2 = Rarely / 3 = Sometimes / 4 = Often / 5 = Almost always
- **Required**: Yes
- **Help text**: "Think of the issues you know are present in your files: did the tool find them?"

### D2. Did the validator flag issues that were not actually present (false positives)?
- **Type**: Single choice
- **Options**: Never / 1-2 times / 3-5 times / More than 5 times
- **Required**: Yes
- **Help text**: "The tool said there was an error, but after checking the data it was correct"

### D3. Which types of checks were most useful to you? (select all)
- **Type**: Checkbox (multiple choice)
- **Options**:
  - File structure checks (CSV format, encoding, broken rows)
  - Column/schema checks (missing columns, duplicates, typos in names)
  - Business rules checks (survey completeness, unique UUIDs, dates)
  - Final diagnostic report (text summary of issues)
- **Required**: Yes

### D4. Did the final diagnostic report help you understand what to fix in the dataset?
- **Type**: 1-5 scale
- **Scale**: 1 = Not at all / 2 = Slightly / 3 = Somewhat / 4 = A lot / 5 = Completely
- **Required**: Yes

### D5. Is there any type of check the validator does NOT perform but you would want?
- **Type**: Long text
- **Required**: No
- **Placeholder**: "e.g., verify GPS coordinates are inside the country, check consistency between survey date and submission date..."

---

## SECTION E: Price Validator (only if selected in A4)

*Routing: show this section only if the tester selected "Price Validator" in question A4*

### E1. Was product classification against the WFP list correct?
- **Type**: 1-5 scale
- **Scale**: 1 = Almost never correct / 2 = Rarely / 3 = Sometimes / 4 = Often / 5 = Almost always correct
- **Required**: Yes
- **Help text**: "Were the product names in your file matched to the correct WFP products?"

### E2. If classification was wrong, what types of errors did you notice? (select all)
- **Type**: Checkbox (multiple choice)
- **Options**:
  - Product matched to the wrong category (e.g., rice classified as wheat)
  - Local product not recognized (local-language name not mapped)
  - Generic products matched to overly specific items
  - Specific products matched to overly generic items
  - Product not classified (left blank/null)
- **Required**: No (conditional: if E1 <= 3)

### E3. Did the validator handle different file formats (CSV, Excel) correctly?
- **Type**: 1-5 scale
- **Scale**: 1 = Many issues / 2 / 3 / 4 / 5 = No issues
- **Required**: Yes

### E4. Is there any type of price data check the validator does NOT perform but you would want?
- **Type**: Long text
- **Required**: No
- **Placeholder**: "e.g., compare prices against historical values, detect statistical outliers, verify units of measure..."

---

## SECTION F: MFI Drafter (only if selected in A4)

*Routing: show this section only if the tester selected "MFI Drafter" in question A4*

### F1. Did the MFI dimension analyses (Assortment, Availability, Price, etc.) reflect the data correctly?
- **Type**: 1-5 scale
- **Scale**: 1 = Almost never / 2 = Rarely / 3 = Sometimes / 4 = Often / 5 = Almost always
- **Required**: Yes
- **Help text**: "When a dimension score was high or low, did the text describe it correctly?"

### F2. Did you notice cases where the report interpreted sub-scores in the opposite direction? (e.g., 80% scarcity described as 'good availability')
- **Type**: Single choice
- **Options**: Never / 1-2 times / 3-5 times / Often
- **Required**: Yes
- **Help text**: "This is a known error: the tool can confuse 'negative' percentages (e.g., % scarcity) with 'positive' percentages"

### F3. Were the market recommendations specific and actionable?
- **Type**: 1-5 scale
- **Scale**: 1 = Generic/useless / 2 = Not very specific / 3 = Somewhat specific / 4 = Specific / 5 = Very specific and useful
- **Required**: Yes

### F4. Did the executive summary capture the key points of the report?
- **Type**: 1-5 scale
- **Scale**: 1 = Not at all / 2 / 3 / 4 / 5 = Perfectly
- **Required**: Yes

### F5. Was the country context (from Seerist/ReliefWeb) relevant and accurate?
- **Type**: Single choice
- **Options**:
  - I did not notice any country context in the report
  - Context was present but not relevant or accurate
  - Context was fairly relevant
  - Context was very relevant and added value
- **Required**: Yes

### F6. Compared to an MFI report you would write manually, what is missing or different?
- **Type**: Long text
- **Required**: No
- **Placeholder**: "e.g., missing temporal comparisons, recommendations too generic, tone not aligned with WFP..."

---

## SECTION G: Market Monitor (only if selected in A4)

*Routing: show this section only if the tester selected "Market Monitor" in question A4*

### G1. Were the quantitative data in the bulletin (prices, MoM%, YoY% changes) correct?
- **Type**: 1-5 scale
- **Scale**: 1 = Almost never / 2 = Rarely / 3 = Sometimes / 4 = Often / 5 = Almost always
- **Required**: Yes

### G2. Did you notice cases where the bulletin made claims unsupported by data? (e.g., "strong currency depreciation" when the exchange rate was stable)
- **Type**: Single choice
- **Options**: Never / 1-2 times / 3-5 times / Often
- **Required**: Yes
- **Help text**: "This type of error can happen when the tool confuses contextual news with quantitative data"

### G3. Did the tool use economic terminology appropriately? (e.g., "hyperinflation" only for monthly inflation > 50%)
- **Type**: 1-5 scale
- **Scale**: 1 = Often inappropriate / 2 / 3 / 4 / 5 = Always appropriate
- **Required**: Yes

### G4. Was the price trend analysis per commodity consistent with your market knowledge?
- **Type**: 1-5 scale
- **Scale**: 1 = Not at all / 2 / 3 / 4 / 5 = Completely
- **Required**: Yes

### G5. Were the retrieved contextual news and events relevant for the country and period analyzed?
- **Type**: Single choice
- **Options**:
  - I did not notice news/events in the report
  - News/events were present but not relevant or were outdated
  - News/events were fairly relevant
  - News/events were very relevant and added value
- **Required**: Yes

### G6. Were the price charts clear and correct?
- **Type**: 1-5 scale
- **Scale**: 1 = Very problematic / 2 / 3 / 4 / 5 = Clear and correct
- **Required**: Yes

### G7. Compared to a price bulletin you would write manually, what is missing or different?
- **Type**: Long text
- **Required**: No
- **Placeholder**: "e.g., missing regional comparisons, price drivers too vague, outlook not realistic..."

---

## SECTION H: Strategic open questions

### H1. What is the MOST USEFUL thing the tool does for you?
- **Type**: Long text
- **Required**: Yes
- **Help text**: "Think of the moment when the tool saved you the most time or produced its best output. What were you doing?"

### H2. What is the MOST FRUSTRATING part of the experience with the tool?
- **Type**: Long text
- **Required**: Yes
- **Help text**: "Think of the moment when you had the most difficulty or the tool disappointed you the most. What happened?"

### H3. If you could change ONE THING about the tool, what would it be?
- **Type**: Long text
- **Required**: Yes
- **Help text**: "Your number one priority to improve the tool"

---

## SECTION I: Feature requests (final survey only, week 4)

*Note: include this section only in the second administration*

### I1. Which new features would you like? (select all that apply)
- **Type**: Checkbox (multiple choice)
- **Options**:
  - Automatic temporal comparisons (current vs previous period)
  - Ability to edit output directly in the tool
  - Support for more languages (French, Spanish, Arabic)
  - Integration with existing WFP platforms (e.g., DataViz, MoDA)
  - Cross-validation across different datasets
  - Custom report templates for Country Offices
  - Chat/questions about the data ("ask the tool")
  - Automatic alerts on data anomalies
  - Batch processing (multiple files/countries at once)
  - Archive of previous reports
- **Required**: No

### I2. Rank the top 3 features you consider most important
- **Type**: Ranking / Short text
- **Required**: No
- **Help text**: "Among the options above or your own ideas, what are the top 3 priorities?"

### I3. Is there anything we did not ask but you want to tell us?
- **Type**: Long text
- **Required**: No

---

## Implementation notes

### Routing logic (conditional)
- **Sections D, E, F, G**: Show based on responses to question A4
  - If A4 includes "MFI Validator" -> show Section D
  - If A4 includes "Price Validator" -> show Section E
  - If A4 includes "MFI Drafter" -> show Section F
  - If A4 includes "Market Monitor" -> show Section G
- **Section I**: Show only in the second administration (week 4)

### Differences between mid-test and end-test surveys
- **Mid-test (week 2)**: Sections A, B, C, D/E/F/G (routing), H
- **End-test (week 4)**: Sections A, B, C, D/E/F/G (routing), H, I

### Data analysis
- Calculate averages for each scalar question, segmented by: agent used, location (HQ vs CO), experience
- NPS: (% responses 9-10) - % responses 0-6) on question B7
- Mid-test vs end-test comparison to track evolution (learning effect)
- Thematic analysis of open-ended responses (H1, H2, H3) with manual clustering

### Result interpretation scale

| Question | Critical score | Acceptable score | Good score |
|---------|----------------|------------------|------------|
| Usefulness (B1) | < 2.5 | 2.5-3.5 | > 3.5 |
| Trust (B2) | < 2.5 | 2.5-3.5 | > 3.5 |
| Expectations (B3) | < 2.5 | 2.5-3.5 | > 3.5 |
| Ease of use (B6) | < 3.0 | 3.0-4.0 | > 4.0 |
| NPS (B7) | < -20 | -20 to +20 | > +20 |
| Adoption (B8) | < 2.5 | 2.5-3.5 | > 3.5 |

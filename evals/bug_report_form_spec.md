# Bug Report Form - Full Specification

## Implementation: Google Form or Microsoft Form

---

## Form Structure

### Field 1: Date and time
- **Type**: Date (auto-populated if possible)
- **Required**: Yes
- **Help text**: "When did you encounter the problem?"
- **Default**: Current date

### Field 2: Your name or initials
- **Type**: Short text
- **Required**: Yes
- **Help text**: "So we can contact you if we need clarifications"

### Field 3: Which agent were you using?
- **Type**: Single choice (radio button)
- **Required**: Yes
- **Options**:
  - MFI Validator (MFI dataset validation)
  - Price Validator (price data validation)
  - MFI Drafter (MFI report generation)
  - Market Monitor (price bulletin generation)
  - Home Page / Navigation
  - Not sure / Other
- **Help text**: "Select the tool you were using when you found the issue"

### Field 4: Type of issue
- **Type**: Single choice (radio button)
- **Required**: Yes
- **Options** (with description):

  **--- Technical issues ---**
  - **Crash / On-screen error** - "The tool showed a red error, froze, or stopped working"
  - **File upload failed** - "The tool did not accept my CSV/Excel file, or read it incorrectly"
  - **Result does not appear** - "The tool seems finished but I do not see any output, or the output is empty"
  - **Timeout / Too slow** - "I waited a long time and the tool did not respond"

  **--- Output quality issues (Validators) ---**
  - **False positive** - "The validator flagged an error that is not actually present (the data is correct)"
  - **False negative** - "The validator did NOT flag an error that is present (the data is wrong but the tool says OK)"
  - **Wrong product classification** - "A product was matched to the wrong WFP product (Price Validator only)"
  - **Unclear validation report** - "The final report is confusing, lacks detail, or does not help me fix the issue"

  **--- Output quality issues (Drafters / Report) ---**
  - **Wrong numeric value** - "A number in the report (score, percentage, price) does not match the real data"
  - **Wrong analysis/interpretation** - "The report says one thing, but the data says the opposite"
  - **Invented information** - "The report contains facts, events, or data that do not exist in the inputs or sources"
  - **Inappropriate terminology** - "Uses terms like 'hyperinflation', 'severe depreciation' without data support"
  - **Missing or incomplete section** - "The report skips a part, or a section is too short/empty"
  - **Low-quality writing** - "The writing is repetitive, generic, or not at WFP report level"

  **--- Interface issues ---**
  - **Layout/visual issue** - "Something is not visible, is cut off, or is positioned incorrectly"
  - **Wrong chart/visualization** - "A chart shows wrong data or is unreadable"
  - **Button/feature not working** - "I clicked something and nothing happened"
  - **DOCX export issues** - "The exported Word file has formatting problems or missing content"

  - **Other** - "None of the above categories describe my issue"

- **Help text**: "Choose the category that best describes the issue. If you are unsure, pick 'Other' and describe it below."

### Field 5: How severe is it?
- **Type**: Single choice (radio button)
- **Required**: Yes
- **Options**:
  - :red_circle: **Blocking** - "I cannot use the tool to complete my work. The output is unusable or the tool does not work."
  - :yellow_circle: **Annoying** - "I can complete my work, but I have to manually fix or verify significant parts of the output."
  - :green_circle: **Cosmetic** - "A minor defect that does not impact work. A visual imperfection or small detail."
- **Help text**: "Think: 'Does this prevent me from using the output for my work?'"

### Field 6: Describe what happened
- **Type**: Long text (paragraph)
- **Required**: Yes
- **Help text**: "Describe in your own words: what you were doing, what you expected, and what happened instead. The more detail you provide, the faster we can fix it. See examples in the instructions."
- **Placeholder**: "I was doing [action] with [data type]. I expected [expected result]. Instead [what happened]."

### Field 7: Can you reproduce it?
- **Type**: Single choice (radio button)
- **Required**: Yes
- **Options**:
  - Yes, it happens every time I repeat the same operation
  - Yes, but only sometimes (intermittent)
  - No, it happened only once
  - I did not try again
- **Help text**: "Did you try the same action again? Did it happen again?"

### Field 8: Screenshot or attached file
- **Type**: File upload
- **Required**: No
- **Help text**: "If possible, attach a screenshot of the error or problematic result. You can also attach the CSV/Excel file that caused the issue (if it does not contain sensitive data)."
- **Accepted formats**: PNG, JPG, PDF, CSV, XLSX, XLS, DOCX

### Field 9: Run ID (if available)
- **Type**: Short text
- **Required**: No
- **Help text**: "If the tool displayed a 'Run ID' (alphanumeric code in the results area), copy it here. It helps us find the logs."
- **Placeholder**: "e.g., a1b2c3d4-e5f6-..."

### Field 10: Additional notes
- **Type**: Long text (paragraph)
- **Required**: No
- **Help text**: "Any other useful detail: data country, period, number of rows, attempted workarounds..."

---

## Confirmation page

**Text shown after submission:**

> Thank you for the report! The team reviews these regularly.
>
> - **Blocking** issues are handled with priority
> - We will reach out if we need clarifications
>
> You can continue using the tool and report other issues at any time.

---

## Tester instructions (attach or link in the form)

### When to fill this form

Fill in the form **every time** something does not work as expected. This includes:
- The tool errors out or freezes
- The output contains wrong data (numbers, percentages, classifications)
- The report says things that contradict the data
- A report uses inappropriate language for the data (e.g., "hyperinflation" when prices are stable)
- Sections, charts, or information are missing
- The interface has visual or functional issues

**Do not** use this form for:
- Ideas for new features (use the bi-monthly survey)
- General experience feedback (use the bi-monthly survey)

### MFI Validator RAW column requirements (for troubleshooting)

Column matching is **exact** (case-insensitive) and uses only the aliases listed below. **No LLM fuzzy matching is performed.**

- Required (validator will fail if missing):
  ```text
  SvyStartTime, _submission_time, _UOALatlng_altitude, _UOALatlng_latitude, _UOALatlng_longitude, _uuid, Adm0Code, Adm1Code, Adm2Code, EnumName, instanceID, MarketID, MarketName, MktAccessCnstr, MktAvailRunout_Gr, MktCompetLessFive_Gr, MktCompetOneContr_Gr, MktPriceStab_Gr, MktProtCnstr, MktStructureCond, MktStructureType, MktTraderNb, MktTrdSkuNb_Cl, ShopCheckoutNb, ShopEmployeeNb, ShopSize, SvyDate, SvyEndTime, SvyMod, SvyModConf, TrdAvailRunout_Gr, TrdConsent2NF2F, TrdConsentF2F, TrdConsentNF2F, TrdCustmGroup, TrdNodDensLocNameAdm0, TrdNodDensLocNameAdm1, TrdNodDensLocNameAdm2, TrdPriceStab_Gr, TrdResilLeadtime, TrdResilNodComplex_Gr, TrdResilNodCrit_Gr, TrdResilNodDens_Gr, TrdResilStockout, TrdServiceCheckoutExp, TrdServiceLoyalty, TrdServicePayType, TrdServicePos, TrdServicePosAnalysis, TrdServiceShopExp, TrdSkuNb_Cl, TrdStructureCond, TrdStructureType, UOAAvailScarce_Gr, UOALatlng, UOAPicture, UOAPriceIncr_Gr, UOAQltyFAnimRefrig, UOAQltyFAnimRefrigWork, UOAQltyFood, UOAQltyFPackGood, UOAQltyFVegFruGood, UOAQltyFVegFruSeparate, UOAQltyPackExpiry, UOAQltyPlastGood, UOASoldGroup_FCer, UOASoldGroup_FOth, UOASoldGroup_Gr, UOASoldGroup_NF, UOAStructureFeat
  ```
- Matching rules:
  - Case-insensitive exact match on required names.
  - Explicit legacy aliases accepted (no fuzzy matching):
    - `SvyStartTime`: `Svy_Start_Time`
    - `_submission_time`: `Submission_Time`
    - `_UOALatlng_altitude`: `UOALatlng_altitude`
    - `_UOALatlng_latitude`: `UOALatlng_latitude`
    - `_UOALatlng_longitude`: `UOALatlng_longitude`
    - `_uuid`: `UUID`
    - `Adm0Code`: `Adm0_Code`
    - `Adm1Code`: `Adm1_Code`
    - `Adm2Code`: `Adm2_Code`
    - `EnumName`: `Enum_Name`, `Enumerator`
    - `instanceID`: `instance_ID`, `ResponseID`, `Response_ID`, `_ID`
    - `MarketID`: `Market_ID`
    - `MarketName`: `Market_Name`
    - `SvyDate`: `Svy_Date`, `SubmissionDate`
    - `SvyEndTime`: `Svy_End_Time`
    - `SvyMod`: `Svy_Mod`, `SurveyType`, `Survey_Type`

### How to write a good report

**Good example:**
> "I was validating an MFI dataset for Sudan (file: sudan_mfi_jan2025.csv, 450 rows).
> The validator flagged 'duplicate UUIDs' in rows 120-130, but I checked manually
> and the UUIDs are all different. This looks like a false positive.
> Run ID: abc123-def456"

**Poor example:**
> "It does not work"

The difference: the first lets us find and fix the problem. The second does not.

### Tips for useful screenshots

1. Capture the **entire screen** of the tool, not just a detail
2. If there is a red error message, make sure it is **readable** in the screenshot
3. If the problem is in the output, include both the tool output and the correct data (if you have it)

### Severity levels - quick guide

| Level | Meaning | Example |
|---------|---------|---------|
| :red_circle: Blocking | I cannot work | Tool crash; report with completely wrong data; file cannot be uploaded |
| :yellow_circle: Annoying | I can work but with effort | I must fix 30% of the report; wrong product classification on 5 products; timeout after 10 min |
| :green_circle: Cosmetic | Minor detail | Typo in report; slightly misaligned chart; wrong chart color |

---

## Implementation notes

### Google Forms
- Use "Sections" to group fields visually
- Enable "Collect email" for follow-up
- Enable file uploads (requires Google account)
- Link responses to a Google Sheet for analysis

### Microsoft Forms
- Use "Sections" to group fields
- File upload supported natively
- Link to Excel for analysis
- Branching logic available for the "Type of issue" field

### Response analysis
Create a linked Google Sheet / Excel with additional columns for the development team:
- **Status**: Open / In progress / Resolved / Not reproducible / Won't fix
- **Assigned to**: Developer name
- **Root cause**: Identified technical root cause
- **Release fix**: Version where it was fixed
- **Dev notes**: Internal technical comments

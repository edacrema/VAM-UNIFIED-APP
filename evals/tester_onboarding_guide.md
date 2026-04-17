# Tester Guide - VAM LLM Alpha Test

---

## Welcome!

Thank you for participating in the VAM LLM alpha test. Your feedback is essential for deciding how to develop this tool.

**What VAM LLM is**: A tool that helps VAM analysts with 4 tasks:
1. **Validate MFI datasets** - Checks structural, schema, and business-rule issues in raw CSV files
2. **Validate price data** - Checks structure and classifies products against the standard WFP list
3. **Generate MFI reports** - Produces draft reports on market functionality, with dimension analysis and recommendations
4. **Generate price bulletins** - Produces draft price bulletins with trends, commodity analysis, and context

**What we expect from you**: Use the tool in your normal work. When you use it, tell us how it went. You do not need to be technical or an AI expert - we care about your judgment as an analyst.

---

## The 3 feedback tools

### 1. Bug Report (when you find a problem)

**When**: Every time something does not work or the output is wrong
**Where**: [LINK TO FORM - to be added]
**Time**: 3-5 minutes per report

**Fill in the form when:**
- The tool errors out or freezes
- A number in the report is wrong
- The report says something that contradicts the data
- A section is missing
- A product is classified incorrectly

**Do NOT use the form for:**
- Ideas for new features (use the survey)
- General opinions (use the survey)

**Tip**: If possible, take a screenshot and note the Run ID (shown on the results page).

---

### 2. Time Tracker (after each use)

**When**: After EVERY tool use (1-2 minutes)
**Where**: [LINK TO GOOGLE SHEET - to be added]
**Time**: 1-2 minutes per entry

After using the tool, fill one row with:
- Which agent you used
- How long it took (including checking the output)
- How long it would take manually (your estimate)
- Whether the output was usable

No stopwatch needed. A reasonable estimate is fine.

---

### 3. Feedback survey (twice per month)

**When**: End of week 2 and end of week 4
**Where**: [LINK TO SURVEY FORM - to be added]
**Time**: 15-20 minutes

A questionnaire about your overall experience: usefulness, accuracy, trust, and what you would improve. We will ask questions about the agents you used.

---

## Monthly timeline

```
WEEK 0 (before the test)
  [x] Attend onboarding (this guide + demo)
  [x] Complete the "time baseline" questionnaire (5 min, one time)
  [x] Verify you can access the tool

WEEK 1-2
  [ ] Use the tool in your work
  [ ] Fill the Time Tracker after each use
  [ ] Report issues with the Bug Report form
  [ ] END OF WEEK 2: Complete the mid-test survey

WEEK 3-4
  [ ] Continue using the tool
  [ ] If you only used 1-2 agents, try the others
  [ ] Fill the Time Tracker after each use
  [ ] Report issues with the Bug Report form
  [ ] END OF WEEK 4: Complete the final survey
```

---

## Practical tips

**To get the best results from the tool:**
- Use clean CSV/Excel files in the expected formats
- For the MFI Validator: use RAW files with the required columns (see below)
- For the Price Validator: supports CSV, XLSX, and XLS
- For the MFI Drafter: either upload the final processed/elaborated MFI CSV or select a Databridges MFI survey
- For the Market Monitor: select country, period, commodities, and regions; price data is retrieved from Databridges

**MFI Validator RAW column requirements (exact match, no fuzzy matching):**
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

**External services:**
- External endpoints and API keys are managed centrally via GCP secrets. Testers do not need to provide keys.

**When the output is not perfect:**
- This is a prototype: errors are expected and valuable
- Do not hesitate to report problems - that is exactly what we need
- The more specific your feedback, the faster we can improve the tool

**How much time to dedicate:**
- You do not need to change how you work
- Use the tool when you would normally perform one of the 4 supported tasks
- Feedback overhead is about 2-3 hours across the entire month

---

## Contacts

For technical issues (cannot access, tool does not start):
- [TECH CONTACT NAME] - [EMAIL]

For questions about the testing process:
- [COORDINATOR NAME] - [EMAIL]

---

*Thank you for your time and contribution!*

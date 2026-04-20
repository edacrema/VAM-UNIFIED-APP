from pathlib import Path
from typing import Optional

import streamlit as st

from streamlit_shared import (
    apply_wfp_theme,
    render_bug_report_sidebar_link,
    render_instructions_sidebar_button,
    render_onboarding_sidebar_button,
    render_wfp_sidebar_logo,
)

st.set_page_config(page_title="How to use the tools", layout="wide")
apply_wfp_theme()

with st.sidebar:
    render_wfp_sidebar_logo()
    render_onboarding_sidebar_button(key="sidebar_onboarding_instructions")
    render_instructions_sidebar_button(key="sidebar_instructions_page")
    render_bug_report_sidebar_link()


st.markdown(
    """
    <style>
    .jump-links {
        background: rgba(255, 255, 255, 0.7);
        border: 1px solid rgba(0, 58, 93, 0.12);
        border-radius: 16px;
        padding: 1rem 1.25rem;
        box-shadow: 0 10px 24px rgba(0, 58, 93, 0.08);
        margin: 1.5rem 0 2rem 0;
    }
    .jump-links ul {
        list-style: none;
        padding-left: 0;
        margin: 0;
    }
    .jump-links li {
        margin: 0.35rem 0;
    }
    .jump-links a {
        text-decoration: none;
        color: var(--wfp-primary);
        font-weight: 600;
    }
    .jump-links a:hover {
        text-decoration: underline;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

st.title("VAM LLM — Service Instructions")

st.markdown(
    """
## About this application

This application provides a suite of AI-powered tools designed to support WFP food security analysts in two key areas: **data validation** and **analytical report generation**.

The tools are organized into two categories:

- **Validators** (MFI Dataset Validator, Price Data Validator): These tools check raw datasets for errors before they are uploaded to DataBridges, preventing failed uploads and reducing the time spent on manual data cleaning.
- **Report Drafters** (Price Bulletin Drafter, MFI Report Drafter): These tools generate complete analytical reports from WFP data, automating tasks that currently require significant manual effort while maintaining the analytical standards expected in WFP publications.

This application is currently in **ALPHA** (an early internal testing phase with a limited number of users) and is being developed under two complementary projects: **VAM LLM** and **MarketAIssist**. The tools you see here are prototypes undergoing active testing and refinement. Your feedback during this phase is essential to improve their accuracy, usability, and relevance to real-world workflows.
    """
)

st.markdown("## Jump to a tool")
st.markdown(
    """
    <div class="jump-links">
        <ul>
            <li><a href="#mfi-dataset-validator">MFI Dataset Validator</a></li>
            <li><a href="#price-data-validator">Price Data Validator</a></li>
            <li><a href="#price-bulletin-drafter">Price Bulletin Drafter</a></li>
            <li><a href="#mfi-report-drafter">MFI Report Drafter</a></li>
        </ul>
    </div>
    """,
    unsafe_allow_html=True,
)


def _asset_path(filename: str) -> Optional[Path]:
    root_dir = Path(__file__).resolve().parent.parent
    candidate = root_dir / "app" / "shared" / "assets" / filename
    return candidate if candidate.exists() else None


def _render_tool_image(filename: str, caption: str) -> None:
    image_path = _asset_path(filename)
    if image_path is None:
        st.info(f"Screenshot not found: {filename}")
        return
    st.image(str(image_path), caption=caption, width="stretch")


st.markdown('<a id="mfi-dataset-validator"></a>', unsafe_allow_html=True)
st.header("1. MFI Dataset Validator")

st.subheader("What it does")
st.markdown(
    """
The MFI Dataset Validator checks your **raw (non-processed) MFI dataset** for errors before you upload it to DataBridges. DataBridges will reject files that contain structural or formatting issues — this tool catches those problems in advance, saving you time and failed uploads.
    """
)

st.subheader("When to use it")
st.markdown(
    """
Use this tool **before uploading your raw MFI data to DataBridges**. It is designed exclusively for non-processed datasets — do not use it with data that has already been processed by DataBridges.
    """
)

st.subheader("How to use it")
left, right = st.columns([1.35, 1])
with left:
    st.markdown(
        """
1. **Upload your file**: Drag and drop (or browse) your raw MFI dataset in **CSV format**.
2. **Select the survey type**: Choose the type of MFI survey the dataset refers to (Full MFI, Reduced MFI, or MFI-N). *Note: in this ALPHA version, only Full MFI is available.*
3. **Click "Validate"**.
        """
    )
with right:
    _render_tool_image("mfidata_validator.jpeg", "MFI Dataset Validator")

st.subheader("Output")
st.markdown(
    """
If the validator detects issues, it will generate a report listing each problem found, including its location in the dataset and a brief explanation of how to fix it. If no issues are found, the dataset is ready for DataBridges upload.
    """
)

st.markdown("---")

st.markdown('<a id="price-data-validator"></a>', unsafe_allow_html=True)
st.header("2. Price Data Validator")

st.subheader("What it does")
st.markdown(
    """
The Price Data Validator checks your **raw price dataset** for errors before you upload it to DataBridges. Just like the MFI Validator, it catches problems that would cause DataBridges to reject your file.
    """
)

st.subheader("When to use it")
st.markdown(
    """
Use this tool **before uploading your raw price data to DataBridges**.
    """
)

st.subheader("How to use it")
left, right = st.columns([1.35, 1])
with left:
    st.markdown(
        """
1. **Upload your price data file**: Drag and drop (or browse) your raw price dataset in **CSV, XLSX, or XLS format**.
2. **Upload the DataBridges template**: Upload the same template you previously registered on DataBridges for this dataset. The validator needs it because DataBridges uses this template to assess compatibility with the uploaded data — the validator performs the same check.
3. **Click "Validate"**.
        """
    )
with right:
    _render_tool_image("pricedata_validator.jpeg", "Price Data Validator")

st.subheader("Output")
st.markdown(
    """
Same as the MFI Dataset Validator: a report listing any issues found, their location, and suggested fixes. If the dataset passes all checks, it is ready for DataBridges upload.
    """
)

st.markdown("---")

st.markdown('<a id="price-bulletin-drafter"></a>', unsafe_allow_html=True)
st.header("3. Price Bulletin Drafter")

st.subheader("What it does")
st.markdown(
    """
The Price Bulletin Drafter generates a complete Market Price Bulletin report for a given country and month. It analyzes price changes for the selected period by comparing them against the previous 12 months of data, and enriches the analysis with contextual information from recent news and reports.
    """
)

st.subheader("How to use it")
left, right = st.columns([1.35, 1])
with left:
    st.markdown(
        """
**Required fields:**

1. **Country**: Select the country for which the report should be generated.
2. **Time Period**: Select the reference month (e.g., January 2026). The system will analyze price trends for that month against the previous 12 months.

Once you select the country and period, the system will **automatically populate**:

- **Commodities**: The commodities available from Databridges for the selected country. You can remove any commodity you want to exclude from the report.
- **Regions (Admin1)**: The Databridges market regions available for the selected country. You can remove any region you want to exclude.
- **Currency Code**: Required to download macroeconomic data from TradingEconomics for the optional modules.

**Optional fields:**

- **News Start Date / News End Date**: Define the time interval within which the system will search for contextual news and reports to inform the analysis. If left empty, the system defaults to the **3 months preceding (and including) the reference month**.
- **Enabled Modules**: The Price Bulletin has a core analysis (generated for all reports) and optional add-on modules that provide additional layers of data and analysis (e.g., exchange rate trends, fuel prices, macroeconomic indicators). *In this ALPHA version, only the Exchange Rate module is available.* You can choose whether to include it or not.

**Click "Run"** to start the agent.
        """
    )
with right:
    _render_tool_image("price_bulletin_drafter.jpeg", "Price Bulletin Drafter")

st.subheader("Output")
st.markdown(
    """
The agent takes approximately **10 minutes** to complete. Once finished, the generated report will appear in the output window below the form. Click **"Export"** to generate and download the report as a **.docx** file.
    """
)

st.markdown("---")

st.markdown('<a id="mfi-report-drafter"></a>', unsafe_allow_html=True)
st.header("4. MFI Report Drafter")

st.subheader("What it does")
st.markdown(
    """
The MFI Report Drafter generates a complete Market Functionality Index (MFI) assessment report from processed MFI data uploaded as a final processed/elaborated CSV.
    """
)

st.subheader("When to use it")
st.markdown(
    """
Use this tool **after** your raw MFI data has been processed by Databridges. Upload the final processed CSV and use the optional overrides only when the file metadata is incomplete.
    """
)

st.subheader("How to use it")
left, right = st.columns([1.35, 1])
with left:
    st.markdown(
        """
1. **Upload the processed MFI CSV**.
2. **Add optional country/date overrides** only if the file metadata is missing or incorrect.
3. **Click "Run"** to generate the MFI report.
        """
    )
with right:
    _render_tool_image("mfi_report_drafter.jpeg", "MFI Report Drafter")

st.subheader("Output")
st.markdown(
    """
The agent takes approximately **20 minutes** to complete. Once finished, the generated report will appear in the output window below the form. Click **"Export"** to generate and download the report as a **.docx** file.
    """
)

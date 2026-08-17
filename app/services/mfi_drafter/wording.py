"""Shared wording rules for MFI narrative safety.

Three consumers need the same definition of what an unsupported conclusion looks like:
the narrative validator that rejects one, the structural inspector that measures how many
reached the page, and the test matrix that proves both. Keeping the definitions here means
enforcement and measurement cannot drift apart — if they did, the remediation's exit gate
would be either unsatisfiable or vacuous depending on which way they diverged.

This module imports only the standard library. ``report_inspector`` depends on it and is
contract-tested to pull in no heavy dependencies, so nothing here may import ``narrative``,
``analysis``, ``schemas``, or ``methodology``.
"""

from __future__ import annotations

import re
from typing import Mapping, Optional

# ---------------------------------------------------------------------------
# Modality conclusions (FIX-02)
# ---------------------------------------------------------------------------

# A conclusion needs a modality subject *and* an operational verdict about it, in one
# sentence. Naming a modality is not a conclusion; saying something "warrants review" is
# not a conclusion; doing both at once is.
MODALITY_SUBJECT_PATTERN = (
    r"\b(?:cash|vouchers?|CVA|CBT|in-kind|hybrid|"
    r"transfer modalit(?:y|ies)|cash-based transfers?)\b"
)

# "feasibility" is deliberately absent. The approved recommendation vocabulary includes
# "feasibility assessment", and the deterministic market recommendation says "triangulate
# it with operational and feasibility evidence" — including it would put the safe
# fallback one noun away from flagging itself.
MODALITY_VERDICT_PATTERN = (
    r"\b(?:feasible|viable|appropriate|suitable|preferable|advisable|"
    r"effective(?:ness)?|ineffective|compromised|constrained|"
    r"undermine[ds]?|erode[sd]?|warrants? caution|"
    r"pose[sd]? (?:significant )?risks?|challenges for|conducive to)\b"
)

# A statement of what the evidence does *not* establish is the permitted form, and is the
# wording the remediation itself introduces. It must never be mistaken for the defect.
METHODOLOGICAL_NEGATION_PATTERN = (
    r"\b(?:does not|do not|did not|cannot|can not|must not|should not|"
    r"is not intended to|are not intended to|no)\b"
    r"[^.]{0,80}?"
    r"\b(?:determine|determines|establish|establishes|measure|measures|"
    r"indicate|indicates|demonstrate|demonstrates|prove|proves|"
    r"imply|implies|infer|infers|assess|assesses)\b"
)

# ---------------------------------------------------------------------------
# Affordability and causality (FIX-02)
# ---------------------------------------------------------------------------

# Word-boundaried, unlike the substring matching it replaces. "unaffordable" is matched
# deliberately: it is as much an affordability conclusion as "affordable" is.
AFFORDABILITY_PATTERN = (
    r"\b(?:un)?affordab(?:le|ility)\b"
    r"|\bpurchasing power\b"
    r"|\binflation(?:ary)?\b"
    r"|\bcost of living\b"
    r"|\bbeneficiar(?:y|ies)\b"
)

CAUSAL_PATTERN = (
    r"\b(?:caused|causing|led to|leads? to|resulted in|results? in|"
    r"because of|drove the|driving the|is responsible for|"
    r"exacerbat(?:e|es|ed|ing)|erod(?:e|es|ed|ing)|"
    r"diminish(?:es|ed|ing)? (?:the )?effectiveness|"
    r"protect(?:s|ed|ing)? (?:household )?purchasing power|"
    r"will (?:reduce|increase|worsen|improve))\b"
)

# ---------------------------------------------------------------------------
# Population wording (FIX-03)
# ---------------------------------------------------------------------------

# Anchored on the percentage-of-respondent-noun construct rather than a bare respondent
# noun. The approved phrase for exactly this population is "the unweighted mean of
# market-level trader proportions", which itself contains "trader" — a bare-noun rule
# would flag the wording introduced to fix the defect.
POOLED_POPULATION_PATTERN = (
    r"\d+(?:\.\d+)?%\s+of\s+(?:all\s+|the\s+)?(?:surveyed\s+)?"
    r"(?:traders|respondents|responses|vendors)\b"
)

APPROVED_AGGREGATION_WORDING = (
    "unweighted",
    "market-level",
    "market level",
    "assessed markets",
    "mean of market",
)

# ---------------------------------------------------------------------------
# Deterministic replacement content
# ---------------------------------------------------------------------------

NEUTRAL_SCOPE_STATEMENT = (
    "MFI findings can inform further feasibility analysis but do not determine "
    "transfer modality, affordability, or household purchasing power."
)

# Every variant is digit-free, modality-free, and respondent-noun-free, so a replacement
# can never trip the validators that caused it.
WITHDRAWN_CLAIM_TEXT: Mapping[str, str] = {
    "summary": (
        "A drafted summary statement for this section could not be validated against the "
        "deterministic assessment evidence and has been withdrawn."
    ),
    "finding": (
        "A drafted finding for this section could not be validated against the "
        "deterministic assessment evidence and has been withdrawn."
    ),
    "geographic_pattern": (
        "A drafted geographic observation could not be validated against the "
        "deterministic assessment evidence and has been withdrawn."
    ),
    "recommendation": (
        "A drafted recommendation could not be validated against the deterministic "
        "assessment evidence and has been withdrawn."
    ),
    "limitation": (
        "A drafted limitation statement could not be validated against the deterministic "
        "assessment evidence and has been withdrawn."
    ),
    "modality_consideration": (
        "A drafted statement could not be validated against the deterministic assessment "
        "evidence and has been withdrawn."
    ),
}

WITHDRAWN_CLAIM_DEFAULT = (
    "A drafted statement could not be validated against the deterministic assessment "
    "evidence and has been withdrawn."
)


def withdrawn_text(claim_kind: Optional[str]) -> str:
    """Return the replacement text for a claim that could not be validated."""
    return WITHDRAWN_CLAIM_TEXT.get(str(claim_kind or ""), WITHDRAWN_CLAIM_DEFAULT)


# ---------------------------------------------------------------------------
# Compiled forms
# ---------------------------------------------------------------------------

_MODALITY_SUBJECT = re.compile(MODALITY_SUBJECT_PATTERN, re.IGNORECASE)
_MODALITY_VERDICT = re.compile(MODALITY_VERDICT_PATTERN, re.IGNORECASE)
_METHODOLOGICAL_NEGATION = re.compile(METHODOLOGICAL_NEGATION_PATTERN, re.IGNORECASE)
_AFFORDABILITY = re.compile(AFFORDABILITY_PATTERN, re.IGNORECASE)
_CAUSAL = re.compile(CAUSAL_PATTERN, re.IGNORECASE)
_POOLED_POPULATION = re.compile(POOLED_POPULATION_PATTERN, re.IGNORECASE)

_NUMBER_RE = re.compile(r"(?<![A-Za-z0-9_])[-+]?\d+(?:\.\d+)?%?")


def split_sentences(text: str) -> list[str]:
    """Split text into sentences for conjunction rules.

    Rules that require two conditions to co-occur are evaluated per sentence, so a claim
    that mentions a modality in one sentence and a verdict about something else in another
    is not treated as a conclusion.
    """
    parts: list[str] = []
    for line in str(text or "").splitlines():
        for chunk in re.split(r"(?<=[.!?])\s+", line):
            chunk = chunk.strip()
            if chunk:
                parts.append(chunk)
    return parts


def numeric_tokens(text: str) -> list[tuple[str, float, bool]]:
    """Extract numeric tokens as ``(token, value, is_percent)``.

    The ``/10`` suffix is dropped before tokenizing so a score reads as one number. This
    is the single tokenizer used for numeric authorization and for the sanitizer's
    invariance guarantee, so the two can never disagree about what a number is.
    """
    cleaned = re.sub(r"/\s*10\b", "", str(text))
    tokens: list[tuple[str, float, bool]] = []
    for match in _NUMBER_RE.finditer(cleaned):
        token = match.group(0)
        is_percent = token.endswith("%")
        try:
            numeric = float(token[:-1] if is_percent else token)
        except ValueError:
            continue
        tokens.append((token, numeric, is_percent))
    return tokens


def is_modality_conclusion(sentence: str) -> bool:
    """Return whether one sentence reaches an operational verdict about a modality."""
    if not _MODALITY_SUBJECT.search(sentence):
        return False
    if not _MODALITY_VERDICT.search(sentence):
        return False
    return not _METHODOLOGICAL_NEGATION.search(sentence)


def modality_conclusions(text: str) -> list[str]:
    """Return every sentence in the text that reaches a modality verdict."""
    return [
        sentence for sentence in split_sentences(text) if is_modality_conclusion(sentence)
    ]


def affordability_claims(text: str) -> list[str]:
    """Return sentences inferring affordability, inflation, or purchasing power."""
    return [
        sentence
        for sentence in split_sentences(text)
        if _AFFORDABILITY.search(sentence)
        and not _METHODOLOGICAL_NEGATION.search(sentence)
    ]


def causal_claims(text: str) -> list[str]:
    """Return sentences asserting a causal or predictive mechanism."""
    return [
        sentence
        for sentence in split_sentences(text)
        if _CAUSAL.search(sentence) and not _METHODOLOGICAL_NEGATION.search(sentence)
    ]


def pooled_population_phrases(text: str) -> list[str]:
    """Return percentages attributed to a respondent population."""
    return [match.group(0) for match in _POOLED_POPULATION.finditer(str(text or ""))]


def has_approved_aggregation_wording(text: str) -> bool:
    """Return whether the text qualifies a value as an unweighted market statistic."""
    lowered = str(text or "").casefold()
    return any(term in lowered for term in APPROVED_AGGREGATION_WORDING)


# ---------------------------------------------------------------------------
# Markdown sanitization (FIX-08)
# ---------------------------------------------------------------------------

_CODE_FENCE = re.compile(r"```[a-zA-Z0-9_+-]*")
_IMAGE_OR_LINK = re.compile(r"!?\[([^\]\n]*)\]\([^)\n]*\)")
_AUTOLINK = re.compile(r"<(https?://[^>\s]+)>")
_BOLD = re.compile(r"\*\*([^*\n]+)\*\*")
_ITALIC = re.compile(r"(?<!\*)\*([^*\n]+)\*(?!\*)")
_ATX_HEADING = re.compile(r"^\s{0,3}#{1,6}\s+", re.MULTILINE)
# Ordered-list markers are deliberately absent: stripping "1. " would delete a number.
_BULLET = re.compile(r"^\s{0,3}[-*+]\s+", re.MULTILINE)
_WHITESPACE = re.compile(r"\s+")

_RESIDUAL_MARKUP_CHECKS: tuple[tuple[str, str], ...] = (
    ("code_fence", r"```"),
    ("backtick", r"`"),
    ("markdown_link", r"\[[^\]\n]*\]\([^)\n]*\)"),
    ("bold_marker", r"\*\*"),
    ("heading_marker", r"(?m)^\s{0,3}#{1,6}\s+\S"),
    ("autolink", r"<https?://"),
)


def _apply_sanitizer_rules(text: str) -> str:
    cleaned = _CODE_FENCE.sub(" ", text)
    cleaned = _IMAGE_OR_LINK.sub(r"\1", cleaned)
    cleaned = _AUTOLINK.sub(r"\1", cleaned)
    cleaned = _BOLD.sub(r"\1", cleaned)
    cleaned = _ITALIC.sub(r"\1", cleaned)
    cleaned = _ATX_HEADING.sub("", cleaned)
    cleaned = _BULLET.sub("", cleaned)
    cleaned = cleaned.replace("`", "")
    return _WHITESPACE.sub(" ", cleaned).strip()


def sanitize_claim_text(text: Optional[str]) -> str:
    """Remove Markdown delimiters without ever altering a number.

    Backticks in generated prose wrap values, so removing them must leave the value
    byte-identical. Rather than trusting the rules to be safe, the result is re-tokenized
    and compared: if the numeric tokens changed in any way the original text is returned
    unchanged, and the validator flags the surviving markup. Delivering raw text that gets
    flagged is strictly safer than silently deleting a digit.

    Underscores are never touched — they appear inside real metric identifiers such as
    ``cereal_food`` — and ordered-list markers are left alone because "1. " is a number.
    """
    raw = str(text or "")
    candidate = _apply_sanitizer_rules(raw)
    if numeric_tokens(candidate) != numeric_tokens(raw):
        return raw.strip()
    return candidate


def residual_markup(text: Optional[str]) -> tuple[str, ...]:
    """Return the names of Markdown constructs still present in canonical text."""
    subject = str(text or "")
    found = [
        name
        for name, pattern in _RESIDUAL_MARKUP_CHECKS
        if re.search(pattern, subject)
    ]
    return tuple(found)

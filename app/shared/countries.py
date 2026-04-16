"""Country aliases and ISO3 helpers for Databridges-backed tools."""
from __future__ import annotations

from typing import Dict, Tuple

COUNTRY_ALIASES = {
    "burma": "Myanmar",
    "cabo verde": "Cape Verde",
    "congo drc": "Democratic Republic of the Congo",
    "drc": "Democratic Republic of the Congo",
    "dr congo": "Democratic Republic of the Congo",
    "gaza": "Gaza Strip",
    "ivory coast": "Cote d'Ivoire",
    "laos": "Lao People's Democratic Republic",
    "palestine": "Palestine, State of",
    "s. sudan": "South Sudan",
    "southsudan": "South Sudan",
    "swaziland": "Eswatini",
    "syria": "Syrian Arab Republic",
    "tanzania": "Tanzania, United Republic of",
    "west bank": "West Bank",
}

COUNTRY_NAME_TO_ISO3: Dict[str, Tuple[str, str]] = {
    "afghanistan": ("Afghanistan", "AFG"),
    "algeria": ("Algeria", "DZA"),
    "angola": ("Angola", "AGO"),
    "armenia": ("Armenia", "ARM"),
    "bangladesh": ("Bangladesh", "BGD"),
    "benin": ("Benin", "BEN"),
    "bolivia": ("Bolivia, Plurinational State of", "BOL"),
    "burkina faso": ("Burkina Faso", "BFA"),
    "burundi": ("Burundi", "BDI"),
    "cambodia": ("Cambodia", "KHM"),
    "cameroon": ("Cameroon", "CMR"),
    "central african republic": ("Central African Republic", "CAF"),
    "chad": ("Chad", "TCD"),
    "colombia": ("Colombia", "COL"),
    "congo": ("Congo", "COG"),
    "cote d'ivoire": ("Cote d'Ivoire", "CIV"),
    "cote d ivoire": ("Cote d'Ivoire", "CIV"),
    "democratic republic of congo": ("Democratic Republic of the Congo", "COD"),
    "democratic republic of the congo": ("Democratic Republic of the Congo", "COD"),
    "djibouti": ("Djibouti", "DJI"),
    "ecuador": ("Ecuador", "ECU"),
    "egypt": ("Egypt", "EGY"),
    "el salvador": ("El Salvador", "SLV"),
    "eswatini": ("Eswatini", "SWZ"),
    "ethiopia": ("Ethiopia", "ETH"),
    "gambia": ("Gambia", "GMB"),
    "gaza strip": ("Gaza Strip", "PSG"),
    "ghana": ("Ghana", "GHA"),
    "guatemala": ("Guatemala", "GTM"),
    "guinea": ("Guinea", "GIN"),
    "guinea-bissau": ("Guinea-Bissau", "GNB"),
    "haiti": ("Haiti", "HTI"),
    "honduras": ("Honduras", "HND"),
    "indonesia": ("Indonesia", "IDN"),
    "iran": ("Iran, Islamic Republic of", "IRN"),
    "iraq": ("Iraq", "IRQ"),
    "jordan": ("Jordan", "JOR"),
    "kenya": ("Kenya", "KEN"),
    "kyrgyzstan": ("Kyrgyzstan", "KGZ"),
    "lao people's democratic republic": ("Lao People's Democratic Republic", "LAO"),
    "lebanon": ("Lebanon", "LBN"),
    "lesotho": ("Lesotho", "LSO"),
    "liberia": ("Liberia", "LBR"),
    "libya": ("Libya", "LBY"),
    "madagascar": ("Madagascar", "MDG"),
    "malawi": ("Malawi", "MWI"),
    "mali": ("Mali", "MLI"),
    "mauritania": ("Mauritania", "MRT"),
    "moldova": ("Moldova, Republic of", "MDA"),
    "mozambique": ("Mozambique", "MOZ"),
    "myanmar": ("Myanmar", "MMR"),
    "nepal": ("Nepal", "NPL"),
    "niger": ("Niger", "NER"),
    "nigeria": ("Nigeria", "NGA"),
    "pakistan": ("Pakistan", "PAK"),
    "palestine, state of": ("Palestine, State of", "PSE"),
    "philippines": ("Philippines", "PHL"),
    "rwanda": ("Rwanda", "RWA"),
    "senegal": ("Senegal", "SEN"),
    "sierra leone": ("Sierra Leone", "SLE"),
    "somalia": ("Somalia", "SOM"),
    "south sudan": ("South Sudan", "SSD"),
    "sri lanka": ("Sri Lanka", "LKA"),
    "sudan": ("Sudan", "SDN"),
    "syrian arab republic": ("Syrian Arab Republic", "SYR"),
    "tajikistan": ("Tajikistan", "TJK"),
    "tanzania, united republic of": ("Tanzania, United Republic of", "TZA"),
    "timor-leste": ("Timor-Leste", "TLS"),
    "turkey": ("Turkey", "TUR"),
    "uganda": ("Uganda", "UGA"),
    "ukraine": ("Ukraine", "UKR"),
    "venezuela, bolivarian republic of": ("Venezuela, Bolivarian Republic of", "VEN"),
    "viet nam": ("Viet Nam", "VNM"),
    "west bank": ("West Bank", "PSW"),
    "yemen": ("Yemen", "YEM"),
    "zambia": ("Zambia", "ZMB"),
    "zimbabwe": ("Zimbabwe", "ZWE"),
}

COUNTRY_CURRENCIES = {
    "Afghanistan": {"code": "AFN", "name": "Afghan Afghani"},
    "Bangladesh": {"code": "BDT", "name": "Bangladeshi Taka"},
    "Democratic Republic of the Congo": {"code": "CDF", "name": "Congolese Franc"},
    "Ethiopia": {"code": "ETB", "name": "Ethiopian Birr"},
    "Haiti": {"code": "HTG", "name": "Haitian Gourde"},
    "Kenya": {"code": "KES", "name": "Kenyan Shilling"},
    "Lebanon": {"code": "LBP", "name": "Lebanese Pound"},
    "Malawi": {"code": "MWK", "name": "Malawian Kwacha"},
    "Myanmar": {"code": "MMK", "name": "Myanmar Kyat"},
    "Nigeria": {"code": "NGN", "name": "Nigerian Naira"},
    "Pakistan": {"code": "PKR", "name": "Pakistani Rupee"},
    "Somalia": {"code": "SOS", "name": "Somali Shilling"},
    "South Sudan": {"code": "SSP", "name": "South Sudanese Pound"},
    "Sudan": {"code": "SDG", "name": "Sudanese Pound"},
    "Syrian Arab Republic": {"code": "SYP", "name": "Syrian Pound"},
    "Tanzania, United Republic of": {"code": "TZS", "name": "Tanzanian Shilling"},
    "Uganda": {"code": "UGX", "name": "Ugandan Shilling"},
    "Yemen": {"code": "YER", "name": "Yemeni Rial"},
    "Zambia": {"code": "ZMW", "name": "Zambian Kwacha"},
}


def resolve_country(country_input: str) -> Tuple[str, str]:
    candidate = str(country_input or "").strip()
    if not candidate:
        raise ValueError("Country is required")

    alias = COUNTRY_ALIASES.get(candidate.lower(), candidate)
    if len(alias) == 3 and alias.isalpha():
        return alias.upper(), alias.upper()

    lowered = alias.lower()
    if lowered in COUNTRY_NAME_TO_ISO3:
        return COUNTRY_NAME_TO_ISO3[lowered]

    raise ValueError(f"Could not resolve '{country_input}' to a supported ISO3 country code.")


def normalize_country_name(country_input: str) -> str:
    return resolve_country(country_input)[0]


def supported_country_options() -> list[dict[str, object]]:
    countries = []
    seen = set()
    for name, iso3 in COUNTRY_NAME_TO_ISO3.values():
        if name in seen:
            continue
        seen.add(name)
        currency = COUNTRY_CURRENCIES.get(name, {"code": "USD", "name": "US Dollar"})
        countries.append(
            {
                "name": name,
                "iso3": iso3,
                "currency_code": currency["code"],
                "currency_name": currency["name"],
                "has_data": True,
            }
        )
    return sorted(countries, key=lambda item: str(item["name"]))

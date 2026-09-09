# Offline MFI basemap

Natural Earth **Admin 0 Countries**, **1:10m**, **5.1.1**, default boundary dataset.
The source contains de facto country boundaries. This is geographic context, not
the authority for assessment inclusion or market identity.

- Source: https://naturalearth.s3.amazonaws.com/5.1.1/10m_cultural/ne_10m_admin_0_countries.zip
- Documentation: https://www.naturalearthdata.com/downloads/10m-cultural-vectors/10m-admin-0-countries/
- Terms: https://www.naturalearthdata.com/about/terms-of-use/ (public domain)
- Checksums, feature count and conversion-tool version: `manifest.json`.

`countries.geojson.gz` preserves source coordinates, polygon parts and interior
rings, without geometry simplification. Properties retain country names and
identifiers needed for drawing. Gzip uses a fixed timestamp and no filename.

To reproduce, download the exact source URL, verify its `source_sha256` against
the manifest, and run `python scripts/build_mfi_basemap.py path/to/archive.zip`
using the recorded pyshp version in a development environment. Compare the
resulting `asset_sha256`; no pyshp or GIS package is needed in the application.
Both files must be shipped together. The existing Docker `COPY . /app` includes
this directory. Generation never downloads cartography.

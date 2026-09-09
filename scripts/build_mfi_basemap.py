"""Convert the pinned Natural Earth archive once; pyshp is a build-only tool.

Usage: python scripts/build_mfi_basemap.py path/to/ne_10m_admin_0_countries.zip
No downloading or shapefile dependency is used by the application.
"""
import argparse
import gzip
import hashlib
import io
import json
from pathlib import Path
import zipfile


SOURCE_URL = "https://naturalearth.s3.amazonaws.com/5.1.1/10m_cultural/ne_10m_admin_0_countries.zip"
SOURCE_SHA256 = 'ce1ac7036499a0edd641fbc093cd209a98f96a49d2eca8480aaacad35138a7f6'
DESTINATION = Path(__file__).resolve().parents[1] / "app/services/mfi_drafter/assets/basemap"


def build(archive, destination=DESTINATION):
    import shapefile  # Development/conversion only; not a runtime requirement.
    raw = Path(archive).read_bytes()
    if hashlib.sha256(raw).hexdigest() != SOURCE_SHA256:
        raise ValueError('Archive differs from the pinned Natural Earth 5.1.1 source')
    with zipfile.ZipFile(io.BytesIO(raw)) as zipped:
        def component(extension):
            name = next(n for n in zipped.namelist() if n.endswith(extension))
            return io.BytesIO(zipped.read(name))
        reader = shapefile.Reader(shp=component('.shp'), shx=component('.shx'), dbf=component('.dbf'), encoding='utf-8')
        features = []
        for record in reader.iterShapeRecords():
            props = record.record.as_dict()
            codes = sorted({str(props[k]) for k in ('ISO_A3', 'ISO_A3_EH', 'ADM0_A3')
                            if isinstance(props.get(k), str) and len(props[k]) == 3 and props[k].isalpha()})
            features.append({'type': 'Feature', 'properties': {'name': props['ADMIN'], 'codes': codes,
                             'iso3': props['ISO_A3_EH'] if props['ISO_A3_EH'] in codes else None},
                             'geometry': record.shape.__geo_interface__})
    payload = json.dumps({'type': 'FeatureCollection', 'features': features},
                         ensure_ascii=False, separators=(',', ':'), allow_nan=False).encode('utf-8')
    # GzipFile avoids platform-dependent header timestamps/names.
    buffer = io.BytesIO()
    with gzip.GzipFile(fileobj=buffer, mode='wb', filename='', mtime=0, compresslevel=9) as output:
        output.write(payload)
    compressed = buffer.getvalue()
    destination.mkdir(parents=True, exist_ok=True)
    (destination / 'countries.geojson.gz').write_bytes(compressed)
    manifest = {'source': 'Natural Earth Admin 0 Countries', 'version': '5.1.1', 'scale': '1:10m',
                'source_url': SOURCE_URL, 'license_url': 'https://www.naturalearthdata.com/about/terms-of-use/',
                'source_sha256': hashlib.sha256(raw).hexdigest(),
                'asset_sha256': hashlib.sha256(compressed).hexdigest(), 'feature_count': len(features),
                'conversion': 'scripts/build_mfi_basemap.py', 'conversion_pyshp_version': shapefile.__version__,
                'coordinates': 'WGS84 longitude/latitude; source geometry retained without simplification'}
    (destination / 'manifest.json').write_text(json.dumps(manifest, indent=2) + '\n', encoding='utf-8')
    print(json.dumps(manifest, indent=2))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('archive', type=Path)
    build(parser.parse_args().archive)

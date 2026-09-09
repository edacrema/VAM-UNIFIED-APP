"""Offline map composition. All drawing belongs to the caller's Figure/Axes."""
from dataclasses import asdict
import math
import textwrap

from .map_basemap import load_basemap, map_contract, valid_location, MFICartographyError
from .visualization import MFIMapLabelInput, place_map_callouts, MAP_LABEL_MAX

MAP_WIDTH = 6.5
FONT_SIZE = 9.0


def map_data(state):
    """Join by stable identity; unique display names are a legacy-only adapter."""
    rows = state.get('markets_data', [])
    by_id = {m['market_key']: m for m in rows if m.get('market_key')}
    if len(by_id) != sum(bool(m.get('market_key')) for m in rows):
        raise MFICartographyError('Duplicate stable identity in source map records')
    by_name = {}
    for m in rows:
        by_name.setdefault(m['market_name'], []).append(m)
    records = []
    for profile in state['assessment_profile']['markets']:
        key = profile.get('market_key')
        row = by_id.get(key) if key else None
        if row is None:
            candidates = by_name.get(profile['market_name'], [])
            # Never silently substitute a different stable identity.
            candidates = [m for m in candidates if not key or not m.get('market_key')]
            if len(candidates) != 1:
                raise MFICartographyError(f"Cannot uniquely locate market identity {key or profile['market_name']!r}")
            row = candidates[0]
        records.append({'market_key': key or 'legacy:' + profile['market_name'],
                        'market_name': profile['market_name'], 'overall_mfi': profile['overall_mfi'],
                        'latitude': row.get('latitude') if valid_location(row) else None,
                        'longitude': row.get('longitude') if valid_location(row) else None,
                        'selected': profile['is_priority_market'], 'selection_order': profile['selection_order'],
                        'score_rank': profile['score_rank']})
    if len({m['market_key'] for m in records}) != len(records):
        raise MFICartographyError('Duplicate stable identity in map data')
    return {'country': state.get('country', ''), 'markets': records, 'cartography': map_contract()}


def viewport(markets):
    """The shortest longitude arc; wrapped display values never replace source coordinates."""
    longs = sorted({float(m['longitude']) % 360 for m in markets})
    gaps = [(longs[(i + 1) % len(longs)] + (360 if i == len(longs) - 1 else 0) - x, i)
            for i, x in enumerate(longs)]
    _, index = max(gaps)
    start = longs[(index + 1) % len(longs)]
    xs = [start + ((float(m['longitude']) - start) % 360) for m in markets]
    middle = (min(xs) + max(xs)) / 2
    shift = 360 * math.floor((middle + 180) / 360)
    xs = [x - shift for x in xs]
    ys = [float(m['latitude']) for m in markets]
    dx, dy = max(.1, (max(xs) - min(xs)) * .08), max(.1, (max(ys) - min(ys)) * .08)
    return xs, (min(xs) - dx, max(xs) + dx, max(-90, min(ys) - dy), min(90, max(ys) + dy))


def _paths(geometry, bounds):
    """Unwrap each ring, preserving holes, then draw only nearby longitude copies."""
    import numpy as np
    from matplotlib.path import Path
    xmin, xmax, ymin, ymax = bounds
    center = (xmin + xmax) / 2
    polygons = [geometry['coordinates']] if geometry['type'] == 'Polygon' else geometry['coordinates']
    for polygon in polygons:
        if max(p[1] for p in polygon[0]) < ymin or min(p[1] for p in polygon[0]) > ymax:
            continue
        rings = []
        outer_center = None
        for i, ring in enumerate(polygon):
            arr = np.asarray(ring, dtype=float).copy()
            arr[:, 0] = np.rad2deg(np.unwrap(np.deg2rad(arr[:, 0])))
            ring_center = (arr[:, 0].min() + arr[:, 0].max()) / 2
            desired = center if outer_center is None else outer_center
            arr[:, 0] += 360 * round((desired - ring_center) / 360)
            if outer_center is None:
                outer_center = (arr[:, 0].min() + arr[:, 0].max()) / 2
            # Nonzero fill rule: exteriors CCW, holes CW.
            area = np.sum(arr[:-1, 0] * arr[1:, 1] - arr[1:, 0] * arr[:-1, 1])
            if (area < 0) == (i == 0):
                arr = arr[::-1].copy()
            rings.append(arr)
        for offset in (-360, 0, 360):
            if rings[0][:, 0].max() + offset < xmin or rings[0][:, 0].min() + offset > xmax:
                continue
            vertices, codes = [], []
            for ring in rings:
                ring = ring.copy()
                ring[:, 0] += offset
                vertices.extend(ring)
                codes.extend([Path.MOVETO] + [Path.LINETO] * (len(ring) - 2) + [Path.CLOSEPOLY])
            yield Path(vertices, codes)


def draw_map(fig, data):
    from matplotlib.patches import PathPatch
    from matplotlib.ticker import FuncFormatter, MaxNLocator
    from app.shared.countries import resolve_country
    manifest, features = load_basemap()
    if data['cartography'] != map_contract(manifest):
        raise MFICartographyError('Map job and worker cartographic versions differ')
    markets = data['markets']
    located = [m for m in markets if valid_location(m)]
    selected = sorted((m for m in markets if m['selected']), key=lambda m: (m['selection_order'], m['market_key']))[:MAP_LABEL_MAX]
    orders = [m['selection_order'] for m in selected]
    if len(set(orders)) != len(orders) or any(n < 1 for n in orders):
        raise MFICartographyError('Selected markets require unique positive selection numbers')
    if not located:
        raise MFICartographyError('No valid coordinates for geographic_map')
    limitations = []
    try:
        _, iso3 = resolve_country(data['country'])
    except ValueError:
        # The shared resolver intentionally supports a smaller operational list.
        # Exact Natural Earth names extend map coverage without changing that list.
        matches = [f for f in features if f['properties']['name'].casefold() == str(data['country']).strip().casefold()]
        iso3 = matches[0]['properties'].get('iso3') if len(matches) == 1 else None
    known = iso3 and any(iso3 in f['properties']['codes'] for f in features)
    if not known:
        limitations.append('Country outline could not be identified; background follows market coordinates.')
    legend = []
    for market in selected:
        label = f"{market['selection_order']}. {market['market_name']}"
        if not valid_location(market):
            label += ' (coordinates unavailable)'
        legend.append((market, textwrap.wrap(label, width=27, break_long_words=True, break_on_hyphens=False)))
    legend_lines = sum(len(lines) for _, lines in legend)
    xs, bounds = viewport(located)
    xmin, xmax, ymin, ymax = bounds
    aspect = 1 / max(math.cos(math.radians((ymin + ymax) / 2)), .001)
    map_height = MAP_WIDTH * .50 * (ymax - ymin) / (xmax - xmin) * aspect
    legend_height = (legend_lines * 11 + len(legend) * 4 + 42) / 72
    height = max(4.8, min(6.0, map_height) + 2.2, legend_height + 2.2)
    fig.set_size_inches(MAP_WIDTH, height)
    # Dedicated axes keep the map, legend and colour bar independent. No tight_layout afterwards.
    panel_bottom, panel_height = 1.3 / height, (height - 2.2) / height
    ax = fig.add_axes([.14, panel_bottom, .50, panel_height])
    legend_ax = fig.add_axes([.69, panel_bottom, .295, panel_height])
    legend_ax.set_axis_off()
    ax.set(xlim=(xmin, xmax), ylim=(ymin, ymax), facecolor='#EAF3F7')
    ax.set_aspect(aspect, adjustable='box', anchor='N')
    outlines, highlighted = [], []
    for feature in features:
        highlight = bool(known and iso3 in feature['properties']['codes'])
        paths = list(_paths(feature['geometry'], bounds))
        if not paths:
            continue
        outlines.append(feature['properties']['name'])
        if highlight:
            highlighted.append(feature['properties']['name'])
        for path in paths:
            ax.add_patch(PathPatch(path, facecolor='#F6F2E8' if highlight else '#F1F0EC',
                                  edgecolor='#58686E' if highlight else '#A7AFAE',
                                  linewidth=.85 if highlight else .45, zorder=1 if highlight else 0))
    values = [m['overall_mfi'] for m in located]
    scatter = ax.scatter(xs, [m['latitude'] for m in located], c=values, cmap='Blues', vmin=0, vmax=10,
                         edgecolors='#243A48', linewidths=.6, s=[34 if m['selected'] else 22 for m in located], zorder=3)
    ax.grid(alpha=.15, linewidth=.5)
    ax.tick_params(labelsize=8)
    ax.xaxis.set_major_locator(MaxNLocator(nbins=4))
    ax.yaxis.set_major_locator(MaxNLocator(nbins=6))
    ax.xaxis.set_major_formatter(FuncFormatter(lambda x, pos: f'{(x + 180) % 360 - 180:g}°'))
    ax.yaxis.set_major_formatter(FuncFormatter(lambda y, pos: f'{y:g}°'))
    ax.set_xlabel('Longitude', fontsize=9)
    ax.set_ylabel('Latitude', fontsize=9)
    fig.canvas.draw()
    color_y = ax.get_position().y0 - .54 / height
    color_ax = fig.add_axes([.14, color_y, .50, .12 / height])
    bar = fig.colorbar(scatter, cax=color_ax, orientation='horizontal', ticks=[0, 2, 4, 6, 8, 10])
    bar.ax.tick_params(labelsize=8)
    bar.set_label('Stored MFI score (0–10)', fontsize=9)
    fig.text(.095, 1 - .22 / height, 'Assessed-market MFI scores', fontsize=12, weight='bold', va='top')
    fig.text(.095, 1 - .49 / height, str(data['country']), fontsize=10, va='top')
    fig.text(.095, .12 / height, 'Natural Earth 1:10m · v5.1.1 | WGS84 · equirectangular view', fontsize=8)
    legend_ax.text(0, 1, 'Selected markets', fontsize=FONT_SIZE, weight='bold', va='top')
    legend_height_pt = (height - 2.2) * 72
    legend_ax.text(0, 1 - 16 / legend_height_pt, 'Numbers follow selection order', fontsize=8, va='top')
    y = 1 - 36 / legend_height_pt
    for _, lines in legend:
        legend_ax.text(0, y, '\n'.join(lines), fontsize=FONT_SIZE, va='top', linespacing=1.22)
        y -= (len(lines) * 11 + 4) / legend_height_pt
    if not selected:
        legend_ax.text(0, y, 'No selected markets', fontsize=FONT_SIZE, va='top')
    if len(located) != len(markets):
        limitations.append(f"Coordinates available for {len(located)}/{len(markets)} assessed markets.")
    by_key = {m['market_key']: (m, x) for m, x in zip(located, xs)}
    inputs = [MFIMapLabelInput(m['market_name'], by_key[m['market_key']][1], float(m['latitude']),
                              m['selection_order'], m['score_rank'], market_key=m['market_key'])
              for m in selected if m['market_key'] in by_key]
    placements = place_map_callouts(ax, inputs, use_selection_numbers=True)
    for p in placements:
        # Leaders are a separate lower layer, so none can strike through another number.
        ax.annotate('', (p.longitude, p.latitude), xytext=p.offset_points, textcoords='offset points',
                    arrowprops={'arrowstyle': '-', 'color': '#52646E', 'linewidth': .6, 'shrinkA': 3, 'shrinkB': 3}, zorder=4)
        ax.annotate(str(p.number), (p.longitude, p.latitude), xytext=p.offset_points,
                    textcoords='offset points', ha='center', va='center', fontsize=9,
                    bbox={'boxstyle': 'circle,pad=.20', 'facecolor': 'white', 'edgecolor': '#34454F', 'linewidth': .7},
                    zorder=5)
    return {'labels': [m['market_name'] for m in located], 'values': values, 'title': 'Assessed-market MFI scores',
            **data['cartography'], 'country_iso3': iso3, 'background_countries': sorted(set(outlines)),
            'highlighted_countries': sorted(set(highlighted)), 'extent': list(bounds),
            'located_market_count': len(located), 'total_market_count': len(markets),
            'source_coordinates': [{'market_key': m['market_key'], 'longitude': m['longitude'], 'latitude': m['latitude']} for m in located],
            'callouts': [asdict(p) for p in placements],
            'legend': [{'number': m['selection_order'], 'market_key': m['market_key'], 'market_name': m['market_name'],
                        'located': valid_location(m)} for m in selected],
            'limitations': limitations, 'figure_width_inches': MAP_WIDTH, 'legend_font_points': FONT_SIZE}

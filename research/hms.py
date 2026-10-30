"""
Minimal, dependency-free reader for NOAA HMS smoke polygon shapefiles (zip with .shp + .dbf),
and point-in-polygon lookup. Avoids needing geopandas/shapely.

density classes: Light = 1, Medium = 2, Heavy = 3 (0 = no smoke polygon covers the point).
"""
import io, struct, zipfile
import numpy as np

DENSITY = {"light": 1, "medium": 2, "heavy": 3}


def _read_dbf(b):
    n_rec, hdr_len, rec_len = struct.unpack("<IHH", b[4:12])
    fields, pos = [], 32
    while b[pos] != 0x0D:
        name = b[pos:pos + 11].split(b"\x00")[0].decode("latin-1")
        ftype, flen = chr(b[pos + 11]), b[pos + 16]
        fields.append((name, ftype, flen)); pos += 32
    rows = []
    for i in range(n_rec):
        off = hdr_len + i * rec_len + 1          # skip deletion flag
        rec = {}
        for name, _, flen in fields:
            rec[name] = b[off:off + flen].decode("latin-1").strip(); off += flen
        rows.append(rec)
    return rows


def _read_shp(b):
    """Return a list of records; each record is a list of rings (Nx2 arrays of lon, lat)."""
    shapes, pos = [], 100
    while pos + 8 <= len(b):
        _, clen = struct.unpack(">ii", b[pos:pos + 8]); pos += 8
        content = b[pos:pos + clen * 2]; pos += clen * 2
        stype = struct.unpack("<i", content[0:4])[0]
        if stype == 0:
            shapes.append([]); continue
        if stype not in (5, 15, 25):
            raise ValueError(f"unsupported shape type {stype}")
        nparts, npts = struct.unpack("<ii", content[36:44])
        parts = list(struct.unpack(f"<{nparts}i", content[44:44 + 4 * nparts]))
        pts = np.frombuffer(content[44 + 4 * nparts:44 + 4 * nparts + 16 * npts], dtype="<f8").reshape(npts, 2)
        bounds = parts + [npts]
        shapes.append([pts[bounds[k]:bounds[k + 1]] for k in range(nparts)])
    return shapes


def read_hms_zip(path_or_bytes):
    """Return list of (density_class, rings) for one day's HMS file."""
    z = zipfile.ZipFile(path_or_bytes if not isinstance(path_or_bytes, bytes) else io.BytesIO(path_or_bytes))
    names = z.namelist()
    shp = next(n for n in names if n.lower().endswith(".shp"))
    dbf = next(n for n in names if n.lower().endswith(".dbf"))
    shapes, recs = _read_shp(z.read(shp)), _read_dbf(z.read(dbf))
    out = []
    for rings, rec in zip(shapes, recs):
        dens = next((v for k, v in rec.items() if k.lower() == "density"), "")
        out.append((DENSITY.get(dens.lower(), 1), rings))     # unknown density -> treat as light
    return out


def _points_in_ring(px, py, ring):
    x, y = ring[:, 0], ring[:, 1]
    x2, y2 = np.roll(x, -1), np.roll(y, -1)
    inside = np.zeros(len(px), dtype=bool)
    for xa, ya, xb, yb in zip(x, y, x2, y2):            # ray casting, vectorised over points
        cond = (ya > py) != (yb > py)
        with np.errstate(divide="ignore", invalid="ignore"):
            xint = (xb - xa) * (py - ya) / (yb - ya) + xa
        inside ^= cond & (px < xint)
    return inside


def max_density_at(points_lon, points_lat, polygons):
    """Highest smoke density class covering each point (even-odd rule across a polygon's rings)."""
    px, py = np.asarray(points_lon, float), np.asarray(points_lat, float)
    best = np.zeros(len(px), dtype=int)
    for dens, rings in polygons:
        if not rings:
            continue
        allpts = np.vstack(rings)
        bx = (px >= allpts[:, 0].min()) & (px <= allpts[:, 0].max()) & (py >= allpts[:, 1].min()) & (py <= allpts[:, 1].max())
        if not bx.any():
            continue
        inside = np.zeros(len(px), dtype=bool)
        for ring in rings:
            inside ^= _points_in_ring(px, py, ring)
        best = np.where(inside & bx, np.maximum(best, dens), best)
    return best


def write_test_shapefile_zip(polygons):
    """Write [(density_name, ring_array), ...] as a zip (.shp/.shx/.dbf). Used only by the tests."""
    recs = []
    for _, ring in polygons:
        ring = np.asarray(ring, float)
        body = struct.pack("<i", 5) + struct.pack("<4d", ring[:, 0].min(), ring[:, 1].min(), ring[:, 0].max(), ring[:, 1].max())
        body += struct.pack("<ii", 1, len(ring)) + struct.pack("<i", 0) + ring.astype("<f8").tobytes()
        recs.append(body)
    shp = io.BytesIO(); shp.write(b"\x00" * 100)
    for i, body in enumerate(recs):
        shp.write(struct.pack(">ii", i + 1, len(body) // 2)); shp.write(body)
    shp_b = bytearray(shp.getvalue())
    shp_b[0:4] = struct.pack(">i", 9994); shp_b[24:28] = struct.pack(">i", len(shp_b) // 2)
    shp_b[28:32] = struct.pack("<i", 1000); shp_b[32:36] = struct.pack("<i", 5)
    flen = 8
    dbf = bytearray(struct.pack("<BBBBIHH", 3, 120, 1, 1, len(polygons), 32 + 32 + 1, 1 + flen) + b"\x00" * 20)
    dbf += b"Density".ljust(11, b"\x00") + b"C" + b"\x00" * 4 + bytes([flen]) + b"\x00" * 15 + b"\x0D"
    for dens, _ in polygons:
        dbf += b" " + dens.encode().ljust(flen)
    dbf += b"\x1A"
    z = io.BytesIO()
    with zipfile.ZipFile(z, "w") as zf:
        zf.writestr("hms_smoke.shp", bytes(shp_b)); zf.writestr("hms_smoke.dbf", bytes(dbf)); zf.writestr("hms_smoke.shx", b"")
    return z.getvalue()

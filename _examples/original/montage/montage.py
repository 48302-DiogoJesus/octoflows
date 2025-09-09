#!/usr/bin/env python3
"""
montage_workflow_win.py — Windows-friendly Montage-like FITS mosaicking workflow.

Simulates the classic Montage steps using Astropy + Reproject:
 1) mImgtbl    — Scan directory, collect WCS metadata
 2) mMakeHdr   — Compute mosaic header (WCS + size)
 3) mProjExec  — Reproject all images to mosaic WCS
 4) mBgModel   — Estimate simple background offsets
 5) mBgExec    — Apply offsets
 6) mAdd       — Co-add corrected images into mosaic

Usage:
    python montage.py ./inputs/1 ./outputs/1
"""

from astropy import units as u
import argparse
from pathlib import Path
import numpy as np
from astropy.io import fits
from astropy.wcs import WCS
from reproject import reproject_interp
from reproject.mosaicking import find_optimal_celestial_wcs
from astropy.visualization import ZScaleInterval, AsinhStretch, ImageNormalize
import matplotlib.pyplot as plt
from tqdm import tqdm
import sys
from io import BytesIO
import cloudpickle

# ---------- Step 1: mImgtbl ----------
def scan_fits(input_dir: Path, pattern: str):
    files = sorted(input_dir.rglob(pattern))
    rows = []
    for f in files:
        # Read file as binary
        with open(f, 'rb') as bf:
            binary_data = cloudpickle.dumps(bf.read())

        # Open FITS from binary
        with BytesIO(cloudpickle.loads(binary_data)) as bio:
            with fits.open(bio) as hdul:
                hdu = hdul[0]
                if hdu.data is None:
                    continue
                wcs = WCS(hdu.header).celestial
                rows.append((f, hdu.data, wcs))
    return rows



# ---------- Step 2: mMakeHdr ----------
def make_header(rows, pixscale=None):
    datasets = [(np.nan_to_num(data, nan=0.0), wcs) for _, data, wcs in rows]
    if pixscale:
        # pixscale is arcsec/pixel, convert to Quantity
        resolution = pixscale * u.arcsec
        target_wcs, shape_out = find_optimal_celestial_wcs(datasets, resolution=resolution)
    else:
        target_wcs, shape_out = find_optimal_celestial_wcs(datasets)
    return target_wcs, shape_out


# ---------- Step 3: mProjExec ----------
def reproject_images(rows, target_wcs, shape_out):
    reprojected = []
    for f, data, wcs in tqdm(rows, desc="Reprojecting"):
        array, _ = reproject_interp((data, wcs), target_wcs, shape_out=shape_out)
        reprojected.append((f, array))
    return reprojected


# ---------- Step 4 + 5: mBgModel + mBgExec ----------
def background_correction(reprojected):
    corrected = []
    for f, arr in reprojected:
        med = np.nanmedian(arr)
        corrected.append((f, arr - med))
    return corrected


# ---------- Step 6: mAdd ----------
def coadd(corrected, method="median"):
    stack = np.stack([arr for _, arr in corrected], axis=0)
    if method == "median":
        return np.nanmedian(stack, axis=0)
    elif method == "mean":
        return np.nanmean(stack, axis=0)
    elif method == "sum":
        return np.nansum(stack, axis=0)
    else:
        raise ValueError("Invalid method. Choose median, mean, or sum.")


# ---------- Save FITS + PNG ----------
def save_results(mosaic, target_wcs, out_dir: Path):
    # PNG
    png_fp = out_dir / "mosaic.png"
    z = ZScaleInterval()
    vmin, vmax = z.get_limits(mosaic)
    norm = ImageNormalize(vmin=vmin, vmax=vmax, stretch=AsinhStretch())
    plt.figure(figsize=(10, 10))
    plt.imshow(mosaic, origin="lower", cmap="gray", norm=norm)
    plt.axis("off")
    plt.savefig(png_fp, dpi=200, bbox_inches="tight", pad_inches=0)
    plt.close()

    print(f"[OUTPUT] PNG:  {png_fp}")


parser = argparse.ArgumentParser(description="Windows-friendly Montage workflow clone")
input_dir = sys.argv[1]
output_dir = sys.argv[2]
pattern = "*.fits"
pixscale = None
method = "median"

out_dir = Path(output_dir)
out_dir.mkdir(parents=True, exist_ok=True)

# 1) Scan
rows = scan_fits(Path(input_dir), pattern)
print(f"[mImgtbl] Found {len(rows)} images")

# 2) Make header
target_wcs, shape_out = make_header(rows, pixscale)
print(f"[mMakeHdr] Mosaic shape {shape_out}")

# 3) Reproject
reproj = reproject_images(rows, target_wcs, shape_out)

# 4+5) Background correction
corrected = background_correction(reproj)

# 6) Co-add
mosaic = coadd(corrected, method)
print("[mAdd] Mosaic built")

# Save results
save_results(mosaic, target_wcs, out_dir)

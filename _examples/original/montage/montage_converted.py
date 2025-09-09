#!/usr/bin/env python3
"""
parallel_montage_workflow.py — Parallelized Montage-like FITS mosaicking workflow using DAG tasks.

This version handles binary data directly instead of file paths:
 1) scan_fits_task          — Scan directory, collect file binary data
 2) extract_wcs_task        — Extract WCS from binary data (parallel)
 3) compute_mosaic_header   — Compute optimal mosaic header/WCS
 4) reproject_single_task   — Reproject individual images from binary data (parallel)
 5) background_correct_task — Apply background correction to individual images (parallel)
 6) coadd_task             — Co-add all corrected images into final mosaic
 7) save_results_task      — Save FITS and PNG outputs

Usage Examples:
    python montage_converted.py simple ./inputs/1 ./outputs/1

"""

from astropy import units as u
from pathlib import Path
import numpy as np
from astropy.io import fits
from astropy.wcs import WCS
from reproject import reproject_interp
from reproject.mosaicking import find_optimal_celestial_wcs
from astropy.visualization import ZScaleInterval, AsinhStretch, ImageNormalize
import matplotlib.pyplot as plt
from typing import List, Tuple, Dict, Any
import os
import sys
from io import BytesIO

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..")))
from src.dag_task_node import DAGTask
from src.utils.logger import create_logger

# Import common worker configurations
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from common.config import WORKER_CONFIG

logger = create_logger(__name__)

def _local_scan_fits_task(input_dir: str) -> List[bytes]:
    """
    Scan directory for FITS files matching pattern and load their binary data.
    Returns list of dicts with filename and binary data.
    """
    input_path = Path(input_dir)
    files = sorted(input_path.rglob("*.fits"))
    
    file_data_list: List[bytes] = []
    for file_path in files:
        with open(file_path, 'rb') as f:
            binary_data = f.read()
        
        file_data_list.append(binary_data)
    
    return file_data_list

@DAGTask
def extract_wcs_task(file_data: bytes) -> Dict[str, Any]:
    """
    Extract WCS and basic metadata from binary FITS data.
    Returns dict with file info or None if file is invalid.
    """
    # Create BytesIO object from binary data
    fits_bytes = BytesIO(file_data)

    with fits.open(fits_bytes) as hdul:
        hdu = hdul[0]
        if hdu.data is None:
            raise ValueError("Invalid FITS file: no data")
        
        wcs = WCS(hdu.header).celestial

        # Store essential info (don't store full data array to save memory)
        return {
            'binary_data': file_data,  # Keep binary data for later tasks
            'shape': hdu.data.shape,
            'wcs_header': wcs.to_header()
        }


@DAGTask
def compute_mosaic_header(wcs_metadata: List[Dict], pixscale: float | None = None) -> Tuple[str, Tuple[int, int]]:
    """
    Compute optimal mosaic WCS and shape from all valid WCS metadata.
    Returns serialized target_wcs header and output shape.
    """
    datasets = []
    for meta in wcs_metadata:
        # Create dummy data array for WCS computation (we only need shapes)
        dummy_data = np.ones(meta['shape'])
        wcs = WCS(meta['wcs_header'])
        datasets.append((dummy_data, wcs))
    
    if pixscale:
        resolution = pixscale * u.arcsec
        target_wcs, shape_out = find_optimal_celestial_wcs(datasets, resolution=resolution)
    else:
        target_wcs, shape_out = find_optimal_celestial_wcs(datasets)

    # Return serialized WCS header and shape
    return target_wcs.to_header().tostring(), shape_out


@DAGTask
def reproject_single_task(file_metadata: Dict[str, Any], mosaic_header: Tuple[str, Tuple[int, int]]) -> np.ndarray:
    """
    Reproject a single FITS image from binary data to target WCS.
    Returns dict with reprojected data and metadata.
    """
    target_wcs_header, shape_out = mosaic_header

    # Reconstruct target WCS
    target_wcs = WCS(fits.Header.fromstring(target_wcs_header))
    
    # Load data from binary
    fits_bytes = BytesIO(file_metadata['binary_data'])
    with fits.open(fits_bytes) as hdul:
        data = hdul[0].data
        wcs = WCS(hdul[0].header).celestial
    
    reprojected_data, _ = reproject_interp((data, wcs), target_wcs, shape_out=shape_out)
    
    return reprojected_data.astype(np.float32)


@DAGTask
def background_correct_task(reprojected_data: np.ndarray) -> np.ndarray:
    """
    Apply background correction to a single reprojected image.
    Returns background-corrected data.
    """
    
    # Simple median background subtraction
    median_bg = np.nanmedian(reprojected_data)
    corrected_data = reprojected_data - median_bg
    
    return corrected_data.astype(np.float32)


@DAGTask
def coadd_task(corrected_data_list: List[np.ndarray], method: str = "median") -> np.ndarray:
    """
    Co-add all background-corrected images into final mosaic.
    Returns the final mosaic array.
    """
    # Extract arrays
    
    # Stack and combine
    stack = np.stack(corrected_data_list, axis=0)
    
    if method == "median":
        mosaic = np.nanmedian(stack, axis=0)
    elif method == "mean":
        mosaic = np.nanmean(stack, axis=0)
    elif method == "sum":
        mosaic = np.nansum(stack, axis=0)
    else:
        raise ValueError("Invalid method. Choose median, mean, or sum.")
    
    return mosaic


def _local_save_results_task(mosaic: np.ndarray, output_dir: str):
    """
    Save final mosaic as FITS and PNG files and display the image.
    Returns dict with output file paths.
    """
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    
    # Save PNG
    png_path = out_dir / "mosaic.png"
    z = ZScaleInterval()
    vmin, vmax = z.get_limits(mosaic)
    norm = ImageNormalize(vmin=vmin, vmax=vmax, stretch=AsinhStretch())
    
    plt.figure(figsize=(10, 10))
    plt.imshow(mosaic, origin="lower", cmap="gray", norm=norm)
    plt.axis("off")
    plt.savefig(png_path, dpi=200, bbox_inches="tight", pad_inches=0)
    
    plt.show()


input_dir = sys.argv[2]
output_dir = sys.argv[3]
PIXSCALE = 0.4
METHOD = "median"

# Task 1: Scan and load all FITS files into memory
file_data_list = _local_scan_fits_task(input_dir)
print(f"[INFO] Loaded {len(file_data_list)} FITS files")

# Task 2: Extract WCS from each file (parallel)
wcs_metadata = []
for file_data in file_data_list:
    wcs_meta = extract_wcs_task(file_data)
    wcs_metadata.append(wcs_meta)

# Task 3: Compute mosaic header
mosaic_header_out = compute_mosaic_header(wcs_metadata, PIXSCALE)

# Task 4: Reproject each image (parallel)
reprojected_data_list = []
for wcs_meta in wcs_metadata:
    reproj_data = reproject_single_task(wcs_meta, mosaic_header_out)
    reprojected_data_list.append(reproj_data)

# Task 5: Background correction (parallel)
corrected_data_list = []
for reproj_data in reprojected_data_list:
    corr_data = background_correct_task(reproj_data)
    corrected_data_list.append(corr_data)

# Task 6: Co-add all images
final_mosaic = coadd_task(corrected_data_list, METHOD)

final_mosaic = final_mosaic.compute(dag_name="montage", config=WORKER_CONFIG, open_dashboard=False)

# Task 7: Save results Locally
# _local_save_results_task(final_mosaic, output_dir)
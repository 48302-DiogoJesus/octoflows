from astropy import units as u
from pathlib import Path
import numpy as np
from astropy.io import fits
from astropy.wcs import WCS
from reproject import reproject_interp
from reproject.mosaicking import find_optimal_celestial_wcs
from astropy.visualization import ZScaleInterval, AsinhStretch, ImageNormalize
import matplotlib.pyplot as plt
from typing import List, Tuple
import os
import sys
import time

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from src.dag_task_node import DAGTask
from src.utils.logger import create_logger

# Import common worker configurations
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from common.config import WORKER_CONFIG

logger = create_logger(__name__)

def _local_scan_fits_task(input_dir: str) -> List[bytes]:
    """
    Scan directory for FITS files matching pattern and load their binary data.
    Returns list of binary data.
    """
    input_path = Path(input_dir)
    files = sorted(input_path.rglob("*.fits"))
    
    file_data_list: List[bytes] = []
    for file_path in files:
        with open(file_path, 'rb') as f:
            binary_data = f.read()
        
        file_data_list.append(binary_data)
    
    return file_data_list

def _util_fits_bytes_to_tmp_file(file_data: bytes) -> Path:
    """Create temporary file from FITS binary data."""
    tmp_path = Path.cwd() / f"tmp_fits_{os.getpid()}_{id(file_data)}.fits"
    with open(tmp_path, 'wb') as tmp:
        tmp.write(file_data)
    return tmp_path

def _compute_global_wcs(file_data_list: List[bytes], pixscale: float | None = None) -> Tuple[WCS, Tuple[int, int]]:
    """
    Compute optimal mosaic WCS from all FITS files.
    This is called locally before creating parallel tasks.
    """
    datasets = []
    
    for file_data in file_data_list:
        tmp_path = None
        try:
            tmp_path = _util_fits_bytes_to_tmp_file(file_data)
            with fits.open(tmp_path) as hdul:
                hdu = hdul[0]
                if hdu.data is not None:
                    wcs = WCS(hdu.header).celestial
                # Use dummy data for WCS computation
                dummy_data = np.ones(hdu.data.shape)
                datasets.append((dummy_data, wcs))
        finally:
            if tmp_path and tmp_path.exists():
                tmp_path.unlink()
    
    if not datasets:
        raise ValueError("No valid FITS files found")
    
    if pixscale:
        resolution = pixscale * u.arcsec
        target_wcs, shape_out = find_optimal_celestial_wcs(datasets, resolution=resolution)
    else:
        target_wcs, shape_out = find_optimal_celestial_wcs(datasets)
    
    return target_wcs, shape_out

@DAGTask
def extract_and_reproject_task(file_data: bytes, target_wcs_header: str, shape_out: Tuple[int, int]) -> np.ndarray:
    """
    Extract WCS from binary FITS data and reproject to target WCS.
    Returns reprojected array.
    """
    # Write bytes to temporary file
    tmp_path = _util_fits_bytes_to_tmp_file(file_data)

    # Open FITS file and extract data/WCS
    with fits.open(tmp_path) as hdul:
        hdu = hdul[0]
        if hdu.data is None:
            logger.info(f"Invalid FITS file: no data")
            raise ValueError("Invalid FITS file: no data")

        data = hdu.data
        wcs = WCS(hdu.header).celestial

    # Reconstruct target WCS
    target_wcs = WCS(fits.Header.fromstring(target_wcs_header))
    
    # Reproject
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

# Configuration
workload = "light"
if len(sys.argv) > 3:
    workload = sys.argv[3]
    if workload not in ["light", "heavy"]:
        print(f"Invalid workload: {workload}. Accepted: 'light' or 'heavy'")
        sys.exit(-1)
else:
    print("No workload specified, defaulting to 'light'")
    workload = "light"

if workload == "light":
    input_dir = "../_inputs/montage_light/"
    output_dir = "../_outputs/montage_light/"
else:
    input_dir = "../_inputs/montage_heavy/"
    output_dir = "../_outputs/montage_heavy/"
PIXSCALE = 0.4
METHOD = "median"

# Task 1: Scan and load all FITS files into memory
file_data_list = _local_scan_fits_task(input_dir)
assert len(file_data_list) > 0, "No FITS files found in input directory"
print(f"[INFO] Loaded {len(file_data_list)} FITS files")

# Task 2: Compute global WCS locally (not a DAG task)
target_wcs, shape_out = _compute_global_wcs(file_data_list, PIXSCALE)
target_wcs_header = target_wcs.to_header().tostring()
print(f"[INFO] Computed target WCS with shape: {shape_out}")

# Task 3: Extract WCS and reproject each image (parallel)
reprojected_data_list = []
for file_data in file_data_list:
    reproj_data = extract_and_reproject_task(file_data, target_wcs_header, shape_out)
    reprojected_data_list.append(reproj_data)

# Task 4: Background correction (parallel)
corrected_data_list = []
for reproj_data in reprojected_data_list:
    corr_data = background_correct_task(reproj_data)
    corrected_data_list.append(corr_data)

# Task 5: Co-add all images
final_mosaic = coadd_task(corrected_data_list, METHOD)

# final_mosaic.visualize_dag(output_file=os.path.join("_dag_visualization", "montage"), open_after=False)

# Compute the DAG
start_time = time.time()
print(f"Starting Workflow | Workload Type: {workload}")
final_mosaic = final_mosaic.compute(dag_name="montage", config=WORKER_CONFIG, open_dashboard=False)
print(f"User waited: {time.time() - start_time:.3f}s")

# Task 6: Save results locally
# _local_save_results_task(final_mosaic, output_dir)
import os
import sys
import io
import numpy as np
from PIL import Image, ImageFilter, ImageOps

# Add parent directory to path to allow importing from src
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from src.dag_task_node import DAGTask
from src.planning.annotations.preload import PreLoadOptimization

# Import centralized configuration
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from common.config import WORKER_CONFIG


def split_image_into_chunks(image: Image.Image, num_chunks: int) -> list[Image.Image]:
    """Split image into roughly equal vertical chunks"""
    width, height = image.size
    chunk_width = width // num_chunks
    chunks = []
    
    for i in range(num_chunks):
        left = i * chunk_width
        right = (i + 1) * chunk_width if i < num_chunks - 1 else width
        chunk = image.crop((left, 0, right, height))
        chunks.append(chunk)
    
    return chunks


def combine_image_chunks(chunks: list[Image.Image]) -> Image.Image:
    """Combine image chunks horizontally"""
    widths, heights = zip(*(chunk.size for chunk in chunks))
    total_width = sum(widths)
    max_height = max(heights)
    
    new_image = Image.new('RGB', (total_width, max_height))
    
    x_offset = 0
    for chunk in chunks:
        new_image.paste(chunk, (x_offset, 0))
        x_offset += chunk.size[0]
    
    return new_image


def add_border_to_image(image: Image.Image, border_width: int = 2, border_color: str = "black") -> Image.Image:
    return ImageOps.expand(image, border=border_width, fill=border_color)


@DAGTask
def determine_chunks_amount(image_data: bytes) -> int:
    image = Image.open(io.BytesIO(image_data)).convert("RGB")
    width, _ = image.size
    return min(width // 64, 10)


def split_image(image_data: bytes, num_chunks: int) -> list[bytes]:
    """Split the image into chunks and return as list of bytes"""
    image = Image.open(io.BytesIO(image_data))
    chunks = split_image_into_chunks(image, num_chunks)
    
    chunk_bytes = []
    for chunk in chunks:
        byte_arr = io.BytesIO()
        chunk.save(byte_arr, format=image.format)
        chunk_bytes.append(byte_arr.getvalue())
    
    return chunk_bytes


@DAGTask
def grayscale_image_part(chunk_data: bytes) -> bytes:
    """Convert a single image chunk to grayscale using the luminance formula:
    Y = 0.2125 * R + 0.7154 * G + 0.0721 * B
    """
    # Open the image chunk
    image = Image.open(io.BytesIO(chunk_data))
    
    # Convert to RGB if not already
    if image.mode != 'RGB':
        image = image.convert('RGB')
    
    # Convert to numpy array for processing
    img_array = np.array(image)
    
    # Apply grayscale conversion using the luminance formula
    grayscale_array = (
        img_array[..., 0] * 0.2125 +
        img_array[..., 1] * 0.7154 +
        img_array[..., 2] * 0.0721
    ).astype('uint8')
    
    # Convert back to PIL Image
    grayscale_image = Image.fromarray(grayscale_array, 'L')
    
    # Add border for visualization
    # grayscale_image = add_border_to_image(grayscale_image)
    
    # Convert to bytes and return
    byte_arr = io.BytesIO()
    grayscale_image.save(byte_arr, format=image.format)
    return byte_arr.getvalue()


@DAGTask
def blur_image_part(chunk_data: bytes, blur_radius: int = 3) -> bytes:
    """Apply Gaussian blur to an image chunk"""
    image = Image.open(io.BytesIO(chunk_data))
    
    # Apply Gaussian blur
    blurred_img = image.filter(ImageFilter.GaussianBlur(radius=blur_radius))
    
    # Add border for visualization
    # blurred_img = add_border_to_image(blurred_img)
    
    # Save to bytes and return
    byte_arr = io.BytesIO()
    blurred_img.save(byte_arr, format=image.format)
    return byte_arr.getvalue()


@DAGTask(forced_optimizations=[PreLoadOptimization()])
def merge_image_parts(processed_chunks: list[bytes]) -> bytes:
    """Combine processed image chunks back into one image"""
    images = [Image.open(io.BytesIO(chunk)) for chunk in processed_chunks]
    combined = combine_image_chunks(images)
    
    byte_arr = io.BytesIO()
    combined.save(byte_arr, format=images[0].format)
    return byte_arr.getvalue()


# Read the input image
image_data: bytes = open("../_inputs/test_image_1.jpg", "rb").read()

num_chunks = 8

chunks = split_image(image_data, num_chunks)

processed_chunks = []
for chunk in chunks:
    blurred = blur_image_part(chunk, blur_radius=2)
    grayscaled = grayscale_image_part(blurred)
    processed_chunks.append(grayscaled)


final_image = merge_image_parts(processed_chunks)

# final_image.visualize_dag(output_file=os.path.join("_dag_visualization", "image_processing_merge"), open_after=False)

final_image = final_image.compute(dag_name="image_processing_merge", config=WORKER_CONFIG)

# image = Image.open(io.BytesIO(final_image))
# image.show()

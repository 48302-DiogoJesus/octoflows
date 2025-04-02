import asyncio
import os
import sys
from PIL import Image, ImageFilter, ImageOps
import numpy as np
import io
from typing import List, Tuple


sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from src.storage.metrics.metrics_storage import MetricsStorage
from src.storage.in_memory_storage import InMemoryStorage
from src.storage.redis_storage import RedisStorage
from src.worker import DockerWorker, LocalWorker
from src.dag_task_node import DAGTask, DAGTaskNode

# INTERMEDIATE STORAGE
redis_intermediate_storage_config = RedisStorage.Config(
   host="localhost", port=6379, password="redisdevpwd123"
)
# INTERMEDIATE STORAGE
inmemory_intermediate_storage_config = InMemoryStorage.Config()

# METRICS STORAGE
redis_metrics_storage_config = RedisStorage.Config(
   host="localhost", port=6380, password="redisdevpwd123"
)

localWorkerConfig = LocalWorker.Config(
    intermediate_storage_config=redis_intermediate_storage_config,
    metadata_storage_config=redis_intermediate_storage_config,  # will use the same as intermediate_storage_config
    metrics_storage_config=MetricsStorage.Config(storage_config=redis_metrics_storage_config, upload_strategy=MetricsStorage.UploadStrategy.AFTER_EACH_TASK)
)

dockerWorkerConfig = DockerWorker.Config(
    docker_gateway_address="http://localhost:5000",
    intermediate_storage_config=redis_intermediate_storage_config,
    metadata_storage_config=redis_intermediate_storage_config,  # will use the same as intermediate_storage_config
    metrics_storage_config=MetricsStorage.Config(storage_config=redis_metrics_storage_config, upload_strategy=MetricsStorage.UploadStrategy.AFTER_EACH_TASK)
)

def split_image_into_chunks(image: Image.Image, num_chunks: int) -> List[Image.Image]:
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

@DAGTask
def determine_chunks_amount(image_data: bytes) -> int:
    image = Image.open(io.BytesIO(image_data)).convert("RGB")
    width, _ = image.size
    
    return min(width // 64, 10)

@DAGTask
def split_image(image_data: bytes, num_chunks: int = 4) -> list[bytes]:
    """Split the image into chunks and return as list of bytes"""
    image = Image.open(io.BytesIO(image_data))
    chunks = split_image_into_chunks(image, num_chunks)
    
    chunk_bytes = []
    for chunk in chunks:
        byte_arr = io.BytesIO()
        chunk.save(byte_arr, format=image.format)
        chunk_bytes.append(byte_arr.getvalue())
    
    return chunk_bytes

def add_border_to_image(image: Image.Image, border_width: int = 2, border_color: str = "black") -> Image.Image:
    return ImageOps.expand(image, border=border_width, fill=border_color)

@DAGTask
def grayscale_image_part(chunk_data: bytes) -> bytes:
    """Convert a single image chunk to grayscale"""
    image = Image.open(io.BytesIO(chunk_data))
    grayscale = image.convert('L')
    grayscale = add_border_to_image(grayscale)
    
    byte_arr = io.BytesIO()
    grayscale.save(byte_arr, format=image.format)
    return byte_arr.getvalue()

@DAGTask
def blur_image_part(chunk_data: bytes, blur_radius: int = 3) -> bytes:
    image = Image.open(io.BytesIO(chunk_data))
    blurred_img = image.filter(ImageFilter.GaussianBlur(radius=blur_radius))
    blurred_img = add_border_to_image(blurred_img)

    # Save blurred image to bytes
    blurred_bytes = io.BytesIO()
    blurred_img.save(blurred_bytes, format=image.format)
    blurred_bytes.seek(0)
    
    return blurred_bytes.getvalue()

@DAGTask
def merge_image_parts(processed_chunks: List[bytes]) -> bytes:
    """Combine processed image chunks back into one image"""
    images = [Image.open(io.BytesIO(chunk)) for chunk in processed_chunks]
    
    combined = combine_image_chunks(images) # type: ignore
    
    byte_arr = io.BytesIO()
    combined.save(byte_arr, format=images[0].format)
    return byte_arr.getvalue()

# WORKFLOW DEFINITION
def main():
    image_data: bytes = open("../_inputs/test_image.jpg", "rb").read()

    num_chunks = determine_chunks_amount(image_data)
    print("Number of chunks:", num_chunks)
    chunks = split_image(image_data, num_chunks)
    # chunks = chunks.compute(config=localWorkerConfig)
    chunks = chunks.compute(config=localWorkerConfig)
    
    processed_chunks = []
    for chunk in chunks:
        grayscaled = grayscale_image_part(chunk)
        blurred = blur_image_part(grayscaled)
        processed_chunks.append(blurred)

    final_image = merge_image_parts(processed_chunks)
    # final_image.visualize_dag(output_file=os.path.join("..", "_dag_visualization", "image_transform"), open_after=True)
    final_image = final_image.compute(config=dockerWorkerConfig)

    # image = Image.open(io.BytesIO(final_image))
    # image.show()

if __name__ == "__main__":
    # asyncio.run(main())
    main()

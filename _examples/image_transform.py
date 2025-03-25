import os
import sys
from PIL import Image, ImageFilter
import numpy as np
import io
from typing import List, Tuple

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from src.storage.in_memory_storage import InMemoryStorage
from src.storage.redis_storage import RedisStorage
from src.worker import DockerWorker, LocalWorker
from src.dag_task_node import DAGTask, DAGTaskNode

redis_intermediate_storage_config = RedisStorage.Config(host="localhost", port=6379, password="redisdevpwd123")
inmemory_intermediate_storage_config = InMemoryStorage.Config()

localWorkerConfig = LocalWorker.Config(
    intermediate_storage_config=redis_intermediate_storage_config
)

dockerWorkerConfig = DockerWorker.Config(
    docker_gateway_address="http://localhost:5000",
    intermediate_storage_config=redis_intermediate_storage_config
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
    
    if width <= 256:
        return 1
    else:
        return min(width // 256, 10)

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

@DAGTask
def grayscale_image_part(chunk_data: bytes) -> bytes:
    """Convert a single image chunk to grayscale"""
    image = Image.open(io.BytesIO(chunk_data))
    grayscale = image.convert('L')
    
    byte_arr = io.BytesIO()
    grayscale.save(byte_arr, format=image.format)
    return byte_arr.getvalue()

@DAGTask
def merge_image_parts(processed_chunks: List[bytes]) -> bytes:
    """Combine processed image chunks back into one image"""
    images = [Image.open(io.BytesIO(chunk)) for chunk in processed_chunks]
    
    combined = combine_image_chunks(images) # type: ignore
    
    byte_arr = io.BytesIO()
    combined.save(byte_arr, format=images[0].format)
    return byte_arr.getvalue()

@DAGTask
def blur_image_part(chunk_data: bytes, blur_radius: int = 5) -> bytes:
    image = Image.open(io.BytesIO(chunk_data))
    blurred_img = image.filter(ImageFilter.GaussianBlur(radius=blur_radius))
    
    # Save blurred image to bytes
    blurred_bytes = io.BytesIO()
    blurred_img.save(blurred_bytes, format=image.format)
    blurred_bytes.seek(0)
    
    return blurred_bytes.getvalue()

# WORKFLOW DEFINITION
if __name__ == "__main__":
    image_data: bytes = open("test_image.jpg", "rb").read()

    num_chunks = determine_chunks_amount(image_data)
    chunks = split_image(image_data, num_chunks)
    processed_chunks = chunks.map(grayscale_image_part)
    final_image = merge_image_parts(processed_chunks)

    # final_image.visualize_dag(open_after=True)

    final_image = final_image.compute(config=localWorkerConfig)

    image = Image.open(io.BytesIO(final_image))
    image.show()
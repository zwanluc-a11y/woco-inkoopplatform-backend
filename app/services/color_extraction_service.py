"""Extract dominant colors from an image for brand styling."""
from __future__ import annotations

import logging
from pathlib import Path
from typing import List

logger = logging.getLogger(__name__)


def extract_dominant_colors(image_path: str, n: int = 3) -> List[str]:
    """
    Extract n dominant colors from an image.

    Returns a list of hex color strings (e.g. ['#003366', '#cc9933', '#ffffff']).
    Filters out near-white and near-black colors.
    """
    try:
        from PIL import Image
    except ImportError:
        logger.warning("Pillow not installed, cannot extract colors")
        return []

    try:
        img = Image.open(image_path).convert("RGB")
    except Exception as e:
        logger.error("Failed to open image %s: %s", image_path, e)
        return []

    # Resize for speed
    img = img.resize((150, 150), Image.Resampling.LANCZOS)

    # Quantize to reduce to a small palette
    quantized = img.quantize(colors=16, method=Image.Quantize.MEDIANCUT)
    palette = quantized.getpalette()
    if not palette:
        return []

    # Count pixels per palette index
    pixel_counts: dict = {}
    for pixel in quantized.getdata():
        pixel_counts[pixel] = pixel_counts.get(pixel, 0) + 1

    # Sort by frequency
    sorted_indices = sorted(pixel_counts.keys(), key=lambda x: pixel_counts[x], reverse=True)

    # Extract colors, filtering out near-white and near-black
    colors = []
    for idx in sorted_indices:
        if len(colors) >= n:
            break
        r = palette[idx * 3]
        g = palette[idx * 3 + 1]
        b = palette[idx * 3 + 2]

        # Skip near-white
        if r > 230 and g > 230 and b > 230:
            continue
        # Skip near-black
        if r < 25 and g < 25 and b < 25:
            continue
        # Skip very grey (low saturation)
        if abs(r - g) < 15 and abs(g - b) < 15 and abs(r - b) < 15:
            if r > 180 or r < 60:
                continue

        hex_color = f"#{r:02x}{g:02x}{b:02x}"
        colors.append(hex_color)

    return colors

"""
Poppler-based PDF Analyzer

Alternative analyzer using Poppler CLI tools (pdfinfo, pdffonts, pdfimages).
Requires poppler-utils to be installed in the container.

This module is optional - only used if Poppler tools are available.
"""

import subprocess
import re
import logging
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)


def run_command(cmd: List[str], timeout: int = 30) -> Optional[str]:
    """Run a shell command and return output."""
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout
        )
        return result.stdout
    except subprocess.TimeoutExpired:
        logger.warning(f"Command timed out: {' '.join(cmd)}")
        return None
    except Exception as e:
        logger.warning(f"Command failed: {' '.join(cmd)} - {e}")
        return None


def get_pdf_info(file_path: str) -> Dict:
    """
    Run pdfinfo and parse the output.
    
    Returns dict with keys like:
    - pages, page_size, file_size, pdf_version, encrypted, etc.
    """
    output = run_command(['pdfinfo', file_path])
    if not output:
        return {}
    
    info = {}
    for line in output.strip().split('\n'):
        if ':' in line:
            key, value = line.split(':', 1)
            key = key.strip().lower().replace(' ', '_')
            info[key] = value.strip()
    
    return info


def get_pdf_fonts(file_path: str) -> List[Dict]:
    """
    Run pdffonts and parse the output.
    
    Returns list of font dicts with:
    - name, type, encoding, embedded, subset
    """
    output = run_command(['pdffonts', file_path])
    if not output:
        return []
    
    fonts = []
    lines = output.strip().split('\n')
    
    # Skip header lines (first 2 lines)
    for line in lines[2:]:
        parts = line.split()
        if len(parts) >= 6:
            fonts.append({
                'name': parts[0],
                'type': parts[1],
                'encoding': parts[2],
                'embedded': parts[3].lower() == 'yes',
                'subset': parts[4].lower() == 'yes',
            })
    
    return fonts


def get_pdf_images(file_path: str) -> List[Dict]:
    """
    Run pdfimages -list and parse the output.
    
    Returns list of image dicts with:
    - page, width, height, color, bpc, enc, size
    """
    output = run_command(['pdfimages', '-list', file_path])
    if not output:
        return []
    
    images = []
    lines = output.strip().split('\n')
    
    # Skip header lines (first 2 lines)
    for line in lines[2:]:
        parts = line.split()
        if len(parts) >= 7:
            try:
                images.append({
                    'page': int(parts[0]),
                    'width': int(parts[2]),
                    'height': int(parts[3]),
                    'color': parts[4],
                    'bpc': parts[5],
                    'enc': parts[6],
                })
            except (ValueError, IndexError):
                continue
    
    return images


def analyze_with_poppler(file_path: str) -> Dict:
    """
    Comprehensive PDF analysis using Poppler tools.
    
    Returns a dict with:
    - info: output from pdfinfo
    - fonts: list of fonts from pdffonts
    - images: list of images from pdfimages
    - issues: list of detected issues
    """
    result = {
        'info': {},
        'fonts': [],
        'images': [],
        'issues': []
    }
    
    # Get basic info
    info = get_pdf_info(file_path)
    result['info'] = info
    
    # Check for issues from pdfinfo
    if info.get('encrypted', '').lower() == 'yes':
        result['issues'].append({
            'severity': 'HIGH',
            'category': 'ENCRYPTION',
            'description': 'PDF is encrypted'
        })
    
    pages = int(info.get('pages', 0))
    if pages > 500:
        result['issues'].append({
            'severity': 'HIGH',
            'category': 'PAGE_COUNT',
            'description': f'Document has {pages} pages'
        })
    elif pages > 200:
        result['issues'].append({
            'severity': 'MEDIUM',
            'category': 'PAGE_COUNT',
            'description': f'Document has {pages} pages'
        })
    
    # Get fonts
    fonts = get_pdf_fonts(file_path)
    result['fonts'] = fonts
    
    non_embedded = [f['name'] for f in fonts if not f.get('embedded')]
    if non_embedded:
        result['issues'].append({
            'severity': 'LOW',
            'category': 'FONT_EMBEDDING',
            'description': f'{len(non_embedded)} fonts not embedded',
            'details': non_embedded[:10]
        })
    
    # Get images
    images = get_pdf_images(file_path)
    result['images'] = images
    
    large_images = [
        f"page {img['page']}: {img['width']}x{img['height']}"
        for img in images
        if img['width'] > 4000 or img['height'] > 4000
    ]
    if large_images:
        result['issues'].append({
            'severity': 'HIGH',
            'category': 'IMAGE_SIZE',
            'description': f'{len(large_images)} images exceed 4000px',
            'details': large_images[:10]
        })
    
    if len(images) > 200:
        result['issues'].append({
            'severity': 'HIGH',
            'category': 'IMAGE_COUNT',
            'description': f'Document contains {len(images)} images'
        })
    elif len(images) > 100:
        result['issues'].append({
            'severity': 'MEDIUM',
            'category': 'IMAGE_COUNT',
            'description': f'Document contains {len(images)} images'
        })
    
    return result


def is_poppler_available() -> bool:
    """Check if Poppler tools are installed."""
    try:
        result = subprocess.run(['pdfinfo', '-v'], capture_output=True, timeout=5)
        return True
    except:
        return False

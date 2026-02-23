"""
PDF Structure Analyzer

Analyzes PDF files to identify potential causes of Adobe API failures.
Uses PyMuPDF (fitz) for PDF inspection.
"""

import fitz  # PyMuPDF
import os
import logging
from dataclasses import dataclass, field, asdict
from typing import List, Optional
from enum import Enum

logger = logging.getLogger(__name__)


class Severity(str, Enum):
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"
    INFO = "INFO"


class IssueCategory(str, Enum):
    FILE_SIZE = "FILE_SIZE"
    PAGE_COUNT = "PAGE_COUNT"
    IMAGE_SIZE = "IMAGE_SIZE"
    IMAGE_COUNT = "IMAGE_COUNT"
    FONT_COUNT = "FONT_COUNT"
    FONT_EMBEDDING = "FONT_EMBEDDING"
    ENCRYPTION = "ENCRYPTION"
    PDF_VERSION = "PDF_VERSION"
    PAGE_DIMENSIONS = "PAGE_DIMENSIONS"
    ANNOTATIONS = "ANNOTATIONS"
    CORRUPTION = "CORRUPTION"
    COMPLEXITY = "COMPLEXITY"


@dataclass
class Issue:
    severity: Severity
    category: IssueCategory
    description: str
    details: List[str] = field(default_factory=list)


@dataclass
class AnalysisResult:
    filename: str
    file_size_mb: float
    page_count: int
    image_count: int
    font_count: int
    has_encryption: bool
    pdf_version: str
    issues: List[Issue] = field(default_factory=list)
    likely_cause: Optional[str] = None
    analysis_error: Optional[str] = None
    
    def to_dict(self) -> dict:
        result = asdict(self)
        result['issues'] = [
            {
                'severity': issue['severity'],
                'category': issue['category'],
                'description': issue['description'],
                'details': issue['details']
            }
            for issue in result['issues']
        ]
        return result


# Thresholds for issue detection
THRESHOLDS = {
    'file_size_mb_high': 100,
    'file_size_mb_medium': 50,
    'page_count_high': 500,
    'page_count_medium': 200,
    'image_count_high': 200,
    'image_count_medium': 100,
    'image_dimension_high': 4000,
    'image_dimension_medium': 3000,
    'font_count_high': 50,
    'font_count_medium': 30,
    'annotation_count_high': 100,
    'annotation_count_medium': 50,
}


def analyze_pdf(file_path: str) -> AnalysisResult:
    """
    Analyze a PDF file and return structured results.
    
    Args:
        file_path: Path to the PDF file
        
    Returns:
        AnalysisResult with findings
    """
    filename = os.path.basename(file_path)
    file_size_mb = os.path.getsize(file_path) / (1024 * 1024)
    
    # Initialize result with defaults
    result = AnalysisResult(
        filename=filename,
        file_size_mb=round(file_size_mb, 2),
        page_count=0,
        image_count=0,
        font_count=0,
        has_encryption=False,
        pdf_version="unknown",
        issues=[]
    )
    
    try:
        doc = fitz.open(file_path)
    except Exception as e:
        result.analysis_error = f"Failed to open PDF: {str(e)}"
        result.issues.append(Issue(
            severity=Severity.HIGH,
            category=IssueCategory.CORRUPTION,
            description="PDF could not be opened - likely corrupted or malformed",
            details=[str(e)]
        ))
        result.likely_cause = "Corrupted or malformed PDF structure"
        return result
    
    try:
        # Basic metadata
        result.page_count = len(doc)
        result.has_encryption = doc.is_encrypted
        result.pdf_version = doc.metadata.get('format', 'unknown') if doc.metadata else 'unknown'
        
        # Check file size
        _check_file_size(result, file_size_mb)
        
        # Check page count
        _check_page_count(result)
        
        # Check encryption
        if result.has_encryption:
            result.issues.append(Issue(
                severity=Severity.HIGH,
                category=IssueCategory.ENCRYPTION,
                description="PDF is encrypted/password-protected"
            ))
        
        # Analyze images
        _analyze_images(doc, result)
        
        # Analyze fonts
        _analyze_fonts(doc, result)
        
        # Analyze page dimensions
        _analyze_page_dimensions(doc, result)
        
        # Analyze annotations
        _analyze_annotations(doc, result)
        
        # Determine likely cause
        result.likely_cause = _determine_likely_cause(result)
        
    except Exception as e:
        logger.error(f"Error during PDF analysis: {e}")
        result.analysis_error = str(e)
    finally:
        doc.close()
    
    return result


def _check_file_size(result: AnalysisResult, file_size_mb: float):
    """Check if file size is problematic."""
    if file_size_mb > THRESHOLDS['file_size_mb_high']:
        result.issues.append(Issue(
            severity=Severity.HIGH,
            category=IssueCategory.FILE_SIZE,
            description=f"File size ({file_size_mb:.1f} MB) exceeds {THRESHOLDS['file_size_mb_high']} MB threshold"
        ))
    elif file_size_mb > THRESHOLDS['file_size_mb_medium']:
        result.issues.append(Issue(
            severity=Severity.MEDIUM,
            category=IssueCategory.FILE_SIZE,
            description=f"File size ({file_size_mb:.1f} MB) is large, may cause timeout"
        ))


def _check_page_count(result: AnalysisResult):
    """Check if page count is problematic."""
    if result.page_count > THRESHOLDS['page_count_high']:
        result.issues.append(Issue(
            severity=Severity.HIGH,
            category=IssueCategory.PAGE_COUNT,
            description=f"Document has {result.page_count} pages, exceeds {THRESHOLDS['page_count_high']} page threshold"
        ))
    elif result.page_count > THRESHOLDS['page_count_medium']:
        result.issues.append(Issue(
            severity=Severity.MEDIUM,
            category=IssueCategory.PAGE_COUNT,
            description=f"Document has {result.page_count} pages, may cause timeout"
        ))


def _analyze_images(doc: fitz.Document, result: AnalysisResult):
    """Analyze images in the PDF."""
    total_images = 0
    large_images = []
    
    for page_num in range(len(doc)):
        page = doc[page_num]
        image_list = page.get_images(full=True)
        total_images += len(image_list)
        
        for img_index, img in enumerate(image_list):
            try:
                xref = img[0]
                base_image = doc.extract_image(xref)
                if base_image:
                    width = base_image.get('width', 0)
                    height = base_image.get('height', 0)
                    
                    if width > THRESHOLDS['image_dimension_high'] or height > THRESHOLDS['image_dimension_high']:
                        large_images.append(f"page {page_num + 1}: {width}x{height}")
                    elif width > THRESHOLDS['image_dimension_medium'] or height > THRESHOLDS['image_dimension_medium']:
                        large_images.append(f"page {page_num + 1}: {width}x{height} (medium)")
            except Exception as e:
                logger.debug(f"Could not analyze image on page {page_num + 1}: {e}")
    
    result.image_count = total_images
    
    # Check image count
    if total_images > THRESHOLDS['image_count_high']:
        result.issues.append(Issue(
            severity=Severity.HIGH,
            category=IssueCategory.IMAGE_COUNT,
            description=f"Document contains {total_images} images, exceeds {THRESHOLDS['image_count_high']} threshold"
        ))
    elif total_images > THRESHOLDS['image_count_medium']:
        result.issues.append(Issue(
            severity=Severity.MEDIUM,
            category=IssueCategory.IMAGE_COUNT,
            description=f"Document contains {total_images} images"
        ))
    
    # Check large images
    if large_images:
        severity = Severity.HIGH if any('medium' not in img for img in large_images) else Severity.MEDIUM
        result.issues.append(Issue(
            severity=severity,
            category=IssueCategory.IMAGE_SIZE,
            description=f"{len(large_images)} images have large dimensions",
            details=large_images[:10]  # Limit to first 10
        ))


def _analyze_fonts(doc: fitz.Document, result: AnalysisResult):
    """Analyze fonts in the PDF."""
    all_fonts = set()
    non_embedded = []
    
    for page_num in range(len(doc)):
        page = doc[page_num]
        fonts = page.get_fonts(full=True)
        
        for font in fonts:
            font_name = font[3] if len(font) > 3 else "unknown"
            all_fonts.add(font_name)
            
            # Check if font is embedded (simplified check)
            font_type = font[1] if len(font) > 1 else ""
            if font_type and 'Type3' not in font_type:
                # Type3 fonts are always embedded
                ext = font[4] if len(font) > 4 else ""
                if not ext:  # No extension usually means not embedded
                    non_embedded.append(font_name)
    
    result.font_count = len(all_fonts)
    
    # Check font count
    if result.font_count > THRESHOLDS['font_count_high']:
        result.issues.append(Issue(
            severity=Severity.MEDIUM,
            category=IssueCategory.FONT_COUNT,
            description=f"Document uses {result.font_count} different fonts"
        ))
    
    # Check non-embedded fonts
    if non_embedded:
        unique_non_embedded = list(set(non_embedded))
        result.issues.append(Issue(
            severity=Severity.LOW,
            category=IssueCategory.FONT_EMBEDDING,
            description=f"{len(unique_non_embedded)} fonts may not be embedded",
            details=unique_non_embedded[:10]
        ))


def _analyze_page_dimensions(doc: fitz.Document, result: AnalysisResult):
    """Analyze page dimensions for unusual sizes."""
    dimensions = {}
    unusual_pages = []
    
    for page_num in range(len(doc)):
        page = doc[page_num]
        rect = page.rect
        width = round(rect.width)
        height = round(rect.height)
        
        dim_key = f"{width}x{height}"
        dimensions[dim_key] = dimensions.get(dim_key, 0) + 1
        
        # Check for unusual dimensions (not standard paper sizes)
        # Standard sizes: Letter (612x792), A4 (595x842), Legal (612x1008)
        is_standard = (
            (abs(width - 612) < 10 and abs(height - 792) < 10) or  # Letter
            (abs(width - 595) < 10 and abs(height - 842) < 10) or  # A4
            (abs(width - 612) < 10 and abs(height - 1008) < 10) or  # Legal
            (abs(height - 612) < 10 and abs(width - 792) < 10) or  # Letter landscape
            (abs(height - 595) < 10 and abs(width - 842) < 10)     # A4 landscape
        )
        
        if not is_standard and width > 1000 or height > 1500:
            unusual_pages.append(f"page {page_num + 1}: {width}x{height}")
    
    # Check for mixed dimensions
    if len(dimensions) > 3:
        result.issues.append(Issue(
            severity=Severity.LOW,
            category=IssueCategory.PAGE_DIMENSIONS,
            description=f"Document has {len(dimensions)} different page sizes",
            details=[f"{dim}: {count} pages" for dim, count in dimensions.items()]
        ))
    
    # Check for unusual dimensions
    if unusual_pages:
        result.issues.append(Issue(
            severity=Severity.MEDIUM,
            category=IssueCategory.PAGE_DIMENSIONS,
            description=f"{len(unusual_pages)} pages have unusual dimensions",
            details=unusual_pages[:10]
        ))


def _analyze_annotations(doc: fitz.Document, result: AnalysisResult):
    """Analyze annotations (forms, comments, etc.)."""
    total_annotations = 0
    annotation_types = {}
    
    for page_num in range(len(doc)):
        page = doc[page_num]
        annots = page.annots()
        
        if annots:
            for annot in annots:
                total_annotations += 1
                annot_type = annot.type[1] if annot.type else "unknown"
                annotation_types[annot_type] = annotation_types.get(annot_type, 0) + 1
    
    if total_annotations > THRESHOLDS['annotation_count_high']:
        result.issues.append(Issue(
            severity=Severity.MEDIUM,
            category=IssueCategory.ANNOTATIONS,
            description=f"Document has {total_annotations} annotations",
            details=[f"{atype}: {count}" for atype, count in annotation_types.items()]
        ))
    elif total_annotations > THRESHOLDS['annotation_count_medium']:
        result.issues.append(Issue(
            severity=Severity.LOW,
            category=IssueCategory.ANNOTATIONS,
            description=f"Document has {total_annotations} annotations"
        ))


def _determine_likely_cause(result: AnalysisResult) -> str:
    """Determine the most likely cause of failure based on issues found."""
    high_issues = [i for i in result.issues if i.severity == Severity.HIGH]
    medium_issues = [i for i in result.issues if i.severity == Severity.MEDIUM]
    
    if not high_issues and not medium_issues:
        return "No obvious structural issues detected - failure may be due to content complexity or API-specific limitations"
    
    # Prioritize causes
    causes = []
    
    for issue in high_issues:
        if issue.category == IssueCategory.CORRUPTION:
            return "Corrupted or malformed PDF structure"
        elif issue.category == IssueCategory.ENCRYPTION:
            causes.append("encrypted/password-protected")
        elif issue.category == IssueCategory.FILE_SIZE:
            causes.append("very large file size")
        elif issue.category == IssueCategory.PAGE_COUNT:
            causes.append("excessive page count")
        elif issue.category == IssueCategory.IMAGE_SIZE:
            causes.append("oversized images")
        elif issue.category == IssueCategory.IMAGE_COUNT:
            causes.append("too many images")
    
    for issue in medium_issues:
        if issue.category == IssueCategory.PAGE_DIMENSIONS:
            causes.append("unusual page dimensions")
        elif issue.category == IssueCategory.COMPLEXITY:
            causes.append("document complexity")
    
    if causes:
        return f"Likely caused by: {', '.join(causes)}"
    
    return "Multiple minor issues may have contributed to failure"

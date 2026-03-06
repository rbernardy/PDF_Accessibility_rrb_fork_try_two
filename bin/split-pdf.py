#!/usr/bin/env python3
"""
Local PDF Splitter

Splits a PDF into chunks locally using the same logic as the Lambda function.
Does NOT upload to S3 or trigger any processing - just creates local chunk files.

Useful for examining how a PDF would be split before uploading to the pipeline.

Usage:
    ./bin/split-pdf.py <pdf_file> [options]

Examples:
    ./bin/split-pdf.py document.pdf
    ./bin/split-pdf.py document.pdf --output-dir ./chunks
    ./bin/split-pdf.py document.pdf --pages-per-chunk 5
    ./bin/split-pdf.py document.pdf --prescan  # Use risk-based chunking
"""

import argparse
import io
import os
import sys
from pathlib import Path


def prescan_pdf_for_risk(pdf_content: bytes, filename: str) -> dict:
    """
    Pre-scan a PDF to detect characteristics that may cause Adobe API failures.
    Same logic as the Lambda function.
    """
    from pypdf import PdfReader
    
    result = {
        'is_risky': False,
        'risk_factors': [],
        'recommended_pages_per_chunk': 90,
        'page_count': 0,
        'has_large_pages': False,
        'has_mixed_sizes': False,
        'is_image_heavy': False
    }
    
    try:
        reader = PdfReader(io.BytesIO(pdf_content))
        num_pages = len(reader.pages)
        result['page_count'] = num_pages
        
        if num_pages == 0:
            return result
        
        page_sizes = set()
        large_page_count = 0
        total_images = 0
        
        LARGE_DIMENSION_THRESHOLD = 1200
        
        for page in reader.pages:
            mediabox = page.mediabox
            width = float(mediabox.width)
            height = float(mediabox.height)
            
            w, h = (min(width, height), max(width, height))
            page_sizes.add((round(w), round(h)))
            
            if width > LARGE_DIMENSION_THRESHOLD or height > LARGE_DIMENSION_THRESHOLD:
                large_page_count += 1
            
            if '/XObject' in page.get('/Resources', {}):
                xobjects = page['/Resources'].get('/XObject', {})
                if hasattr(xobjects, 'keys'):
                    for obj_name in xobjects.keys():
                        try:
                            xobj = xobjects[obj_name]
                            if xobj.get('/Subtype') == '/Image':
                                total_images += 1
                        except:
                            pass
        
        if large_page_count > 0:
            result['has_large_pages'] = True
            result['risk_factors'].append(f'{large_page_count} pages with large dimensions')
            result['is_risky'] = True
        
        if len(page_sizes) > 2:
            result['has_mixed_sizes'] = True
            result['risk_factors'].append(f'{len(page_sizes)} different page sizes')
            result['is_risky'] = True
        
        images_per_page = total_images / num_pages if num_pages > 0 else 0
        if images_per_page >= 1.0 or total_images > num_pages * 0.8:
            result['is_image_heavy'] = True
            result['risk_factors'].append(f'Image-heavy ({total_images} images)')
            result['is_risky'] = True
        
        file_size_mb = len(pdf_content) / (1024 * 1024)
        mb_per_page = file_size_mb / num_pages if num_pages > 0 else 0
        if mb_per_page > 2.0:
            result['risk_factors'].append(f'High density ({mb_per_page:.1f} MB/page)')
            result['is_risky'] = True
        
        if result['is_risky']:
            if num_pages <= 10:
                result['recommended_pages_per_chunk'] = 1
            elif num_pages <= 20:
                result['recommended_pages_per_chunk'] = 2
            else:
                result['recommended_pages_per_chunk'] = 5
                
    except Exception as e:
        print(f"Warning: Pre-scan error: {e}", file=sys.stderr)
    
    return result


def split_pdf(pdf_path: str, output_dir: str, pages_per_chunk: int, max_chunk_size_mb: int = 95) -> list:
    """
    Split a PDF into chunks locally.
    
    Returns list of created chunk file paths.
    """
    from pypdf import PdfReader, PdfWriter
    
    max_chunk_size_bytes = max_chunk_size_mb * 1024 * 1024
    
    with open(pdf_path, 'rb') as f:
        pdf_content = f.read()
    
    reader = PdfReader(io.BytesIO(pdf_content))
    num_pages = len(reader.pages)
    
    file_basename = Path(pdf_path).stem
    
    os.makedirs(output_dir, exist_ok=True)
    
    chunks = []
    chunk_index = 0
    current_page = 0
    
    while current_page < num_pages:
        chunk_index += 1
        end_page = min(current_page + pages_per_chunk, num_pages)
        
        chunk_created = False
        while not chunk_created and current_page < end_page:
            output = io.BytesIO()
            writer = PdfWriter()
            
            for i in range(current_page, end_page):
                writer.add_page(reader.pages[i])
            
            writer.write(output)
            chunk_size = output.tell()
            output.seek(0)
            
            pages_in_chunk = end_page - current_page
            
            if chunk_size <= max_chunk_size_bytes or pages_in_chunk <= 1:
                # Write chunk to file
                chunk_filename = f"{file_basename}_chunk_{chunk_index}.pdf"
                chunk_path = os.path.join(output_dir, chunk_filename)
                
                with open(chunk_path, 'wb') as f:
                    f.write(output.read())
                
                chunk_size_mb = chunk_size / (1024 * 1024)
                
                chunks.append({
                    'path': chunk_path,
                    'filename': chunk_filename,
                    'pages': pages_in_chunk,
                    'start_page': current_page + 1,
                    'end_page': end_page,
                    'size_mb': round(chunk_size_mb, 2)
                })
                
                if pages_in_chunk <= 1 and chunk_size > max_chunk_size_bytes:
                    print(f"  Warning: {chunk_filename} exceeds size limit ({chunk_size_mb:.1f}MB)")
                
                current_page = end_page
                chunk_created = True
            else:
                # Reduce pages
                reduction = max(1, pages_in_chunk // 2)
                end_page = current_page + (pages_in_chunk - reduction)
    
    return chunks


def main():
    parser = argparse.ArgumentParser(
        description='Split a PDF into chunks locally (no S3, no processing).',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s document.pdf                          # Split with default 90 pages/chunk
  %(prog)s document.pdf -o ./chunks              # Output to specific directory
  %(prog)s document.pdf -p 5                     # 5 pages per chunk
  %(prog)s document.pdf --prescan                # Use risk-based chunking
  %(prog)s document.pdf --prescan -p 10          # Prescan, but max 10 pages/chunk
        """
    )
    parser.add_argument('pdf_file', help='PDF file to split')
    parser.add_argument('-o', '--output-dir', help='Output directory (default: <filename>_chunks/)')
    parser.add_argument('-p', '--pages-per-chunk', type=int, default=90,
                        help='Pages per chunk (default: 90)')
    parser.add_argument('--prescan', action='store_true',
                        help='Use pre-scan to determine optimal chunk size')
    parser.add_argument('--max-size-mb', type=int, default=95,
                        help='Max chunk size in MB (default: 95)')
    
    args = parser.parse_args()
    
    # Check for pypdf
    try:
        from pypdf import PdfReader, PdfWriter
    except ImportError:
        print("Error: pypdf is required. Install with: pip install pypdf", file=sys.stderr)
        sys.exit(1)
    
    # Validate input file
    pdf_path = args.pdf_file
    if not os.path.exists(pdf_path):
        print(f"Error: File not found: {pdf_path}", file=sys.stderr)
        sys.exit(1)
    
    if not pdf_path.lower().endswith('.pdf'):
        print(f"Warning: File does not have .pdf extension", file=sys.stderr)
    
    # Determine output directory
    if args.output_dir:
        output_dir = args.output_dir
    else:
        output_dir = f"{Path(pdf_path).stem}_chunks"
    
    # Read PDF
    with open(pdf_path, 'rb') as f:
        pdf_content = f.read()
    
    file_size_mb = len(pdf_content) / (1024 * 1024)
    
    print(f"Input: {pdf_path}")
    print(f"File size: {file_size_mb:.2f} MB")
    
    # Determine pages per chunk
    pages_per_chunk = args.pages_per_chunk
    
    if args.prescan:
        print()
        print("Running pre-scan...")
        prescan_result = prescan_pdf_for_risk(pdf_content, pdf_path)
        
        print(f"  Pages: {prescan_result['page_count']}")
        print(f"  Risk assessment: {'RISKY' if prescan_result['is_risky'] else 'NORMAL'}")
        if prescan_result['risk_factors']:
            print(f"  Risk factors: {', '.join(prescan_result['risk_factors'])}")
        print(f"  Recommended pages/chunk: {prescan_result['recommended_pages_per_chunk']}")
        
        # Use the more aggressive (smaller) of prescan recommendation and user-specified
        prescan_pages = prescan_result['recommended_pages_per_chunk']
        pages_per_chunk = min(pages_per_chunk, prescan_pages)
        
        if pages_per_chunk != args.pages_per_chunk:
            print(f"  Using: {pages_per_chunk} pages/chunk (more aggressive of prescan and --pages-per-chunk)")
    
    print()
    print(f"Output directory: {output_dir}")
    print(f"Pages per chunk: {pages_per_chunk}")
    print()
    
    # Split the PDF
    print("Splitting PDF...")
    chunks = split_pdf(pdf_path, output_dir, pages_per_chunk, args.max_size_mb)
    
    print()
    print(f"Created {len(chunks)} chunk(s):")
    print()
    
    total_size = 0
    for chunk in chunks:
        total_size += chunk['size_mb']
        page_range = f"pages {chunk['start_page']}-{chunk['end_page']}" if chunk['pages'] > 1 else f"page {chunk['start_page']}"
        print(f"  {chunk['filename']}: {chunk['pages']} pages ({page_range}), {chunk['size_mb']:.2f} MB")
    
    print()
    print(f"Total: {len(chunks)} chunks, {total_size:.2f} MB")
    print(f"Output: {os.path.abspath(output_dir)}/")


if __name__ == '__main__':
    main()

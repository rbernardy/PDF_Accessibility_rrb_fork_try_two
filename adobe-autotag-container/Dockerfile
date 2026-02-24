# Use an official Python runtime as a parent image
FROM public.ecr.aws/docker/library/python:3.12-slim

# Force rebuild: 2026-02-24-v4 - Added version marker logging
# Build timestamp: Mon Feb 24 2026 21:30:00 UTC
LABEL version="2.4" description="Adobe Autotag with global RPM rate limiting, anti-thundering-herd, and version logging"

# CACHE BUSTER - Change this value to force Docker rebuild
ARG CACHE_BUST=20260224-2130

# Set the working directory in the container to /app
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY . /app

# Create required directories
RUN mkdir -p /output/AutotagPDF \
    /output/ExtractTextInfoFromPDF \
    /output/zipfile/images

# Install Python dependencies and clean up unnecessary packages
RUN pip install --no-cache-dir -r requirements.txt && \
    # Remove documentation/sphinx packages pulled in by pdfservices-sdk (saves ~70MB)
    pip uninstall -y sphinx sphinx-rtd-theme sphinxcontrib-applehelp \
        sphinxcontrib-devhelp sphinxcontrib-htmlhelp sphinxcontrib-jquery \
        sphinxcontrib-jsmath sphinxcontrib-qthelp sphinxcontrib-serializinghtml \
        babel pygments alabaster imagesize snowballstemmer docutils 2>/dev/null || true && \
    # Remove pip and setuptools (not needed at runtime, saves ~18MB)
    pip uninstall -y pip setuptools 2>/dev/null || true && \
    # Clean up cache
    rm -rf /root/.cache /tmp/*

# Set the command to run the Python script when the container launches
CMD ["python", "./adobe_autotag_processor.py"]

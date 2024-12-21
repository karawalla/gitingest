"""
Download Router Module

This module provides endpoints for downloading ingested text files. It includes robust error handling,
file validation, and size checks to ensure safe and reliable file downloads.

Key Features:
- File size validation to prevent memory issues
- Content validation to ensure data integrity
- Comprehensive error handling with specific HTTP status codes
- Detailed logging for monitoring and debugging
- Security measures against potential file system vulnerabilities

Author: Rishi Karawalla
Date: 2024-12-20
"""

from fastapi import HTTPException, APIRouter
from fastapi.responses import Response
from config import TMP_BASE_PATH
import os
import logging
from typing import Optional

# Constants for file size limits and validation
MAX_FILE_SIZE_BYTES = 50 * 1024 * 1024  # 50MB limit to prevent memory exhaustion
logger = logging.getLogger(__name__)

router = APIRouter()

def validate_file_content(content: str) -> Optional[str]:
    """
    Validates the content of a text file for safety and size constraints.
    
    Args:
        content (str): The content of the file to validate
        
    Returns:
        Optional[str]: Error message if validation fails, None if content is valid
        
    Validation checks:
    - Ensures file is not empty
    - Verifies file size is within acceptable limits
    """
    if not content.strip():
        return "File is empty"
    if len(content.encode('utf-8')) > MAX_FILE_SIZE_BYTES:
        return f"File size exceeds maximum limit of {MAX_FILE_SIZE_BYTES // 1024 // 1024}MB"
    return None

@router.get("/download/{digest_id}")
async def download_ingest(digest_id: str):
    """
    Endpoint to download an ingested text file by its digest ID.
    
    Args:
        digest_id (str): Unique identifier for the ingested file
        
    Returns:
        Response: FastAPI Response object containing the file content
        
    Raises:
        HTTPException: With appropriate status codes for various error conditions:
            - 404: File or directory not found
            - 413: File exceeds size limit
            - 422: Content validation failed
            - 403: Permission denied
            - 500: Unexpected server error
            
    Security measures:
    - Validates directory existence before access
    - Checks file size before loading into memory
    - Validates file content before sending
    - Uses proper error handling to prevent information leakage
    """
    try:
        # Construct and validate the directory path
        directory = f"{TMP_BASE_PATH}/{digest_id}"
        
        # Security check: Ensure directory exists before attempting access
        if not os.path.exists(directory):
            logger.warning(f"Directory not found for digest_id: {digest_id}")
            raise HTTPException(status_code=404, detail="Digest directory not found")
            
        # Find all .txt files and take the first one
        # Note: We only process .txt files for security and consistency
        txt_files = [f for f in os.listdir(directory) if f.endswith('.txt')]
        
        if not txt_files:
            logger.warning(f"No .txt files found in directory for digest_id: {digest_id}")
            raise HTTPException(status_code=404, detail="No text file found for this digest")
            
        file_path = f"{directory}/{txt_files[0]}"
        
        # Perform size check before attempting to read file
        # This prevents potential memory issues with large files
        file_size = os.path.getsize(file_path)
        if file_size > MAX_FILE_SIZE_BYTES:
            logger.error(f"File size {file_size} exceeds limit for digest_id: {digest_id}")
            raise HTTPException(status_code=413, detail="File too large to download")
            
        # Read and validate file content
        with open(file_path, "r") as f:
            content = f.read()
            
        # Perform content validation
        error_msg = validate_file_content(content)
        if error_msg:
            logger.error(f"Content validation failed for digest_id: {digest_id} - {error_msg}")
            raise HTTPException(status_code=422, detail=error_msg)
        
        logger.info(f"Successfully processed download request for digest_id: {digest_id}")
        
        # Return file with appropriate headers for download
        return Response(
            content=content,
            media_type="text/plain",
            headers={
                "Content-Disposition": f"attachment; filename={txt_files[0]}",
                "Content-Length": str(len(content.encode('utf-8')))
            }
        )
    except FileNotFoundError:
        logger.error(f"FileNotFoundError for digest_id: {digest_id}")
        raise HTTPException(status_code=404, detail="Digest not found")
    except PermissionError:
        logger.error(f"PermissionError accessing files for digest_id: {digest_id}")
        raise HTTPException(status_code=403, detail="Permission denied accessing digest")
    except Exception as e:
        # Catch-all for unexpected errors to prevent sensitive info leakage
        logger.exception(f"Unexpected error processing digest_id: {digest_id}")
        raise HTTPException(status_code=500, detail="Internal server error")
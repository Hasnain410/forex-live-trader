"""
S3 Storage Service for Live Trading
====================================

Handles chart storage to S3 with optional local cleanup.
Uses the same bucket as the backtester for consistency.
"""

import asyncio
import logging
from pathlib import Path
from typing import Optional

import boto3
from botocore.exceptions import ClientError

from ..config import settings

logger = logging.getLogger(__name__)

# S3 client (initialized lazily)
_s3_client = None


def get_s3_client():
    """Get or create S3 client."""
    global _s3_client
    if _s3_client is None:
        _s3_client = boto3.client('s3')
    return _s3_client


def upload_to_s3(
    local_path: str,
    s3_key: str,
    bucket: str = None,
    delete_local: bool = False
) -> bool:
    """
    Upload a file to S3.

    Args:
        local_path: Path to local file
        s3_key: S3 object key (path within bucket)
        bucket: S3 bucket name (defaults to settings.s3_bucket)
        delete_local: Whether to delete local file after upload

    Returns:
        True if upload succeeded, False otherwise
    """
    bucket = bucket or settings.s3_bucket

    try:
        s3 = get_s3_client()
        s3.upload_file(
            local_path,
            bucket,
            s3_key,
            ExtraArgs={'ContentType': 'image/png'}
        )
        logger.info(f"Uploaded {local_path} to s3://{bucket}/{s3_key}")

        if delete_local:
            Path(local_path).unlink(missing_ok=True)
            logger.debug(f"Deleted local file: {local_path}")

        return True

    except ClientError as e:
        logger.error(f"S3 upload failed for {local_path}: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error uploading to S3: {e}")
        return False


async def upload_to_s3_async(
    local_path: str,
    s3_key: str,
    bucket: str = None,
    delete_local: bool = False
) -> bool:
    """
    Async wrapper for S3 upload.

    Runs the upload in a thread pool to avoid blocking the event loop.
    """
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(
        None,
        upload_to_s3,
        local_path,
        s3_key,
        bucket,
        delete_local
    )


def upload_chart_to_s3(
    local_path: str,
    pair: str,
    delete_local: bool = False
) -> Optional[str]:
    """
    Upload a chart to S3 with the standard path structure.

    Charts are stored as:
        s3://forex-backtester-hasnain/live-trader-charts/{PAIR}/{filename}

    Args:
        local_path: Path to local chart file
        pair: Currency pair (e.g., 'EURUSD')
        delete_local: Whether to delete local file after upload

    Returns:
        S3 URL if successful, None otherwise
    """
    path = Path(local_path)
    filename = path.name

    # Build S3 key: live-trader-charts/EURUSD/EURUSD_20251228_0800_London_Open.png
    s3_key = f"live-trader-charts/{pair}/{filename}"

    if upload_to_s3(local_path, s3_key, delete_local=delete_local):
        return f"s3://{settings.s3_bucket}/{s3_key}"
    return None


async def upload_chart_to_s3_async(
    local_path: str,
    pair: str,
    delete_local: bool = False
) -> Optional[str]:
    """
    Async version of upload_chart_to_s3.
    """
    path = Path(local_path)
    filename = path.name

    s3_key = f"live-trader-charts/{pair}/{filename}"

    if await upload_to_s3_async(local_path, s3_key, delete_local=delete_local):
        return f"s3://{settings.s3_bucket}/{s3_key}"
    return None


def get_chart_s3_url(pair: str, filename: str) -> str:
    """
    Get the S3 URL for a chart.

    Args:
        pair: Currency pair
        filename: Chart filename

    Returns:
        S3 URL (s3://bucket/key format)
    """
    s3_key = f"live-trader-charts/{pair}/{filename}"
    return f"s3://{settings.s3_bucket}/{s3_key}"


def get_chart_https_url(pair: str, filename: str) -> str:
    """
    Get the HTTPS URL for a chart (via CloudFront or S3).

    Uses CloudFront CDN if available for faster delivery.

    Args:
        pair: Currency pair
        filename: Chart filename

    Returns:
        HTTPS URL for the chart
    """
    # CloudFront distribution for forex-backtester-hasnain
    cloudfront_domain = "d2qsrlw6g3vj7o.cloudfront.net"
    s3_key = f"live-trader-charts/{pair}/{filename}"
    return f"https://{cloudfront_domain}/{s3_key}"


def download_from_s3(
    s3_key: str,
    local_path: str,
    bucket: str = None
) -> bool:
    """
    Download a file from S3.

    Args:
        s3_key: S3 object key
        local_path: Path to save file locally
        bucket: S3 bucket name

    Returns:
        True if download succeeded, False otherwise
    """
    bucket = bucket or settings.s3_bucket

    try:
        s3 = get_s3_client()

        # Ensure local directory exists
        Path(local_path).parent.mkdir(parents=True, exist_ok=True)

        s3.download_file(bucket, s3_key, local_path)
        logger.info(f"Downloaded s3://{bucket}/{s3_key} to {local_path}")
        return True

    except ClientError as e:
        logger.error(f"S3 download failed for {s3_key}: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error downloading from S3: {e}")
        return False


def check_s3_exists(s3_key: str, bucket: str = None) -> bool:
    """
    Check if an object exists in S3.

    Args:
        s3_key: S3 object key
        bucket: S3 bucket name

    Returns:
        True if object exists, False otherwise
    """
    bucket = bucket or settings.s3_bucket

    try:
        s3 = get_s3_client()
        s3.head_object(Bucket=bucket, Key=s3_key)
        return True
    except ClientError:
        return False


def list_charts_in_s3(pair: str = None, prefix: str = None) -> list:
    """
    List charts in S3.

    Args:
        pair: Currency pair to filter by (optional)
        prefix: Custom prefix (optional, overrides pair)

    Returns:
        List of S3 keys
    """
    bucket = settings.s3_bucket

    if prefix is None:
        if pair:
            prefix = f"live-trader-charts/{pair}/"
        else:
            prefix = "live-trader-charts/"

    try:
        s3 = get_s3_client()

        keys = []
        paginator = s3.get_paginator('list_objects_v2')

        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get('Contents', []):
                keys.append(obj['Key'])

        return keys

    except ClientError as e:
        logger.error(f"S3 list failed: {e}")
        return []

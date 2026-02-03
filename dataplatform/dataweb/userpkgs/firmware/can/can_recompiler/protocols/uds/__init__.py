"""
UDS (Unified Diagnostic Services) Application Package

This package provides UDS application layer processing for diagnostic
services, including service identification and data extraction.
"""

from .frames import UDSMessage

__all__ = ["UDSMessage"]

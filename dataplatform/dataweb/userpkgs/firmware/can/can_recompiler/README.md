# CAN Recompiler Package - Development Guide

Development environment and testing tools for the CAN recompiler package.

> **For usage documentation, see the main [CAN Library README](../README.md)**

## Development Setup

### First Time Setup
```bash
make setup
```
This creates a virtual environment, installs dependencies (pytest, pandas, numpy), and prepares the testing environment.

### Running Tests
```bash
# Run all tests
make test

# Run tests with detailed output and timing
make test-verbose

# Quick environment check
make test-quick

# Test coverage analysis
make coverage           # Show coverage summary
make coverage-html      # Generate detailed HTML report
make coverage-report    # Show detailed terminal report
```

### Development Commands
```bash
# Show all available commands
make help

# Check environment status
make check-env

# Show test suite information
make test-info

# Clean up virtual environment
make clean

# Complete rebuild
make rebuild
```

## Testing

The package includes 52 comprehensive unit tests covering:
- ISO-TP single and multi-frame reassembly
- J1939 Transport Protocol (TP.CM, TP.DT, TP.ACK)
- Protocol detection and classification
- Edge cases and error handling
- Session management and cleanup

Use `make coverage-html` to view detailed line-by-line coverage analysis.

## Package Structure

```
├── constants.py          # Protocol constants and magic numbers  
├── enums.py              # Protocol type enumerations
├── frame_types.py        # Data structures and frame definitions
├── processor.py          # Main CAN frame processor interface
├── j1939_processor.py    # J1939 protocol implementation  
├── isotp_processor.py    # ISO-TP protocol implementation
├── protocol_detection.py # Core CAN protocol detection logic
├── utils.py              # Utility functions for data extraction
└── test_can_recompiler.py # Comprehensive test suite
```

## Requirements
- Python 3.8+
- pandas >= 1.3.0
- numpy >= 1.20.0
- pytest >= 6.0 (for testing)

Automatically installed by `make setup`.
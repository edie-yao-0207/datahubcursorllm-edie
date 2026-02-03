# CAN Recompiler Library

A modular Python library for processing and reassembling CAN bus frames from automotive diagnostic protocols including ISO-TP (UDS/OBD-II) and J1939.

## Features

- **ISO-TP Reassembly**: Handle UDS and OBD-II multi-frame message reconstruction
- **J1939 Transport Protocol**: Support for heavy-duty vehicle multi-packet messages  
- **Protocol Detection**: Automatic classification of transport and application protocols
- **Modular Design**: Clean separation of concerns across protocol-specific processors
- **DataWeb Integration**: Designed for use in Spark/DataWeb pipelines
- **Standalone Usage**: Can be built and used independently of DataWeb

## Installation

### For Databricks/DataWeb Usage
```bash
# Minimal dependencies (recommended for Databricks clusters)
pip install can_recompiler
```

### For Standalone Development
```bash
# Includes pandas and other dependencies  
pip install can_recompiler[standalone]

# For development with testing tools
pip install can_recompiler[dev]
```

## Quick Start

### Basic Usage

```python
from can_recompiler import CANFrameProcessor

# Create processor
processor = CANFrameProcessor()

# Process a CAN frame (from pandas DataFrame row)
parsed_frame = processor.parse_frame(row_data)
if parsed_frame:
    print(f"Decoded: {parsed_frame}")
```

### DataWeb Integration

```python
# Import directly from can_recompiler (available on worker nodes):
from can_recompiler import (
    CANFrameProcessor,
    detect_protocol_for_df, 
    create_decode_traces_udf,
    create_passthrough_dataframe,
    TransportProtocolType,
    ApplicationProtocolType
)
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType

# Create protocol detection UDF 
detect_protocol_udf = udf(detect_protocol_for_df, IntegerType())

# Create decoding UDF for Spark processing
decode_udf = create_decode_traces_udf(COLUMNS)

# Apply protocol classification and decoding
classified_df = raw_frames_df.withColumn("transport_id", detect_protocol_udf(col("id")))
decoded_df = classified_df.groupBy("trace_uuid").applyInPandas(decode_udf, schema=SCHEMA)
```

## Architecture

The library is organized into several focused modules:

- **`processor.py`**: Main `CANFrameProcessor` interface
- **`isotp_processor.py`**: ISO-TP (UDS/OBD-II) frame reassembly 
- **`j1939_processor.py`**: J1939 Transport Protocol handling
- **`spark_udfs.py`**: Spark UDF functions for distributed processing
- **`frame_types.py`**: Data structures for frames and parsing results
- **`enums.py`**: Protocol and identifier type enumerations
- **`utils.py`**: Helper functions for data extraction

## Supported Protocols

### Transport Protocols
- **ISO-TP** (ISO 15765-2): UDS and OBD-II communications
- **J1939**: Heavy-duty vehicle transport protocol

### Application Protocols  
- **UDS** (Unified Diagnostic Services)
- **J1939** (SAE J1939 application layer)

## Development

> **For detailed development instructions, testing, and contribution guidelines, see [can_recompiler/README.md](can_recompiler/README.md)**

### Building Independently

```bash
cd dataplatform/dataweb/userpkgs/firmware/can
make build && make install
# OR for development
make install-dev
```

## Environment Variables

- `CAN_RECOMPILER_DEBUG=true`: Enable debug logging output

## License

Proprietary - Samsara Inc.
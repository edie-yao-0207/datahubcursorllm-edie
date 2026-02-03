"""Widget specification builders for different visualization types."""


def build_counter_spec(title: str, desc: str, columns: list, config: dict) -> dict:
    """Build counter widget spec."""
    value_field = config.get("value_field")

    if not value_field and columns:
        # Auto-detect value field
        for col in columns:
            if col in ["value", "total", "count", "sum", "avg"]:
                value_field = col
                break
            if any(x in col.lower() for x in ["count", "total", "cost", "users"]):
                value_field = col
                break
        if not value_field:
            value_field = columns[0] if columns else "value"

    encoding = {
        "value": {
            "fieldName": value_field,
            "rowNumber": config.get("row_number", 1),
        }
    }

    # Add format if specified
    fmt = config.get("format")
    if fmt == "currency":
        encoding["value"]["format"] = {
            "type": "number-currency",
            "currencyCode": "USD",
            "abbreviation": "none",
            "decimalPlaces": {"type": "max", "places": 2},
        }
    elif fmt == "percent":
        decimal_places = config.get("decimal_places", 1)
        encoding["value"]["format"] = {
            "type": "number-percent",
            "decimalPlaces": {"type": "max", "places": decimal_places},
        }
    elif fmt in ("number", "number-plain"):
        decimal_places = config.get("decimal_places", 0)
        abbreviation = config.get("abbreviation", "none")
        encoding["value"]["format"] = {
            "type": "number-plain",
            "abbreviation": abbreviation,
            "decimalPlaces": {"type": "max", "places": decimal_places},
        }

    # Add target encoding if specified (for counter with target comparison)
    target_field = config.get("target_field")
    if target_field:
        encoding["target"] = {
            "fieldName": target_field,
            "format": {
                "type": "number-plain",
                "abbreviation": "none",
                "decimalPlaces": {"type": "max", "places": 2},
            },
        }

    frame = {"showTitle": True, "title": title}
    if desc:
        frame["showDescription"] = True
        frame["description"] = desc

    return {
        "version": 2,
        "widgetType": "counter",
        "encodings": encoding,
        "frame": frame,
    }


def build_line_spec(title: str, desc: str, columns: list, config: dict) -> dict:
    """Build line chart spec.

    Config options:
        x_axis: str (field name for x-axis, default "date")
        y_fields: list[str] (field names for y-axis)
        y_format: "currency" | "percent" | "number" (format for y-axis)
        y_label: str (y-axis label)
        x_label: str (x-axis label)
        color_by: str (field to group/color by - enables multi-series charts)
        show_legend: bool (default True)
        show_points: bool (show data points, default False)
        reference_lines: list[dict] (SLO/SLI threshold lines)
            - value: number (y-value for the line)
            - label: str (display name)
            - color: str ("green", "red", "yellow", "#hex")
            - style: str ("solid", "dashed", "dotted")
    """
    x_axis = config.get("x_axis", "date")
    y_fields = config.get("y_fields", [])
    color_by = config.get("color_by")

    if not y_fields and columns:
        # Auto-detect y fields
        y_fields = [
            c
            for c in columns
            if c
            not in [
                "date",
                "database",
                "table",
                "service",
                "team",
                "region",
                "name",
                "status",
                "alert_type",
            ]
        ][:3]

    # Validate y_fields is not empty
    if not y_fields:
        raise ValueError(
            f"Cannot build line chart '{title}': no y-axis fields specified. "
            f"Either provide 'y_fields' in config or ensure query returns plottable columns."
        )

    # Build y-axis encoding with optional formatting
    y_format = config.get("y_format")

    # For single field, use fieldName directly; for multiple, use fields array
    if len(y_fields) == 1:
        y_encoding = {"fieldName": y_fields[0], "scale": {"type": "quantitative"}}
    else:
        y_encoding = {
            "scale": {"type": "quantitative"},
            "fields": [{"fieldName": f} for f in y_fields],
        }

    # Apply y-axis format (Databricks format structure)
    if y_format == "currency":
        y_encoding["format"] = {
            "type": "number-currency",
            "currencyCode": "USD",
            "abbreviation": "none",
            "decimalPlaces": {"type": "max", "places": 2},
        }
    elif y_format == "percent":
        y_encoding["format"] = {
            "type": "number-percent",
            "decimalPlaces": {"type": "max", "places": 1},
        }
    elif y_format == "number":
        y_encoding["format"] = {
            "type": "number-plain",
            "abbreviation": "none",
            "decimalPlaces": {"type": "max", "places": 0},
        }

    # Add y-axis label if specified
    if config.get("y_label"):
        y_encoding["axis"] = {"title": config["y_label"]}

    # Build x-axis encoding
    x_encoding = {"fieldName": x_axis, "scale": {"type": "temporal"}}

    # Add x-axis label if specified
    if config.get("x_label"):
        x_encoding["axis"] = {"title": config["x_label"]}

    spec = {
        "version": 3,
        "widgetType": "line",
        "encodings": {"x": x_encoding, "y": y_encoding},
        "frame": {
            "showTitle": True,
            "title": title,
            "showDescription": bool(desc),
            "description": desc,
        },
    }

    # Add color encoding for multi-series/stacked charts
    if color_by:
        spec["encodings"]["color"] = {
            "fieldName": color_by,
            "scale": {"type": "categorical"},
        }

    # Optional: show data points
    if config.get("show_points"):
        spec["point"] = True

    # Add SLO/SLI reference lines as annotations (Databricks format)
    reference_lines = config.get("reference_lines", [])
    if reference_lines:
        spec["annotations"] = []
        # Color positions for Databricks theme colors
        color_map = {
            "red": 0,
            "orange": 1,
            "yellow": 2,
            "green": 3,
            "blue": 4,
            "purple": 5,
        }
        for i, ref in enumerate(reference_lines):
            color_pos = color_map.get(ref.get("color", "").lower(), i)
            annotation = {
                "type": "horizontal-line",
                "encodings": {
                    "y": {
                        "dataValue": str(ref.get("value", 0)),
                        "dataType": "DOUBLE",
                    },
                    "label": {
                        "value": ref.get("label", f"Target: {ref.get('value')}")
                    },
                    "color": {
                        "value": {
                            "themeColorType": "visualizationColors",
                            "position": color_pos,
                        }
                    },
                },
            }
            spec["annotations"].append(annotation)

    # Add label encoding (hide by default for cleaner charts)
    spec["encodings"]["label"] = {"show": False}

    return spec


def build_bar_spec(title: str, desc: str, columns: list, config: dict) -> dict:
    """Build bar chart spec.

    Config options:
        x_axis: str (field name for x-axis)
        y_fields: list[str] (field names for y-axis)
        y_format: "currency" | "percent" | "number" (format for y-axis)
        y_label: str (y-axis label)
        x_label: str (x-axis label)
        stacked: bool (stack bars, default False)
        horizontal: bool (horizontal bars, default False)
        color_by: str (field to color by)
    """
    x_axis = config.get("x_axis", "date")
    y_fields = config.get("y_fields", [])
    stacked = config.get("stacked", False)

    if not y_fields and columns:
        y_fields = [
            c
            for c in columns
            if c
            not in [
                "date",
                "database",
                "table",
                "service",
                "team",
                "region",
                "name",
                "status",
                "alert_type",
            ]
        ][:3]

    # Validate y_fields is not empty
    if not y_fields:
        raise ValueError(
            f"Cannot build bar chart '{title}': no y-axis fields specified. "
            f"Either provide 'y_fields' in config or ensure query returns plottable columns."
        )

    frame = {"showTitle": True, "title": title}
    if desc:
        frame["showDescription"] = True
        frame["description"] = desc

    # Build y-axis encoding with optional formatting (Databricks format)
    y_format = config.get("y_format")

    if len(y_fields) == 1:
        y_encoding = {"fieldName": y_fields[0], "scale": {"type": "quantitative"}}
    else:
        y_encoding = {
            "scale": {"type": "quantitative"},
            "fields": [{"fieldName": f} for f in y_fields],
        }

    # Apply y-axis format (Databricks format structure)
    if y_format == "currency":
        y_encoding["format"] = {
            "type": "number-currency",
            "currencyCode": "USD",
            "abbreviation": "none",
            "decimalPlaces": {"type": "max", "places": 2},
        }
    elif y_format == "percent":
        y_encoding["format"] = {
            "type": "number-percent",
            "decimalPlaces": {"type": "max", "places": 1},
        }
    elif y_format == "number":
        y_encoding["format"] = {
            "type": "number-plain",
            "abbreviation": "none",
            "decimalPlaces": {"type": "max", "places": 0},
        }

    if config.get("y_label"):
        y_encoding["axis"] = {"title": config["y_label"]}

    if stacked:
        y_encoding["stackType"] = "stacked"

    # Build x-axis encoding
    x_encoding = {
        "fieldName": x_axis,
        "scale": {"type": "categorical"},
        "axis": {"labelAngle": 45},
    }

    if config.get("x_label"):
        x_encoding["axis"]["title"] = config["x_label"]

    spec = {
        "version": 3,
        "widgetType": "bar",
        "encodings": {"x": x_encoding, "y": y_encoding},
        "frame": frame,
    }

    # Optional: color encoding
    color_scheme = config.get("color_scheme")
    color_field = config.get("color_field") or config.get("color_by")
    color_reverse = config.get("color_reverse", False)

    if color_scheme:
        # Quantitative color with color scheme (e.g., "spectral")
        # Default to y-field for color if not explicitly set
        if not color_field and y_fields:
            color_field = y_fields[0]
        if color_field:
            color_scale = {
                "type": "quantitative",
                "colorRamp": {"mode": "scheme", "scheme": color_scheme},
            }
            if color_reverse:
                color_scale["reverse"] = True
            spec["encodings"]["color"] = {
                "fieldName": color_field,
                "scale": color_scale,
            }
    elif color_field:
        # Categorical color (original behavior)
        spec["encodings"]["color"] = {
            "fieldName": color_field,
            "scale": {"type": "categorical"},
        }

    return spec


def build_area_spec(title: str, desc: str, columns: list, config: dict) -> dict:
    """Build area chart spec.

    Config options (same as line chart):
        x_axis: str (field name for x-axis, default "date")
        y_fields: list[str] (field names for y-axis)
        y_format: "currency" | "percent" | "number" (format for y-axis)
        y_label: str (y-axis label)
        x_label: str (x-axis label)
        stacked: bool (stack areas, default True for area)
        color_by: str (field to group/color by - enables multi-series charts)
    """
    spec = build_line_spec(title, desc, columns, config)
    spec["widgetType"] = "area"

    color_by = config.get("color_by")
    stacked = config.get("stacked", True)

    # Default to stacked for area charts
    if stacked:
        if color_by:
            # When color_by is used, we have a single y_field and use stackType: "stacked"
            # This matches Databricks format for grouped stacked area charts
            if "fieldName" in spec["encodings"]["y"]:
                spec["encodings"]["y"]["stackType"] = "stacked"
        else:
            # For stacked area charts without color_by, use SUM-wrapped field names
            # to match Databricks export format
            if "fields" in spec["encodings"]["y"]:
                y_fields = config.get("y_fields", [])
                spec["encodings"]["y"]["fields"] = [
                    {"fieldName": f"sum({field})"} for field in y_fields
                ]
                # For stacked charts without color_by, also set stackType
                spec["encodings"]["y"]["stackType"] = "stacked"

    return spec


def build_pie_spec(title: str, desc: str, columns: list, config: dict) -> dict:
    """Build pie chart spec.

    Config options:
        label_field: str (field name for slice labels/categories)
        value_field: str (field name for slice values)
    """
    label_field = config.get("label_field")
    value_field = config.get("value_field")

    # Try to auto-detect fields if not specified
    if not label_field and columns:
        # Look for a categorical field
        for col in columns:
            if col.lower() in [
                "name",
                "label",
                "category",
                "type",
                "status",
                "stage",
                "outcome",
                "label_status",
            ]:
                label_field = col
                break
        if not label_field:
            label_field = columns[0]

    if not value_field and columns:
        # Look for a numeric field
        for col in columns:
            if col.lower() in ["count", "value", "total", "sum", "amount"]:
                value_field = col
                break
        if not value_field and len(columns) > 1:
            value_field = columns[1]

    if not label_field or not value_field:
        raise ValueError(
            f"Cannot build pie chart '{title}': need both label_field and value_field. "
            f"Either provide them in config or ensure query returns appropriate columns."
        )

    frame = {"showTitle": True, "title": title}
    if desc:
        frame["showDescription"] = True
        frame["description"] = desc

    spec = {
        "version": 3,
        "widgetType": "pie",
        "encodings": {
            "angle": {
                "fieldName": value_field,
                "scale": {"type": "quantitative"},
            },
            "color": {
                "fieldName": label_field,
                "scale": {"type": "categorical"},
            },
            "label": {
                "show": config.get("show_labels", True),
            },
        },
        "frame": frame,
    }

    return spec


def build_heatmap_spec(title: str, desc: str, columns: list, config: dict) -> dict:
    """Build heatmap chart spec.

    Config options:
        x_field: str (field for x-axis categories)
        y_field: str (field for y-axis categories)
        color_field: str (field for color intensity/values)
        auto_columns: bool - if True, omit explicit field encodings and let
            Databricks auto-discover columns from query results
    """
    auto_columns = config.get("auto_columns", False)
    
    frame = {"showTitle": True, "title": title}
    if desc:
        frame["showDescription"] = True
        frame["description"] = desc

    # If auto_columns is enabled, return minimal spec with empty encodings
    # Databricks will auto-detect x, y, color fields from query results
    if auto_columns:
        return {
            "version": 3,
            "widgetType": "heatmap",
            "encodings": {},
            "frame": frame,
        }
    
    x_field = config.get("x_field") or config.get("x_axis")
    y_field = config.get("y_field") or config.get("y_axis")
    color_field = config.get("color_field") or config.get("value_field")

    # Auto-detect fields if not specified
    if columns and len(columns) >= 3:
        if not x_field:
            x_field = columns[0]
        if not y_field:
            y_field = columns[1]
        if not color_field:
            # Look for a numeric field for color
            for col in columns:
                if col.lower() in [
                    "count",
                    "value",
                    "total",
                    "sum",
                    "event_count",
                ]:
                    color_field = col
                    break
            if not color_field:
                color_field = columns[2]

    if not x_field or not y_field or not color_field:
        raise ValueError(
            f"Cannot build heatmap '{title}': need x_field, y_field, and color_field. "
            f"Either provide them in config or ensure query returns at least 3 columns."
        )

    spec = {
        "version": 3,
        "widgetType": "heatmap",
        "encodings": {
            "x": {
                "fieldName": x_field,
                "scale": {"type": "categorical"},
            },
            "y": {
                "fieldName": y_field,
                "scale": {"type": "categorical"},
            },
            "color": {
                "fieldName": color_field,
                "scale": {"type": "quantitative"},
            },
        },
        "frame": frame,
    }

    return spec


def build_histogram_spec(title: str, desc: str, columns: list, config: dict) -> dict:
    """Build histogram chart spec.

    Config options:
        x_field: str (field for x-axis quantitative values)
        y_field: str (field for y-axis aggregation, e.g., "count(trace_cnt)")
        color_field: str (field for color, defaults to "count(*)")
        color_scheme: str (color scheme, default "spectral")
        color_reverse: bool (reverse color scale, default True)
    """
    x_field = config.get("x_field") or config.get("x_axis")
    y_field = config.get("y_field")
    color_field = config.get("color_field")
    color_scheme = config.get("color_scheme", "spectral")
    color_reverse = config.get("color_reverse", True)

    # Auto-detect fields if not specified
    if columns and len(columns) >= 1:
        if not x_field:
            x_field = columns[0]

    if not x_field:
        raise ValueError(
            f"Cannot build histogram '{title}': need x_field. "
            f"Either provide it in config or ensure query returns at least 1 column."
        )

    # Default y_field to count(x_field) if not set
    if not y_field:
        y_field = f"count({x_field})"

    # Default color field to count(*)
    if not color_field:
        color_field = "count(*)"

    frame = {"showTitle": True, "title": title}
    if desc:
        frame["showDescription"] = True
        frame["description"] = desc

    color_scale = {
        "type": "quantitative",
        "colorRamp": {"mode": "scheme", "scheme": color_scheme},
    }
    if color_reverse:
        color_scale["reverse"] = True

    spec = {
        "version": 3,
        "widgetType": "histogram",
        "encodings": {
            "x": {
                "fieldName": x_field,
                "scale": {"type": "quantitative"},
            },
            "y": {
                "fieldName": y_field,
                "scale": {"type": "quantitative"},
            },
            "color": {
                "fieldName": color_field,
                "scale": color_scale,
            },
            "label": {"show": False},
        },
        "frame": frame,
    }

    return spec


def build_table_spec(title: str, desc: str, columns: list, config: dict) -> dict:
    """Build table widget spec.

    Config options:
        items_per_page: int (default 25)
        condensed: bool (default True)
        auto_columns: bool (default False) - if True, don't create explicit
            column definitions; let Databricks auto-discover columns from
            query results. Useful when using SELECT * or similar.
        columns: dict mapping column names to format options:
            format: "currency" | "percent" | "number" | "date" | "string"
            decimals: int (for number/currency)
            title: str (display name override)
            visible: bool
            align: "left" | "right" | "center"
            width: int (column width in pixels)
            word_wrap: bool (wrap long text)
            highlight_negatives: bool (for numbers, show negatives in red)
            scale_by_100: bool (for percent, multiply 0-1 values by 100, default True)
    """
    # If auto_columns is True, let Databricks auto-discover all columns
    auto_columns = config.get("auto_columns", False)
    
    # Column format overrides from config
    col_config = config.get("columns", {})

    col_defs = []
    
    # Skip explicit column definitions if auto_columns is enabled
    # This lets Databricks auto-discover all columns from the query result
    if not auto_columns:
        for i, col in enumerate(columns):
            # Get column-specific config
            col_fmt = col_config.get(col, {})

            # Determine column type and format
            col_type = "string"
            display_as = "string"
            align = col_fmt.get("align", "left")
            fmt = col_fmt.get("format")

            # Auto-detect format if not specified
            if not fmt:
                col_lower = col.lower()
                if any(
                    x in col_lower
                    for x in ["cost", "price", "spend", "budget", "revenue"]
                ):
                    fmt = "currency"
                elif any(
                    x in col_lower
                    for x in ["percent", "pct", "ratio", "growth", "rate"]
                ):
                    fmt = "percent"
                elif any(
                    x in col_lower
                    for x in [
                        "count",
                        "queries",
                        "users",
                        "total",
                        "avg",
                        "sum",
                        "score",
                        "days",
                        "num_",
                        "z_score",
                    ]
                ):
                    fmt = "number"
                elif any(x in col_lower for x in ["date", "time", "timestamp"]):
                    fmt = "date"

            # Determine type, displayAs, alignContent, and numberFormat based on format
            # Databricks tables use "float"/"integer" for type, not "number"
            # numberFormat uses patterns like "$#,##0.00" for currency, "0.0%" for percent
            scale_percent = False  # Only set to True for percent format
            if fmt == "currency":
                col_type = "float"
                display_as = "number"
                align = col_fmt.get("align", "right")
                decimals = col_fmt.get("decimals", 2)
                number_format = "$#,##0" + (f".{'0' * decimals}" if decimals > 0 else "")
            elif fmt == "percent":
                col_type = "float"
                display_as = "number"
                align = col_fmt.get("align", "right")
                decimals = col_fmt.get("decimals", 1)
                number_format = "0" + (f".{'0' * decimals}" if decimals > 0 else "") + "%"
                # Flag to multiply 0-1 values by 100 for display (default True for percent)
                scale_percent = col_fmt.get("scale_by_100", True)
            elif fmt == "number":
                decimals = col_fmt.get("decimals")
                if decimals is None or decimals == 0:
                    col_type = "integer"
                    number_format = "0"
                else:
                    col_type = "float"
                    number_format = "0" + f".{'0' * decimals}"
                display_as = "number"
                align = col_fmt.get("align", "right")
            elif fmt == "date":
                col_type = "datetime"
                display_as = "datetime"
                align = col_fmt.get("align", "left")
                number_format = None
            elif fmt == "link":
                # Link columns display as clickable links
                col_type = "string"
                display_as = "link"
                align = col_fmt.get("align", "left")
                number_format = None
            else:
                # Default to string (includes "string", "text", and unrecognized)
                col_type = "string"
                display_as = "string"
                align = col_fmt.get("align", "left")
                number_format = None

            # Build column definition with EXACT property order that Databricks expects
            # Order matters! This matches the working format from manual fixes
            col_def = {"fieldName": col}

            # numberFormat comes early (before booleanValues) if present
            if number_format is not None:
                col_def["numberFormat"] = number_format

            # Template and link properties in specific order
            col_def["booleanValues"] = ["false", "true"]
            col_def["imageUrlTemplate"] = "{{ @ }}"
            col_def["imageTitleTemplate"] = "{{ @ }}"
            col_def["imageWidth"] = ""
            col_def["imageHeight"] = ""
            col_def["linkUrlTemplate"] = "{{ @ }}"
            # For link columns, show custom label; otherwise show the value
            link_label = col_fmt.get("label", "Link") if display_as == "link" else "{{ @ }}"
            col_def["linkTextTemplate"] = link_label
            col_def["linkTitleTemplate"] = "{{ @ }}"
            col_def["linkOpenInNewTab"] = True

            # Type and display properties
            col_def["type"] = col_type
            col_def["displayAs"] = display_as

            # Visibility and ordering
            col_def["visible"] = col_fmt.get("visible", True)
            col_def["order"] = 100000 + i
            col_def["title"] = col_fmt.get("title", col)

            # Search and alignment
            col_def["allowSearch"] = False
            col_def["alignContent"] = align

            # Final properties
            col_def["allowHTML"] = False
            col_def["highlightLinks"] = False
            col_def["useMonospaceFont"] = False
            col_def["preserveWhitespace"] = False

            # Optional properties
            if col_fmt.get("width"):
                col_def["width"] = col_fmt["width"]
            if col_fmt.get("word_wrap"):
                col_def["wordWrap"] = True
            if fmt == "date":
                col_def["dateTimeFormat"] = col_fmt.get("date_format", "YYYY-MM-DD")
            if scale_percent:
                col_def["scalePercentBy100"] = True

            # Add conditional formatting if specified
            conditional_fmt = col_fmt.get("conditional_format")
            if conditional_fmt:
                # Color name to Databricks theme color position mapping
                color_map = {
                    "red": 4,
                    "orange": 2,
                    "yellow": 2,
                    "green": 3,
                    "blue": 0,
                    "purple": 1,
                    "cyan": 5,
                    "pink": 6,
                }

                rules = []
                for rule in conditional_fmt:
                    fn = rule.get("fn", "is not null")
                    value = rule.get("value")
                    color = rule.get("color", "green")

                    color_position = color_map.get(color.lower(), 3)

                    rule_def = {
                        "if": {
                            "column": col,
                            "fn": fn,
                        },
                        "value": {
                            "foregroundColor": {
                                "themeColorType": "visualizationColors",
                                "position": color_position,
                            }
                        },
                    }

                    # Add literal value if needed (not for "is not null" etc)
                    if value is not None and fn not in ["is not null", "is null"]:
                        rule_def["if"]["literal"] = str(value)

                    rules.append(rule_def)

                col_def["cellFormat"] = {
                    "default": {"foregroundColor": None},
                    "rules": rules,
                }

            col_defs.append(col_def)

    frame = {"showTitle": True, "title": title}

    # Add description if present
    if desc:
        frame["showDescription"] = True
        frame["description"] = desc

    # Get items per page from config (default 25)
    items_per_page = config.get("items_per_page", 25)

    # Get condensed mode from config (default True)
    condensed = config.get("condensed", True)

    # Build encodings - if auto_columns is enabled, omit columns entirely
    # to let Databricks auto-discover and show all columns from the query
    if auto_columns:
        encodings = {}
    else:
        encodings = {"columns": col_defs}

    return {
        "version": 1,
        "widgetType": "table",
        "encodings": encodings,
        "invisibleColumns": [],
        "allowHTMLByDefault": False,
        "itemsPerPage": items_per_page,
        "paginationSize": "default",
        "condensed": condensed,
        "withRowNumber": False,
        "frame": frame,
    }


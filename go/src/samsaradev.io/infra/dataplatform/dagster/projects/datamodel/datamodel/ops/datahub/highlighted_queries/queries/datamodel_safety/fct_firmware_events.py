queries = {
    "Share of events triggered by firmware release (CM 31/32)": """
                    WITH build_counts AS (
                    SELECT
                        `date`,
                        device_build,
                        COUNT(*) build_count
                    FROM
                        datamodel_safety.fct_firmware_events
                    WHERE
                        `date` >= TO_DATE(date_sub(current_date(), 7)) -- Events on either CM 31/32 itself or a VG connected to a CM 31/32
                        -- AND (product_id IN (43,44) OR associated_product_id IN (43,44))
                        AND product_id IN (43, 44)
                    GROUP BY
                        1,
                        2
                    ORDER BY
                        1,
                        3 DESC
                    )
                    SELECT
                    `date`,
                    device_build,
                    build_count,
                    build_count / SUM(build_count) OVER (PARTITION BY `date`) AS share_build
                    FROM
                    build_counts
                    WHERE
                    build_count > 500 -- filter out smaller builds for readability
                    ORDER BY
                    1,
                    4 DESC
        """,
    "Share of each event type in the backend (CM 31/32)": """
                    SELECT
                        harsh_accel_name,
                        COUNT_IF(in_backend) AS in_backend_count,
                        COUNT(*) AS total_count,
                        ROUND(in_backend_count / total_count, 4) AS share_in_backend
                        FROM
                        datamodel_safety.fct_firmware_events
                        WHERE
                        `date` >= TO_DATE(date_sub(current_date(), 7)) -- Events on either CM 31/32 itself or a VG connected to a CM 31/32
                        AND (
                            product_id IN (43, 44)
                            OR associated_product_id IN (43, 44)
                        )
                        GROUP BY
                        1
                        ORDER BY
                        4 DESC
                                """,
    "Mean number of events by type (CM 31/32)": """
                                        SELECT
                        harsh_accel_name,
                        ROUND(
                            COUNT(*) / 7,
                            2
                        ) AS average_counts
                        FROM
                        datamodel_safety.fct_firmware_events
                        WHERE
                        `date` >= TO_DATE(date_sub(current_date(), 7)) -- Events on either CM 31/32 itself or a VG connected to a CM 31/32
                        AND (
                            product_id IN (43, 44)
                            OR associated_product_id IN (43, 44)
                        )
                        GROUP BY
                        1
                        ORDER BY
                        2 DESC
        """,
}

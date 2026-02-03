queries = {
    "Current gateway to device pairings, with device metadata": """
        with devices as
        (select device_id, product_name, product_id, device_type
        from datamodel_core.dim_devices
        where date = (select max(date) from datamodel_core.dim_devices))
        , serials as
        (select device_id, serial
        from datamodel_core.dim_devices_sensitive
        where date = (select max(date) from datamodel_core.dim_devices_sensitive))

        select intervals.gateway_id,
            intervals.device_id,
            intervals.start_interval,
            intervals.prev_device_id,
            intervals.prev_gateway_id,
            intervals.is_current,
            devices.product_id,
            devices.product_name,
            devices.device_type,
            serials.serial
        from datamodel_core.fct_gateway_device_intervals intervals
        left join devices
        on intervals.device_id = devices.device_id
        left join serials
        on intervals.device_id = serials.device_id
        where intervals.is_current = true
            """
}

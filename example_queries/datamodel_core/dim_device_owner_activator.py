queries = {
    "Activated AG51 devices with mismatch between owner and activator": """
        select
        date,
        device_id,
        first_heartbeat_date,
        last_heartbeat_date,
        last_activation_sdfc_account_id,
        order_sfdc_account_id,
        p.*
        from
        datamodel_core.dim_device_owner_activator doa
        join definitions.products p on p.product_id = doa.product_id
        where
        p.name = 'AG51'
        and order_date between (date(order_date) - interval 1 month)
        and now()
        and date = (
            select
            max(date)
            from
            datamodel_core.dim_device_owner_activator
        )
        and doa.is_owner_activator_mismatch
        LIMIT
        1000
            """
}

queries = {
    "Product variant name for the default variant of all VG products": """
    select
        pv.product_id,
        pv.variant_id,
        pv.variant_name
    from datamodel_core.dim_product_variants pv
    join definitions.products p
    on pv.product_id = p.product_id
    where pv.variant_id = 0
    and p.name like 'VG%'
            """
}

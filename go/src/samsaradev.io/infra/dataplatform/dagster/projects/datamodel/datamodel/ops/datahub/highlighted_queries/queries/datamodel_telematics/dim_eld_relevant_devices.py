queries = {
    "Count of eld relevant devices as of most recent date partition": """
    select count_if(is_eld_relevant) as eld_relevant_device_count
    from datamodel_telematics.dim_eld_relevant_devices
    where date = (select max(date) from datamodel_telematics.dim_eld_relevant_devices)
            """
}

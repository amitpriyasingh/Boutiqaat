SELECT
    attribute_id,
    entity_type_id,
    attribute_code,
    attribute_model,
    backend_model,
    backend_type,
    backend_table,
    frontend_model,
    frontend_input,
    frontend_label,
    frontend_class,
    source_model,
    is_required,
    is_user_defined,
    default_value,
    is_unique,
    note
FROM boutiqaat_v2.eav_attribute
WHERE \$CONDITIONS
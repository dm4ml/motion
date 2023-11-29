::: motion.dicts.Properties
    handler: python
    options:
        members:
            - serve_result
        show_root_full_path: false
        show_root_toc_entry: false
        show_root_heading: true
        show_source: false
        show_signature_annotations: true

::: motion.dicts.State
    handler: python
    options:
        members:
            - instance_id
        show_root_full_path: false
        show_root_toc_entry: false
        show_root_heading: true
        show_source: false
        show_signature_annotations: true

::: motion.MTable
    handler: python
    options:
        members:
            - data
            - filesystem
            - from_pandas
            - from_arrow
            - from_schema
            - add_row
            - remove_row
            - add_column
            - append_column
            - remove_column
            - remove_column_by_name
            - knn
            - apply_distance
        show_root_full_path: false
        show_root_toc_entry: false
        show_root_heading: true
        show_source: true
        show_signature_annotations: true


::: motion.dicts.MDataFrame
    handler: python
    options:
        members:
            - __getstate__
            - __setstate__
        show_root_full_path: false
        show_root_toc_entry: false
        show_root_heading: true
        show_source: true
        show_signature_annotations: true

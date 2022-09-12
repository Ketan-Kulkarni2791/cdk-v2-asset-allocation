"""Module for Storing the Schema Description for Asset Allocation Tables."""


class tableSchemas:
    """Storing the schemas of the various tables."""
    taa_saa_paa_metadata = [
        {'Name': 'file_name', 'Type': 'varchar'},
        {'Name': 'version_number', 'Type': 'int'},
        {'Name': 'insertion_date', 'Type': 'timestamp'},
        {'Name': 'etag', 'Type': 'varchar'},
        {'Name': 'file_date', 'Type': 'date'},
        {'Name': 'status', 'Type': 'varchar'}
    ]

    asset_alloc_data = [
        {'Name': 'amount', 'Type': 'double'},
        {'Name': 'amount_type', 'Type': 'varchar(75)'},
        {'Name': 'level', 'Type': 'double'},
        {'Name': 'wal_nm', 'Type': 'int'},
        {'Name': 'pfg_ast_clss_nm', 'Type': 'varchar(75)'},
        {'Name': 'pfg_sblvl_1_nm', 'Type': 'varchar(75)'},
        {'Name': 'pfg_svlvl_1_5_nm', 'Type': 'varchar(75)'},
        {'Name': 'pfg_sblvl_2_nm', 'Type': 'varchar(75)'},
        {'Name': 'pfg_sblvl_3_nm', 'Type': 'varchar(75)'},
        {'Name': 'pfg_sblvl_4_nm', 'Type': 'varchar(75)'},
    ]
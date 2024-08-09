import aws_cdk.aws_glue_alpha as glue
from layers.buckets import pollingstations_private_data
from layers.databases import dc_data_baker
from layers.models import BaseTable, BaseTableColumns


class AddressBaseCleanedColumns(BaseTableColumns):
    uprn: str
    address: str
    postcode: str
    location: str
    address_type: str



addressbase_cleaned_raw = BaseTable(
    table_name="addressbase_cleaned_raw",
    description="Addressbase table as produced for loading into WDIV",
    bucket=pollingstations_private_data,
    s3_prefix="addressbase/current/addressbase_cleaned_raw/",
    database=dc_data_baker,
    data_format=glue.DataFormat.CSV,
    columns=AddressBaseCleanedColumns
)

TABLES = [
    addressbase_cleaned_raw
]

if __name__ == "__main__":
    print(addressbase_cleaned_raw)
    print(addressbase_cleaned_raw.as_glue_definition())

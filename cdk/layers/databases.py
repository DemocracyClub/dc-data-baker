from layers.models import GlueDatabase

dc_data_baker = GlueDatabase(database_name="dc_data_baker")

DATABASES = [dc_data_baker]

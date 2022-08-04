# noticias_de_ayer
Desde cuentas de Twitter


Con este código, puede descargar tweets de una cuenta en particular. Luego cargar los tweets con los campos necesarios a una tabla en bigquery.


Campos resultantes:
text: string
created_at: timestamp
retweet_count: integer
favorite_count: integer
source: string
account_name: string


Se puede particionar por created_at y clusterizar por account_name.


Para ejecutar este código se debe setear o cambiar estas variables:
- TW_API_KEY
- TW_API_SECRET
- ACCOUNT_NAME

Y cambiar esta variable en el código: bq_table_id

# Tableau API
to make use of `tableauserverclient`, first `pip install optional_requirements.txt`  
you will also need a `secrets.toml` file in the following format:  
```
[tableau]
server = 'tableau.server.address.com'
site = 'insert-sitename'
token_name = 'INSERT-YOUR-TOKEN-NAME-HERE'
token_value = 'INSERT-YOUR-TABLEAU-API-KEY-HERE'
```
you can find your server address and site name from the url you use to access tableau, for example:  
`https://server.name.here.com/#/site/site_name` the server name would be `https://server.name.here.com` and the site name would be `site_name`  
next, uncomment the imports under `# for using tableau API:`, set `TABLEAU_API` to `True` and set the workbook name to the name of the workbook you use for mandatory use (view names must match those in the [data readme](data/README.md) without the `_data.csv`).

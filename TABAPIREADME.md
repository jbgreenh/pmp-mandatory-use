# Tableau API
to make use of `tableauserverclient`:  
1. first `pip install -r optional_requirements.txt`  
2. you will also need a `secrets.toml` file in the following format:  
    ```
    [tableau]
    server = 'tableau.server.address.com'
    site = 'insert-sitename'
    token_name = 'INSERT-YOUR-TOKEN-NAME-HERE'
    token_value = 'INSERT-YOUR-TABLEAU-API-KEY-HERE'
    ```
    you can find your server address and site name from the url you use to access tableau, for example:  
    `https://server.name.here.com/#/site/site_name` the server name would be `https://server.name.here.com` and the site name would be `site_name`  
3. update the workbook in tableau with these 4 parameters (you can set the default values all to the same date for speed in the tableau client, since the `mu` notebook sets these values itself for querying tableau anyway): 
    | parameter        | description |
    |------------------|-------------|
    | first_of_month   | short date  |
    | first_for_search | short date  |
    | last_of_month    | short date  |
    | last_for_search  | short date  |
4. add the following calculated fields:
    | calculated field   | data source     | code                                                                                         | description                                            |
    |--------------------|-----------------|----------------------------------------------------------------------------------------------|--------------------------------------------------------|
    | between_active     | dispensations   | [rx_end] >= [first_of_month] and [Filled At] <= [last_of_month]                              | replace other time based filters with this set to True |
    | between_f_l_month  | dispensations   | [Written At] <= [last_of_month] and [Written At] >= [first_of_month]                         | replace other time based filters with this set to True |
    | between_naive      | dispensations   | [naive_end] >= [first_of_month] and [Filled At] <= [last_of_month]                           | replace other time based filters with this set to True |
    | between_for_search | search requests | [Search Creation Date] <= [last_for_search] and [Search Creation Date] >= [first_for_search] | replace other time based filters with this set to true |
5. next, uncomment the imports under `# for using tableau API:`
6. set `TABLEAU_API` to `True` in the constants section and set `WORKBOOK_NAME` to the name of the workbook you use for mandatory use (view names must match those in the [data readme](data/README.md) without the `_data.csv`).
7. choose to either use `AUTO_DATE` which will use the full previous month (if today is `May 24, 2024`, written start date will be set to `April 1, 2024` and written end date will be set to `April 30, 2024`) or to set `FIRST_WRITTEN_DATE` and `LAST_WRITTEN_DATE` manually, note that setting these dates too far apart will significantly slow performance and may cause the `tableauserverclient` to time out

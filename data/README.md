# data  
the following is a description of the input files required in this ``data`` folder to successfully run the notebook  
file names and field names should match those below exactly
# base  
this data is required to run the notebook at its most basic version, outputting prescribers, dipsensations, searches, and search rate  
## ``dispensations_data.csv``
data on all dispensations requiring a search
___  
filter the data according to state mandatory use requirements  
| filter                   | description                                                                            |
|--------------------------|----------------------------------------------------------------------------------------|
| AHFS Description         | according to state requirements                                                        |
| Refill Y/N               | according to state requirements                                                        |
| Current Prescriber State | state                                                                                  |
| MY(Written At)           | month(s) for the report, <br />performance improves as fewer months are included |
| Days Supply              | according to state requirements                                                        |
| Prescriber PDMP Role     | exclude medical residents with prescriptive authority                                  |  
___
| field name                                  | description                                                 |
|---------------------------------------------|-------------------------------------------------------------|
| Animal Name                                 | for filtering veterinarians                                 |
| Prescriber DEA                              | for identifying the prescriber                              |
| Prescription Number                         | for singling out prescriptions                              |
| Generic Name                                | generic drug name                                           |
| AHFS Description                            | for distinguishing opioids and benzodiazepines              |
| Prescriber First Name                       | for when a prescriber is not registered                     |
| Prescriber Last Name                        | for when a prescriber is not registered                     |
| Patient First Name                          | for matching with searches                                  |
| Patient Last Name                           | for matching with searches                                  |
| Month, Day, Year of Written At              | when the prescription was written                           |
| Month, Day, Year of Filled At               | when the prescription was filled                            |
| Month, Day, Year of Dispensations Created At | when the record of the dispensation was reported to the PMP |
| Month, Day, Year of Patient Birthdate       | for matching with searches                                  |
| Days Supply                                 | as a dimension, for calculating overlaps                    |
| Daily MME                                   | as a dimension, for comparing to MME threshold              |
## ``searches_data.csv``  
data on searches performed
___
| filter | description |
|---|---|
| Search Creation Date | filter to the month(s) of the written date filter from the ``dispensations_data`` file, <br />with an additional ``DAYS_BEFORE`` from the previous month, and one extra day <br />from the following month |
___
| field name                               | description                                                                                                                           |
|------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------|
| True ID                                  | calculated field dimension that uses ``Delegator ID`` if it exists or ``Requestor ID`` if not: <br /> ``IFNULL([Delegator ID],[Requestor ID])`` |
| Month, Day, Year of Search Creation Date | when the search was performed                                                                                                         |
| Month, Day, Year of Searched DOB         | for matching to dispensations                                                                                                         |
| Searched First Name                      | for matching to dispensations                                                                                                         |
| Searched Last Name                       | for matching to dispensations                                                                                                         |
| Partial First Name?                      | for handling partial searches                                                                                                         |
| Partial Last Name?                       | for handling partial searches                                                                                                         |
## ``ID_data.csv``  
data on pmp database users
___
| filter | description                  |
|--------|------------------------------|
| Active | Y; only keep active accounts |
___
| field name               | description                         |
|--------------------------|-------------------------------------|
| User ID                  | dimension for identifying the user  |
| User Full Name           | for adding user name to the results |
| Associated DEA Number(s) | comma separated list of DEA numbers |
| User Specialty           | final specialty                     |
| Specialty Level 1        | first level of taxonomy             |
| Specialty Level 2        | second level of taxonomy            |
| Specialty Level 3        | third level of taxonomy             |
# supplement  
this data is required to add supplemental information such as overlapping prescriptions, opioids to opioid naive patients, etc.  
number of rx written over MME threshold and opioid and benzodiazepine counts do not require supplemental data
## ``active_rx_data.csv``  
all active prescriptions for opioids and benzodiazepines  
___
| filter           | description                                                                                                                                                  |
|------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------|
| rx_end           | calculated field ``[Filled At]+[Days Supply]``; <br />filter start date to the first day from the written date filter in ``dispensations_data``, no end date |
| Filled At        | no start date, filter end date to the last day from the written date filter in ``dispensations_data``                                                        |
| AHFS Description | opiate agonists, opiate partial agonists, <br />benzodiazepines (anticonvulsants), benzodiazepines (anxiolytic, sedative/hypnotics)                          |
___
| field name                                   | description                                                 |
|----------------------------------------------|-------------------------------------------------------------|
| Animal Name                                  | for filtering veterinarians                                 |
| Prescriber DEA                               | for identifying the prescriber                              |
| AHFS Description                             | for distinguishing opioids and benzodiazepines              |
| Month, Day, Year of Patient Birthdate        | for matching with other rx                                  |
| Patient First Name                           | for matching with other rx                                  |
| Patient Last Name                            | for matching with other rx                                  |
| Month, Day, Year of Dispensations Created At | when the record of the dispensation was reported to the PMP |
| Month, Day, Year of Written At               | when the prescription was written                           |
| Month, Day, Year of Filled At                | when the prescription was filled                            |
| Month, Day, Year of rx_end                   | calculated field ``[Filled At]+[Days Supply]``              |
## ``naive_rx_data.csv``  
all opioid prescriptions that are active + ``NAIVE_DAYS``
___

| filter           | description                                                                                                                                                                                     |
|------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| naive_end        | calculated field ``[rx_end]+60``; replace ``60`` with the desired ``NAIVE_DAYS``; <br />filter start date to the first day from the written date filter in ``dispensations_data``, no end date |
| Filled At        | no start date, filter end date to the last day from the written date filter in ``dispensations data``                                                                                           |
| AHFS Description | opiate agonists, opiate partial agonists                                                                                                                                                        |
___
| field name                            | description                                        |
|---------------------------------------|----------------------------------------------------|
| Animal Name                           | for filtering veterinarians                        |
| Month, Day, Year of Patient Birthdate | for matching with other rx                         |
| Patient First Name                    | for matching with other rx                         |
| Patient Last Name                     | for matching with other rx                         |
| Month, Day, Year of Filled At         | when the prescription was filled                   |
| Max. naive_end                        | max ``naive_end`` when grouped by the above fields |
# data  
the following is a description of the required input data to successfully run the notebook  
# base  
this data is required to run the notebook at its most basic version, outputting prescribers, dipsensations, searches, and search rate  
### dispensations_data.csv  
data on all dispensations requiring a search
### searches_data.csv  
data on searches performed
### ID_data.csv  
data on pmp database users
# supplement  
this data is required to add supplemental information such as overlapping prescriptions, opioids to opioid naive patients, etc.  
### active_rx_data.csv  
all active prescriptions for opioids and benzodiazepines  
### naive_rx_data.csv  
all opioid prescriptions that are active + ``NAIVE_DAYS``

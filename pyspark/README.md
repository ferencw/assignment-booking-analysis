# Commercial Booking Analysis

## Run the analysis locally
### Prerequisites
- Installed Docker 
- Access to docker registry which contains `datamechanics/spark:3.1-latest`

### Run the docker container locally
#### Setup input parameters (date range)
- Open `.env` file and edit the fields
```
    START_DATE=2018-11-11
    END_DATE=2019-12-27
```
#### Execute the follwing command to run the job
```
    docker compose up
```
#### Read results
- Got to `out/top_countries`
- Open the csv output

## Run the analysis on a hadoop cluster
#### Package the sorce
```
    ./zip.src.sh
```
#### Run you spark submit command to your cluster

```
spark-submit \
    --py-files file:///opt/application/my_spark_app.zip \
    src/main.py \
    --airport_data {Path to the Airports data} \
    --bookings_data {Path to your bookings data} \
    --start_date {Start date of the analysis} \
    --end_date {End date of the Analysis} \
    --output {Output dir to put the result}
```
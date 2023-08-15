# Commercial Booking Analysis

## Run the analysis locally

### Run the docker container locally
#### Prerequisites
- Installed Docker 
- Access to docker registry which contains `datamechanics/spark:3.1-latest`
- SBT installed and setup

#### Setup input parameters (date range)
- Open `.env` file and edit the fields
```
    START_DATE=2018-11-11
    END_DATE=2019-12-27
```
#### Execute the follwing commands to run the job
```
    sbt assembly
    docker compose up
```
#### Read results
- Got to `out/top_countries`
- Open the csv output

## Run the analysis on a hadoop cluster
#### Package the source
```
    sbt assembly
```
#### Run you spark submit command to your cluster

```
spark-submit \
    --class Main \ 
    {Path to application.jar} 
    {Path to the Airports data} \
    {Path to your bookings data} \
    {Start date of the analysis} \
    {End date of the Analysis} \
    {Output dir to put the result}
```
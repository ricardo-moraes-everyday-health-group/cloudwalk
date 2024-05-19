# Cloudwalk
Data Engineer Test

A docker to beat the challenge proposed [here](https://github.com/betosaulo-cw/data-engineer-test/tree/main)

 * Airflow slim-2.9.1
 * PostgreSQL 13

To keep the container light the Airflow backend database was used to persist the API data.

Prerequisites
-----

I assume you have installed Docker and it is running.

See the [Docker website](http://www.docker.io/gettingstarted/#h_installation) for installation instructions.

Build
-----

Steps to build a Docker image:

1. Clone this repo

        git clone https://github.com/moraes-s-ricardo/cloudwalk.git

2. Build the image and bring it up

        cd {your_base_path}/cloudwalk
        docker compose up airflow-init
        docker compose up

    This will configure all that's nedeed to run the DAG and bring them up.

3. Once everything has started up, you should be able to access the airflow UI via [http://localhost:8080/](http://localhost:8080/) on your host machine.

        User: airflow
        Password: airflow

4. Trigger the DAG named gross_domestic_product using the Airflow UI and wait a few seconds

5. The DAG will generate an HTML file name gdp_report.html on {your_base_path}/cloudwalk/output/. 
You can open it with your prefered browser and see a pivot table and the SQL statement used to create it.

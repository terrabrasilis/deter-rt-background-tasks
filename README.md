## DETER-RT background tasks

DETER-RT background tasks are a set of tasks designed to collect the input data from the shared location, import it into the database, perform an automated validation using the optical DETER, and mark the remaining alerts for use by the validator, following the draw rules. The scheduling of the execution of these tools is managed by Airflow.

In short, Airflow is a platform created by the community to programmatically author, schedule, and monitor workflows or DAGs. In Airflow, a DAG (Directed Acyclic Graph) is a collection of tasks that you want to run, organized in a way that reflects their relationships and dependencies. A DAG is defined in a Python script, which represents the DAG's structure (tasks and their dependencies) as code.

The overall architectural design is in the file [docs/overview.drawio.pdf](./docs/overview.drawio.pdf).


## Deploy

Steps to deploy DAGs in Airflow

    - Create AirFlow variables as behavior configurations. See the "Airflow Configurations" section;
    - Create AirFlow connections as data sources access configurations. See the "Airflow Configurations" section;
    - Clone this code repository into the /projects/ directory in the AirFlow directory structure;

### PostgreSQL Databases - TODO

Create a database based on database model described in the architectural design.


### Airflow configurations - TODO

Describes the details about the "Connections" and "Variables" that must be created in the Airflow environment to support the DAGs of this project.


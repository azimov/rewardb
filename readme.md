# REWARD-B

REal-World Assessment and Research of Drug Benefits.

## Project structure

The `dashboard` folder contains the shiny application.

The remainder of the project is organised as an R package.

## Installation
Inside an RStudio session, run the following:

    devtools::install()

## Creating a dashboard for your study
### Creating a configuration file
First, you need to create a configuration YAML file. 
The best way to do this is to copy one of the existing configurations in the `config` directory.
E.g. open the file `../config/config.dev.yml` and save a copy `../config/config.my-study.yml`.

Now editing the new file you can set the application name, short_name and description fields.
The `short_name` field should be unique to your application because it is the postgres schema that the results will be stored in.
For this example we are only interested in two outcome ids and a custom atlas cohort.
Edit the fields in the top of the file as follows:

    name: My study dashboard
    short_name: my_study # This is the name of the database schema!
    description: My study all about x
    outcome_concept_ids: [] # These are atlas ids and  will not be multiplied by 100
    custom_outcome_cohort_ids: [] # These are atlas ids and  will not be multiplied by 100
    target_concept_ids: ~

Notice that the `target_cohort_ids` field has the character`~`, this indicates that the field is
null. This is because, for this example, we are interested in all exposures for the specified outcomes.

We then need to make sure that the database is configured correctly (remember, YAML cares about indentation!):
    
    connectionDetails:
        dbms: "postgresql"
        server: "reward.cterqq54xyuu.us-east-1.rds.amazonaws.com/rewardb_dev"
        port: 5432
        user: "reward_admin"
        password: "you will need to find this from someone" 

The same applies for the CDM data source that the results were generated in:

    resultsDatabase:
        schema: "scratch.dbo"
        cohortDefinitionTable: "homer_cohort_definition_May2020"
        outcomeCohortDefinitionTable: "homer_outcome_cohort_definition_May2020"
        asurvResultsTable: "homer_AllbyAll_results_May2020"

### Making the database 
Then we want to create the database schema and perform empirical calibration if . 
Note that running this is required if 

    appContext <- rewardb::buildDatabase("config/config.my-study.yml")
    
### Running empirical calibration

    TODO

### Viewing the dashboard
Then you want to edit the dashboard file `dashboard/global.R` and edit the line that reads:

    appContext <- rewardb::loadAppContext("../config/config.tnfs.yml")
 
point at your new config file, e.g. `../config/config.my-study.yml`. 
The path must be relative to where `global.R` is found.

Then to run the shiny application, inside R:

    shiny::runApp("dashboard")

## Deploying to shared shiny server

TODO
 
 ## Authors
 Contact James Gilbert (JGilber2@its.jnj.com) or Chris Knoll (cknoll1@its.jnj.com) for more info
 on this software.

# Testing

The testing of this package requires postgres database connections.
This can either be configured in the file inst/tests/test.cfg or the included docker
file can be used to build a docker container.

If you wish to use the docker container (recommended), from the inst/test folder run the commands:

    docker build . -t eunomia
    docker run --rm -p 54321:5432 eunomia
    
The docker container will now be running with a database that can accept connections on port 54321.
The username and password are postgres. The CDM database is `eunomia` the rewardb database is `rewardb`.
 
In an RStudio session then run:
    
    devtools::test()
    
If, instead, you choose to use a hosted postgres instance modify the test.cfg.yml file
to the required settings. Note that the tests requires permissions for the creation and deletion of schemas.
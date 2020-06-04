# REWARD-B

REal-World Assessment and Research of Drug Benefits.

## Project structure

The `dashboard` folder contains the shiny application.

The remainder of the project is organised as an R package.

## dashboard

This repository includes the reward-b shiny application.
Currently under development. To use, point your RStudio workspace at the directory with this 
repository and run:

    devtools::install(".")
    shiny::runApp()
    
 The development version uses a small subest of data in an SQlite database. This is for quick
 development, full data requires access to generated results.
 
 ## Authors
 Contact James Gilbert (JGilber2@its.jnj.com) or Chris Knoll (cknoll1@its.jnj.com) for more info
 on this software.

# Create app.R code template
appDotRtpl <- templates::tmpl(~`renv::load("{{ renvPath }}")
library(rewardb)
launchDashboard("config.yml", "{{ globalConfigPath }}")`)

#' @title
#' Deploy shiny application
#'
#' @description
#' Deploy shiny dashboards to shiny server via ssh
#'
#' @export
deployShinyApp <- function(appConfigPath,
                           user,
                           globalConfigPath = "~/reward/config/global-cfg.yml",
                           shinyServer = "sharedshiny-prod.jnj.com",
                           port = 22,
                           deployPath = "~/ShinyApps/reward/",
                           renvPath = "~/reward/",
                           keyfile = NULL) {
  
  appCfg <- yaml::read_yaml(appConfigPath)
  deployDir <- file.path(deployPath, appCfg$short_name, fsep = "/")
  # Files to upload
  fp <- tempfile()
  dir.create(fp)

  appR <- templates::tmplUpdate(appDotRtpl, renvPath = renvPath, globalConfigPath = globalConfigPath)
  outFile <- file(file.path(fp, "app.R"))
  writeLines(appR, outFile)
  close(outFile)
  file.copy(appConfigPath, file.path(fp, "config.yml"))

  connString <- paste0(user, "@", shinyServer, ":", port)
  # Start ssh session
  session <- ssh::ssh_connect(connString, keyfile = keyfile)
  files <- Sys.glob(file.path(fp, "*"))
  tryCatch({
    # create directory and sub dirs, if any do not exist
    ssh::ssh_exec_wait(session, command = paste("mkdir -p", deployDir))
    ssh::scp_upload(session, files, to = deployDir, verbose = TRUE)
    },
    error = ParallelLogger::logError
  )
  unlink(fp)
  # Close ssh session
  ssh::ssh_disconnect(session)
}

#' @title
#' Update shiny server
#'
#' @description
#' Updates code in the remote ren package
#'
#' @export
updateShinyServer <- function(user, shinyServer = "sharedshiny-prod.jnj.com", port = 22, renvPath = "~/reward/", keyfile = NULL) {
  connString <- paste0(user, "@", shinyServer, ":", port)
  session <- ssh::ssh_connect(connString, keyfile = keyfile)

  tryCatch({
    # create directory and sub dirs, if any do not exist
    ssh::ssh_exec_(session, command = paste("cd", renvPath, "; git pull"))
    ssh::ssh_exec_wait(session, command = paste("cd", renvPath, "; R -e 'renv::load(); devtools::install(upgrade = TRUE)' "))
    },
    error = ParallelLogger::logError
  )
  # Close ssh session
  ssh::ssh_disconnect(session)
}
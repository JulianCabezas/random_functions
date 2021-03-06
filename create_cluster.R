# Function to create a cluster for paralelizing in Windows machines
# It creates a cluster with n nodes copyng THE COMPLETE environment, so it uses A LOT of RAM
# You can use this function as input for the parallel package functions (e.g parLapply)
# Use with precaution, remember to destroy the cluster after using it
# Based on parallelsugar package https://github.com/nathanvan/parallelsugar

create_cluster <- function(nnodes, libroute = NULL) {

  # nnodes: Number of nodes
  # libroute: Provided library route (different of the default library)

  tryCatch({
    cl <- parallel::makeCluster(nnodes) # Create an empty cluster with n nodes

    loaded.package.names <- c( # Find the loaded packages in the session
      sessionInfo()$basePkgs, # base packages
      names(sessionInfo()$otherPkgs)
    ) # aditional loaded packages

    parallel::clusterExport(cl, # Send the packages and the complete environment to the cluster
      "loaded.package.names", # loaded packages
      envir = environment()
    ) # "Environment"

    ## Charge the packages in all nodes
    if (is.null(libroute)) { # Use default library location
      parallel::parLapply(cl, 1:length(cl), function(xx) {
        lapply(loaded.package.names, function(yy) {
          library(yy, character.only = TRUE)
        })
      })
    } else {
      parallel::parLapply(cl, 1:length(cl), function(xx) { # If we provide a library location use it (useful when working in remote machines)
        lapply(loaded.package.names, function(yy) {
          library(yy, character.only = TRUE, lib.loc = libroute)
        })
      })
    }
  })
  return(cl) # The result is the cluster
}

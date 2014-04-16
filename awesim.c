/*
 * Copyright (C) 2014 University of Chicago.
 * See COPYRIGHT notice in top-level directory.
 *
 */

#include <string.h>
#include <assert.h>

#include "util.h"
#include "lp_awe_server.h"
#include "lp_awe_client.h"

#include "ross.h"
#include "codes/lp-io.h"
#include "codes/codes.h"
#include "codes/codes_mapping.h"
#include "codes/configuration.h"
#include "codes/model-net.h"
#include "codes/lp-type-lookup.h"


/* arguments to be handled by ROSS - strings passed in are expected to be
 * pre-allocated */
static char conf_file_name[256]={0};
char worktrace_file_name[256]={0};
char jobtrace_file_name[256]={0};
/* this struct contains default parameters used by ROSS, as well as
 * user-specific arguments to be handled by the ROSS config sys. Pass it in
 * prior to calling tw_init */
const tw_optdef app_opt [] =
{
	TWOPT_GROUP("Required config or workload file" ),
        TWOPT_CHAR("codes-config", conf_file_name, "name of codes configuration file"),
        TWOPT_CHAR("worktrace", worktrace_file_name, "workload trace of workunit"),
        TWOPT_CHAR("jobtrace", jobtrace_file_name, "job trace"),
	{TWOPT_END()}
};

int main(
    int argc,
    char **argv)
{
    int nprocs;
    int rank;
    
    /*printf("I am here: line number %d in file %s\n", __LINE__, __FILE__);*/
    
    /* TODO: explain why we need this (ROSS has cutoff??) */
    g_tw_ts_end = s_to_ns(60*60*24*365); /* one year, in nsecs */

    /* ROSS initialization function calls */
    tw_opt_add(app_opt); /* add user-defined args */
    /* initialize ROSS and parse args. NOTE: tw_init calls MPI_Init */
    tw_init(&argc, &argv); 

    if (!conf_file_name[0]) 
    {
        fprintf(stderr, "Expected \"codes-config\" option, please see --help.\n");
        MPI_Finalize();
        return 1;
    }
    
    if (!worktrace_file_name[0]) 
    {
        fprintf(stderr, "Expected \"worktrace\" option, please see --help.\n");
        MPI_Finalize();
        return 1;
    }

    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &nprocs);
    
    /* loading the config file into the codes-mapping utility, giving us the
     * parsed config object in return. 
     * "config" is a global var defined by codes-mapping */
    if (configuration_load(conf_file_name, MPI_COMM_WORLD, &config)){
        fprintf(stderr, "Error loading config file %s.\n", conf_file_name);
        MPI_Finalize();
        return 1;
    }
    
    /* Setup the model-net parameters specified in the global config object,
     * returned is the identifier for the network type */
    /*net_id = model_net_set_params();*/
    /* in this example, we are using simplenet, which simulates point to point 
     * communication between any two entities (other networks are trickier to
     * setup). Hence: */
        
    /* register the server LP type with codes-base 
     * (model-net LP type is registered internally in model_net_set_params() */
    register_lp_awe_server();
    register_lp_awe_client();
        
    /* Setup takes the global config object, the registered LPs, and 
     * generates/places the LPs as specified in the configuration file. 
     * This should only be called after ALL LP types have been registered in 
     * codes */
    
    codes_mapping_setup();
       
    
    init_awe_server();
        
    /* begin simulation */ 
    tw_run();
    
    /* model-net has the capability of outputting network transmission stats */
    /*model_net_report_stats(net_id);*/

    tw_end();
    
    return 0;
}


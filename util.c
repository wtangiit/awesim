#include <stdlib.h>	/* exit, malloc, realloc, free */
#include <stdio.h>	/* fopen, fgetc, fputs, fwrite */
#include <string.h>

#include "util.h"

int net_id = 0;

static Workunit* parse_workunit_by_trace(gchar * line);
static Job* parse_job_by_trace(gchar *line);

/*describing workflow task dependency, hardcoded for now, change later*/
static int task_dep_mgrast[10][10] = {
    {0,0,0,0,0,0,0,0,0,0},
    {1,0,0,0,0,0,0,0,0,0},
    {0,1,0,0,0,0,0,0,0,0},
    {0,0,1,0,0,0,0,0,0,0},
    {0,0,0,1,0,0,0,0,0,0},
    {0,0,0,0,1,0,0,0,0,0},
    {1,0,0,0,0,0,0,0,0,0},
    {0,0,0,0,0,0,1,0,0,0},
    {0,0,0,0,0,0,0,1,0,0},
    {0,0,0,0,0,1,0,0,1,0},
};
const char* ready_string = "0000000000";

/*serve as MAX epoch time, Sat, 20 Nov 2286 17:46:39 GMT*/
double kickoff_epoch_time = 9999999999; 
double finish_epoch_time = 0;
tw_stime finish_stime = 0;

/* convert ns to seconds */
tw_stime ns_to_s(tw_stime ns)
{
    return(ns / (1000.0 * 1000.0 * 1000.0));
}

/* convert seconds to ns */
tw_stime s_to_ns(tw_stime ns)
{
    return(ns * (1000.0 * 1000.0 * 1000.0));
}

/*convert eporch time to simulation time*/
tw_stime etime_to_stime(double etime) {
    return etime - kickoff_epoch_time;
}


static void free_key(gpointer data) {
	//not used for now (key is part of the data thus can be freed with the data)
}

static void free_value(gpointer data) {
	free(data);
}

void print_workunit(Workunit* work) {
    printf("workid=%s;cmd=%s;queued=%f;runtime=%f;\n", 
        work->id, 
        work->cmd, 
        work->stats.st_created,
        work->stats.runtime
    );
}

void print_job(Job* job) {
    printf("jobid=%s;num_tasks=%d;queued=%f;state=%s;task_splits=", 
        job->id, 
        job->num_tasks, 
        job->stats.created,
        job->state
    );
    for (int i=0;i<job->num_tasks;i++) {
         printf("%d,", job->task_splits[i]); 
    }
    printf("\n");
}

void print_key_value(gpointer key, gpointer value, gpointer user_data)
{
    if (strcmp((char*)user_data, "work_map")==0) {
        Workunit* work = (Workunit*)value;
        print_workunit(work);
    }
    if (strcmp((char*)user_data, "job_map")==0) {
        Job* job = (Job*)value;
        print_job(job);
    }
}

void display_hash_table(GHashTable *table, char* name)
{
    printf("displaying hash table:\n");
    g_hash_table_foreach(table, print_key_value, name);
}

GHashTable* parse_worktrace(char* workload_path) {
    FILE *f;
    char line[MAX_LEN_TRACE_LINE];
    f = fopen(workload_path, "r");
    if (f == NULL) {
 	perror(workload_path);
	exit(1);
    }
    
    GHashTable *work_map = NULL;
    work_map =  g_hash_table_new_full(g_str_hash, g_str_equal, free_key, free_value);
    printf("[awe_server]parsing work trace ...\n");
    
    while ( fgets ( line, sizeof(line), f ) != NULL ){ /* read a line */
        Workunit* work=NULL;   
        work = parse_workunit_by_trace((gchar*)line);
        memset(line, 0, sizeof(line));
        
        g_hash_table_insert(work_map, work->id, work);
    }
    
    printf("[awe_server]parsing work trace ... done: %u workunit parsed\n", g_hash_table_size(work_map));
    
    finish_stime = etime_to_stime(finish_epoch_time);
    
    return work_map;
}

static void increment_task_splits(GHashTable *job_map, char* work_id) {
    gchar ** parts = g_strsplit(work_id, "_", 3);
    char* job_id = parts[0];
    int task_id = atoi(parts[1]);
       
    Job* job = g_hash_table_lookup(job_map, job_id);
    job->task_splits[task_id] += 1;
    job->task_remainwork[task_id] += 1;
}

Workunit* parse_workunit_by_trace(gchar* line) {
    Workunit* work=NULL;
    work = malloc(sizeof(Workunit));
    memset(work, 0, sizeof(Workunit));
    gchar ** parts = NULL;
    g_strstrip(line);
    parts = g_strsplit(line, ";", 30);
    int i;
    for (i = 0; i < 30; i++) {
        if (!parts[i])
            break;
        gchar **pair = g_strsplit(parts[i], "=", 2);
       /* printf("key=%s, val=%s\n", pair[0], pair[1]);*/
        char* key = pair[0];
        char* val = pair[1];
        char *endptr;
        if (strcmp(key, "workid")==0) {
            strcpy(work->id, val);
        } else if (strcmp(key, "cmd")==0) {
            strcpy(work->cmd, val);
        } else if (strcmp(key, "runtime")==0) {
            work->stats.runtime = atoi(val);
        } else if (strcmp(key, "size_infile")==0) {
            work->stats.size_infile = strtoll(val, &endptr, 10);
        } else if (strcmp(key, "size_outfile")==0) {
            work->stats.size_outfile = strtoll(val, &endptr, 10);
        } else if (strcmp(key, "time_data_in")==0) {
            work->stats.time_data_in =atof(val);
        } else if (strcmp(key, "time_data_out")==0){
            work->stats.time_data_out = atof(val);
        }
    }
    increment_task_splits(job_map, work->id);
    return work;
}

GHashTable* parse_jobtrace(char* jobtrace_path) {
    FILE *f;
    char line[MAX_LEN_TRACE_LINE];
    f = fopen(jobtrace_path, "r");
    if (f == NULL) {
 	perror(jobtrace_path);
	exit(1);
    }
    
    GHashTable *job_map = NULL;
    job_map =  g_hash_table_new_full(g_str_hash, g_str_equal, free_key, free_value);
    printf("[awe_server]parsing job trace ...\n");
    
    while ( fgets ( line, sizeof(line), f ) != NULL ){ /* read a line */
        Job* jb=NULL;   
        jb = parse_job_by_trace((gchar*)line);
        memset(line, 0, sizeof(line));
        
        if (jb->stats.created < kickoff_epoch_time) {
            kickoff_epoch_time = jb->stats.created;
        }
        
        if (jb->stats.created > finish_epoch_time) {
            finish_epoch_time = jb->stats.created;
        }
        g_hash_table_insert(job_map, jb->id, jb);
    }
    
    printf("[awe_server]parsing job trace ... done: %u jobs parsed\n", g_hash_table_size(job_map));
    
    finish_stime = etime_to_stime(finish_epoch_time);
    
    return job_map;
}

Job* parse_job_by_trace(gchar* line) {
    Job* jb=NULL;
    jb = malloc(sizeof(Job));
    memset(jb, 0, sizeof(Job));
    gchar ** parts = NULL;
    g_strstrip(line);
    parts = g_strsplit(line, ";", 30);
    int i;
    for (i = 0; i < 30; i++) {
        if (!parts[i])
            break;
        gchar **pair = g_strsplit(parts[i], "=", 2);
        char* key = pair[0];
        char* val = pair[1];
        if (strcmp(key, "jobid")==0) {
            strcpy(jb->id, val);
        } else if (strcmp(key, "queued")==0) {
            jb->stats.created = atoi(val);
        } else if (strcmp(key, "num_tasks")==0) {
            jb->num_tasks = atoi(val);
        }
    }
    if (jb->num_tasks==0) {
        jb->num_tasks=10;
    }
    jb->remain_tasks = jb->num_tasks;
    strcpy(jb->state, "raw");
    for (int i=0; i<jb->num_tasks; i++) {
        for (int j=0; j<jb->num_tasks; j++) {
            jb->task_dep[i][j] = task_dep_mgrast[i][j];            
        }
    }
    return jb;
}


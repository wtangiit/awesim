#include <stdlib.h>	/* exit, malloc, realloc, free */
#include <stdio.h>	/* fopen, fgetc, fputs, fwrite */
#include <string.h>

#include "util.h"

static Workunit* parse_workunit_by_trace(gchar * line);

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
    printf("We free key: %s \n", data);
    free(data);
}

static void free_value(gpointer data) {
    printf("We free value: %s \n", data);
    free(data);
}

void print_workunit(Workunit* work) {
    printf("workid=%s;cmd=%s;queued=%f;runtime=%f;\n", 
        work->id, 
        work->cmd, 
        work->stats.created,
        work->stats.runtime
    );
}

void print_key_value(gpointer key, gpointer value, gpointer user_data)
{
    if (strcmp((char*)user_data, "work_map")==0) {
        Workunit* work = (Workunit*)value;
        print_workunit(work);
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
        
        if (work->stats.created < kickoff_epoch_time) {
            kickoff_epoch_time = work->stats.created;
        }
        
        if (work->stats.created > finish_epoch_time) {
            finish_epoch_time = work->stats.created;
        }

        g_hash_table_insert(work_map, work->id, work);
    }
    
    printf("[awe_server]parsing work trace ... done: %u workunit parsed\n", g_hash_table_size(work_map));
    
    finish_stime = etime_to_stime(finish_epoch_time);
    
    return work_map;
}

Workunit* parse_workunit_by_trace(gchar* line) {
    Workunit* work=NULL;
    work = malloc(sizeof(Workunit));
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
        if (strcmp(key, "workid")==0) {
            strcpy(work->id, val);
        } else if (strcmp(key, "cmd")==0) {
            strcpy(work->cmd, val);
        } else if (strcmp(key, "queued")==0) {
            work->stats.created = atoi(val);
        } else if (strcmp(key, "runtime")==0) {
            work->stats.runtime = atoi(val);
        }
    }
    return work;
}


/* 
 * File:   util.h
 * Author: wtang
 *
 * Created on April 2, 2014, 10:17 AM
 */

#ifndef UTIL_H
#define	UTIL_H

#include "ross.h"
#include "glib.h"
#include "awe_types.h"

#define MAX_LEN_TRACE_LINE 2048

#define Mega 1048576

typedef int bool;
#define True 1
#define False 0

#define now_sec(lp)  ns_to_s(tw_now(lp))

extern int net_id;

extern double kickoff_epoch_time;

extern char worktrace_file_name[256];
extern char jobtrace_file_name[256];

extern const char* ready_string;

extern FILE *event_log;

tw_stime etime_to_stime(double etime);

tw_stime ns_to_s(tw_stime ns);
tw_stime s_to_ns(tw_stime ns);

#define ns_tw_lookahead 1000000   /* 0.001s */

int testMap();

GHashTable* parse_worktrace(char* workload_path);
GHashTable* parse_jobtrace(char* jobtrace_path);
void display_hash_table(GHashTable *table, char* name);
void print_workunit(Workunit* work);

#endif	/* UTIL_H */


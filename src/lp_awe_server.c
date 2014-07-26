#include <string.h>
#include <assert.h>
#include "ross.h"
#include "glib.h"

#include "lp_awe_server.h"
#include "util.h"
#include "awe_types.h"

#include "codes/codes.h"
#include "codes/codes_mapping.h"
#include "codes/configuration.h"
#include "codes/lp-type-lookup.h"

GHashTable *work_map=NULL;
GHashTable *job_map=NULL;

static GQueue* work_queue;
static GQueue* client_req_queue;

/* define state*/
typedef struct awe_server_state awe_server_state;
/* this struct serves as the ***persistent*** state of the LP representing the 
 * server in question. This struct is setup when the LP initialization function
 * ptr is called */
struct awe_server_state {
    int total_job;
    int total_task;
    int total_work;
    tw_stime start_ts;    /* time that we started sending requests */
    tw_stime end_ts;      /* time that last request finished */
};


/* ROSS expects four functions per LP:
 * - an LP initialization function, called for each LP
 * - an event processing function
 * - a *reverse* event processing function (rollback), and
 * - a finalization/cleanup function when the simulation ends
 */
static void lpf_awe_server_init(awe_server_state * ns, tw_lp * lp);
static void lpf_awe_server_event(awe_server_state * ns, tw_bf * b, awe_msg * m, tw_lp * lp);
static void lpf_awe_server_rev_event(awe_server_state * ns, tw_bf * b, awe_msg * m, tw_lp * lp);
static void lpf_awe_server_finalize(awe_server_state * ns, tw_lp * lp);

/*event handlers*/
static void handle_kick_off_event(awe_server_state * ns, tw_bf * b, awe_msg * m, tw_lp * lp);
static void handle_job_submit_event(awe_server_state * ns, tw_bf * b, awe_msg * m, tw_lp * lp);
static void handle_work_checkout_event(awe_server_state * ns, tw_bf * b, awe_msg * m, tw_lp * lp);
static void handle_work_enqueue_event(awe_server_state * ns, tw_bf * b, awe_msg * m, tw_lp * lp);
static void handle_work_done_event(awe_server_state * ns, tw_bf * b, awe_msg * m, tw_lp * lp);

/*event planner*/
static void plan_work_enqueue_event(char* work_id, tw_lp *lp) ;

/*awe-server specific functions*/
static void parse_ready_tasks(Job* job, tw_lp * lp);
static char* get_first_work_by_stage(int stage);
static int client_match_work(tw_lpid clientid, char* workid);
static int get_group_id(tw_lpid client_id);

/* set up the function pointers for ROSS, as well as the size of the LP state
 * structure (NOTE: ROSS is in charge of event and state (de-)allocation) */
tw_lptype awe_server_lp = {
     (init_f) lpf_awe_server_init,
     (event_f) lpf_awe_server_event,
     (revent_f) lpf_awe_server_rev_event,
     (final_f) lpf_awe_server_finalize, 
     (map_f) codes_mapping,
     sizeof(awe_server_state),
};

/*start implementations*/

const tw_lptype* awe_server_get_lp_type()
{
    return(&awe_server_lp);
}

static void delete_job_entry(gpointer key, gpointer user_data) {
	g_hash_table_remove(job_map, (char*)key);
	printf("[awe-server]remove job %s from job_map\n", (char*)key);
}

static int jobmap_cleaning() {
    GHashTableIter iter;
    gpointer key, value;
    g_hash_table_iter_init(&iter, job_map);
    GSList* invalid_job_list=NULL;
    int ct = 0;
    while (g_hash_table_iter_next(&iter, &key, &value)) {
        Job* job = (Job*)value;
        if (job->num_tasks == 0) {
        	 invalid_job_list = g_slist_append(invalid_job_list, job->id);
        	 ct ++;
             continue;    
        }
        /* clean jobs with any task with split==0, meaning no workunit data available from the trace*/
        for (int i=0;i<job->num_tasks;i++) {
            if (job->task_splits[i] == 0) {
                invalid_job_list = g_slist_append(invalid_job_list, job->id);
                ct ++;
                continue;
            }
        }
        if (job->stats.created < kickoff_epoch_time) {
            kickoff_epoch_time = job->stats.created;
        }
    }
    g_slist_foreach(invalid_job_list, delete_job_entry, &ct);
    g_slist_free(invalid_job_list);
    return ct;
}

void init_awe_server() {
    /*parse workload file and init job_map and work_map, make sure parse job_map first*/
    job_map = parse_jobtrace(jobtrace_file_name);
    work_map = parse_worktrace(worktrace_file_name);
    int ct = jobmap_cleaning(); 
    printf("[awe_server]checking jobs...done, %d invalid jobs removed\n", ct);
    //display_hash_table(job_map, "job_map");
    printf("[awe_server]total valid jobs: %d\n", g_hash_table_size (job_map));
}

void register_lp_awe_server()
{
    /* lp_type_register should be called exactly once per process per 
     * LP type */
    lp_type_register("awe_server", awe_server_get_lp_type());
}

tw_lpid get_awe_server_lp_id() {
    tw_lpid rtn_id;
    codes_mapping_get_lp_id("AWE_SERVER", "awe_server", 0, 0, &rtn_id);
    return rtn_id;
}

void lpf_awe_server_init(
    awe_server_state * ns,
    tw_lp * lp)
{
    tw_event *e;
    awe_msg *m;
    tw_stime kickoff_time;

    memset(ns, 0, sizeof(*ns));
    work_queue = g_queue_new();
    client_req_queue = g_queue_new();
    
    /* skew each kickoff event slightly to help avoid event ties later on */
    kickoff_time = 0;

    /* first create the event (time arg is an offset, not absolute time) */
    e = codes_event_new(lp->gid, kickoff_time, lp);
    /* after event is created, grab the allocated message and set msg-specific
     * data */ 
    m = tw_event_data(e);
    m->event_type = KICK_OFF;
    m->src = lp->gid;
    /* event is ready to be processed, send it off */
    tw_event_send(e);

    return;
}

/* event processing entry point
 * - simply forward the message to the appropriate handler */
void lpf_awe_server_event(
    awe_server_state * ns,
    tw_bf * b,
    awe_msg * m,
    tw_lp * lp)
{
    switch (m->event_type)
    {
        case KICK_OFF:
            handle_kick_off_event(ns, b, m, lp);
            break;
        case JOB_SUBMIT:
            handle_job_submit_event(ns, b, m, lp);
            break;
        case WORK_DONE:
            handle_work_done_event(ns, b, m, lp);
            break;
        case WORK_ENQUEUE:
            handle_work_enqueue_event(ns, b, m, lp);
            break;
        case WORK_CHECKOUT:
            handle_work_checkout_event(ns, b, m, lp);
            break;
        default:
            printf("\nawe_server Invalid message type %d from %lu\n", m->event_type, m->src);
        break;
    }
}

/* reverse event processing entry point
 * - simply forward the message to the appropriate handler */
void lpf_awe_server_rev_event(
    awe_server_state * ns,
    tw_bf * b,
    awe_msg * m,
    tw_lp * lp)
{
    return;
}

/* once the simulation is over, do some output */
void lpf_awe_server_finalize(
    awe_server_state * ns,
    tw_lp * lp)
{
    ns->end_ts = tw_now(lp);
    printf("[awe_server][%lu]start_time=%lf;end_time=%lf, makespan=%lf, total_job=%d, total_task=%d, total_workunit=%d\n",
        lp->gid,
        ns->start_ts, 
        ns->end_ts,
        ns->end_ts - ns->start_ts,
        ns->total_job,
        ns->total_task,
        ns->total_work);
    return;
}

/* handle initial event (initialize job submission) */
void handle_kick_off_event(
    awe_server_state * ns,
    tw_bf * b,
    awe_msg * m,
    tw_lp * lp)
{
    printf("%lf;awe_server;%lu]Start serving\n", now_sec(lp), lp->gid);
    GHashTableIter iter;
    gpointer key, value;
    g_hash_table_iter_init(&iter, job_map);
    
    while (g_hash_table_iter_next(&iter, &key, &value)) {
        Job* job = (Job*)value;
        tw_event *e;
        awe_msg *msg;
        tw_stime submit_time;
        submit_time =  s_to_ns(etime_to_stime(job->stats.created)) + ns_tw_lookahead;
        if (fraction < 1.0) {
        	submit_time = submit_time * fraction;
        }
        e = codes_event_new(lp->gid, submit_time, lp);
        msg = tw_event_data(e);
        msg->event_type = JOB_SUBMIT;
        strcpy(msg->object_id, job->id);
        tw_event_send(e);
    }
    return;
}

void handle_job_submit_event(
    awe_server_state * ns,
    tw_bf * b,
    awe_msg * m,
    tw_lp * lp)
{
    char* job_id = m->object_id;
    Job* job = g_hash_table_lookup(job_map, job_id);
    assert(job);
    fprintf(event_log, "%lf;awe_server;%lu;JQ;jobid=%s inputsize=%llu\n", now_sec(lp), lp->gid, job_id, job->inputsize);
    parse_ready_tasks(job, lp);
    return;
}

void handle_work_enqueue_event(
    awe_server_state * ns,
    tw_bf * b,
    awe_msg * m,
    tw_lp * lp)
{
    fprintf(event_log, "%lf;awe_server;%lu;WQ;work=%s\n", now_sec(lp), lp->gid, m->object_id);
    
    char* workid=NULL;
    workid = malloc(sizeof(char[MAX_LENGTH_ID]));
    strcpy(workid, m->object_id);
    assert(workid);
    tw_lpid *clientid;
    int has_match = 0;

    int len = g_queue_get_length(client_req_queue);
    if (len > 0) {
    	int n = -1;
    	for (int i=0; i<len; i++) {
    		clientid = g_queue_peek_nth(client_req_queue, i);
    		if (client_match_work(*clientid, workid)) {
    			 n = i;
    			 break;
    		}
    	}
    	if (n >=0) {
    		clientid = g_queue_pop_nth(client_req_queue, n);
    		has_match = 1;
    	}
    }
    
    if (has_match) {
        tw_event *e;
        awe_msg *msg;
        e = codes_event_new(*clientid, ns_tw_lookahead, lp);
        msg = tw_event_data(e);
        msg->event_type = WORK_CHECKOUT;
        strcpy(msg->object_id, workid);
        tw_event_send(e);
        fprintf(event_log, "%lf;awe_server;%lu;WC;work=%s client=%lu\n", now_sec(lp), lp->gid, workid, *clientid);
        free(workid);
        free(clientid);
    } else {
    	g_queue_push_tail(work_queue, workid);
    }
    return;
}

void handle_work_checkout_event(
    awe_server_state * ns,
    tw_bf * b,
    awe_msg * m,
    tw_lp * lp)
{
    tw_event *e;
    awe_msg *msg;


    e = codes_event_new(m->src, ns_tw_lookahead, lp);
    msg = tw_event_data(e);
    msg->event_type = WORK_CHECKOUT;
    memset(msg->object_id, 0, sizeof(msg->object_id));
    
    tw_lpid client_id = m->src;
//    char group_name[MAX_LENGTH_GROUP];
    //char lp_type_name[MAX_LENGTH_GROUP];
    //int  lp_type_id, grp_id, grp_rep_id, offset;
 //   codes_mapping_get_lp_info(client_id, group_name, &grp_id, &lp_type_id,
   //         lp_type_name, &grp_rep_id, &offset);

    int group_id = 0;
    group_id = get_group_id(client_id);

    /*if queue is empty, msg->object_id is "", otherwise msg->object-id is the dequeued workid*/
    int got_work = 0;
    char workid[MAX_LENGTH_ID];
    if (!g_queue_is_empty(work_queue)) {
        if (group_id == 1) {  //client from remote site
            char* work = get_first_work_by_stage(5); //checkout task 5 (blat) only for remote site
            if (work) {
            	strcpy(workid, work);
            	got_work = 1;
            }
        } else {
        	strcpy(workid, g_queue_pop_head(work_queue));
        	got_work = 1;
        }
    }

    if (got_work) { //eligible work found, send back to the requesting client
        fprintf(event_log, "%lf;awe_server;%lu;WC;work=%s client=%lu\n", now_sec(lp), lp->gid, workid, m->src);
        assert (strlen(workid) > 10);
        strcpy(msg->object_id, workid);
        tw_event_send(e);
    } else {  //no eligible work found, put client request to the waiting queue
        tw_lpid *clientid = NULL;
        clientid = malloc(sizeof(tw_lpid));
        *clientid = m->src;
        g_queue_push_tail(client_req_queue, clientid);
    }
    return;
}

void handle_work_done_event(awe_server_state * ns,
        tw_bf * b,
        awe_msg * m,
        tw_lp * lp) 
{
    char *work_id = m->object_id;
    
    gchar ** parts = g_strsplit(work_id, "_", 3);
    char* job_id = parts[0];
    int task_id = atoi(parts[1]);
    Job* job = g_hash_table_lookup(job_map, job_id);
    job->task_remainwork[task_id] -= 1;
    fprintf(event_log, "%lf;awe_server;%lu;WD;workid=%s\n", now_sec(lp), lp->gid, work_id);
    ns->total_work += 1;
    /*handle task done*/
    if (job->task_remainwork[task_id] == 0) { 
    	 fprintf(event_log, "%lf;awe_server;%lu;TD;taskid=%s_%d\n", now_sec(lp), lp->gid, job_id, task_id);
         ns->total_task +=1;
         job->task_states[task_id]=2;
         for (int j=0; j<job->num_tasks; j++) {
             job->task_dep[j][task_id] = 0;
         }
         parse_ready_tasks(job, lp);
         job->remain_tasks -= 1;
         /*handle job done*/
        if (job->remain_tasks==0) {
             fprintf(event_log, "%lf;awe_server;%lu;JD;jobid=%s\n", now_sec(lp), lp->gid, job_id);
             ns->total_job += 1;
        }
    }
}

void parse_ready_tasks(Job* job, tw_lp * lp) {
    for (int i=0; i<job->num_tasks; i++) {
        bool is_ready = True;
        for (int j=0; j<job->num_tasks; j++) {
            if (job->task_dep[i][j] == 1) {
                is_ready = False;
                break;
            }
        }
        if (is_ready && job->task_states[i]==0) {
            fprintf(event_log, "%lf;awe_server;%lu;TQ;taskid=%s_%d splits=%d\n", now_sec(lp), lp->gid, job->id, i, job->task_splits[i]);
            job->task_states[i]=1;
            if (job->task_splits[i] == 1) {
                char work_id[MAX_LENGTH_ID];
                sprintf(work_id, "%s_%d_0", job->id, i);
                plan_work_enqueue_event(work_id, lp);
            } else if (job->task_splits[i] > 1) {
                for (int j=1; j<=job->task_splits[i]; j++) {
                    char work_id[MAX_LENGTH_ID];
                    sprintf(work_id, "%s_%d_%d", job->id, i, j);
                    plan_work_enqueue_event(work_id, lp);
                }
            }
        }
    }
}

void plan_work_enqueue_event(char* work_id, tw_lp *lp) {
    tw_event *e;
    awe_msg *msg;
    e = codes_event_new(lp->gid, ns_tw_lookahead, lp);
    msg = tw_event_data(e);
    msg->event_type = WORK_ENQUEUE;
    strcpy(msg->object_id, work_id);
    tw_event_send(e);
}

char* get_first_work_by_stage(int stage) {
	char *workid = NULL;
	int len = g_queue_get_length(work_queue);
    int n = -1;
	for (int i=0; i<len; i++) {
	    workid = g_queue_peek_nth(work_queue, i);
        gchar **seg = g_strsplit(workid, "_", 3);
        int taskid = atoi(seg[1]);
        if (taskid == stage) {
        	n = i;
        	break;
        }
	}
	if (n >= 0) {
		return g_queue_pop_nth(work_queue, n);
	}
	return NULL;
}

int client_match_work(tw_lpid client_id, char* workid) {
    int match = 1;
    //char group_name[MAX_LENGTH_GROUP];
    //char lp_type_name[MAX_LENGTH_GROUP];
    //codes_mapping_get_lp_info(clientid, group_name, grp_id, lp_type_id, lp_type_name, grp_rep_id, offset);

    int group_id = 0;
    group_id = get_group_id(client_id);
    if (group_id == 1) {  //remote client
    	gchar **seg = g_strsplit(workid, "_", 3);
    	int taskid = atoi(seg[1]);
    	if (taskid != 5) {
    		match = 0;
    	}
    }
    return match;
}

int get_group_id(tw_lpid client_id) {
    if (client_id < 55) {  //TO-DO, make the pivot configurable, or use code_mapping_get_lp_info directly in the future
    	return 0;
    } else {
        return 1;
    }
}


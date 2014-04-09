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

static GQueue* work_queue;
static GQueue* client_req_queue;

/* define state*/
typedef struct awe_server_state awe_server_state;
/* this struct serves as the ***persistent*** state of the LP representing the 
 * server in question. This struct is setup when the LP initialization function
 * ptr is called */
struct awe_server_state {
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

void init_awe_server() {
    /*parse workload file and init work_map*/
    work_map = parse_worktrace(worktrace_file_name);
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
    awe_server_state * qs,
    tw_lp * lp)
{
    tw_event *e;
    awe_msg *m;
    tw_stime kickoff_time;
    
    memset(qs, 0, sizeof(*qs));
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
    /* event is ready to be processed, send it off */
    tw_event_send(e);

    return;
}

/* event processing entry point
 * - simply forward the message to the appropriate handler */
void lpf_awe_server_event(
    awe_server_state * qs,
    tw_bf * b,
    awe_msg * m,
    tw_lp * lp)
{
   switch (m->event_type)
    {
        case KICK_OFF:
            handle_kick_off_event(qs, b, m, lp);
            break;
        case JOB_SUBMIT:
            handle_job_submit_event(qs, b, m, lp);
            break;
        case WORK_CHECKOUT:
            handle_work_checkout_event(qs, b, m, lp);
            break;
        default:
	    printf("\n Invalid message type %d ", m->event_type);
        break;
    }
}

/* reverse event processing entry point
 * - simply forward the message to the appropriate handler */
void lpf_awe_server_rev_event(
    awe_server_state * qs,
    tw_bf * b,
    awe_msg * m,
    tw_lp * lp)
{
    return;
}

/* once the simulation is over, do some output */
void lpf_awe_server_finalize(
    awe_server_state * qs,
    tw_lp * lp)
{
    qs->end_ts = tw_now(lp);
    printf("[server %lu]start_time=%lf;end_time=%lf, makespan=%lf\n",
        lp->gid,
        qs->start_ts, 
        qs->end_ts,
        qs->end_ts - qs->start_ts);
    return;
}

/* handle initial event (initialize job submission) */
void handle_kick_off_event(
    awe_server_state * qs,
    tw_bf * b,
    awe_msg * m,
    tw_lp * lp)
{
    GHashTableIter iter;
    gpointer key, value;
    g_hash_table_iter_init(&iter, work_map);
    
    while (g_hash_table_iter_next(&iter, &key, &value)) {
        // do something with key and value
        Workunit* work = (Workunit*)value;
        /*print_workunit(work);*/
        
        tw_event *e;
        awe_msg *msg;
        tw_stime submit_time;
        
        submit_time =  etime_to_stime(work->stats.created);
        e = codes_event_new(lp->gid, submit_time, lp);
        msg = tw_event_data(e);
        msg->event_type = JOB_SUBMIT;
        strcpy(msg->object_id, work->id);
        /* event is ready to be processed, send it off */
        tw_event_send(e);
    }
    return;
}

void handle_job_submit_event(
    awe_server_state * qs,
    tw_bf * b,
    awe_msg * m,
    tw_lp * lp)
{
    printf("[%lf][awe_server]WQ;work=%s\n", tw_now(lp), m->object_id);
    
    char* workid=NULL;
    workid = malloc(sizeof(char[MAX_LENGTH_ID]));
    strcpy(workid, m->object_id);
    assert(workid);
    g_queue_push_tail(work_queue, workid);
   
    if (!g_queue_is_empty(client_req_queue)) {
        tw_lpid *clientid = g_queue_pop_head(client_req_queue);
        char* workid = g_queue_pop_head(work_queue);
        if (workid) {
            tw_event *e;
            awe_msg *msg;
            e = codes_event_new(*clientid, g_tw_lookahead, lp);
            msg = tw_event_data(e);
            msg->event_type = WORK_CHECKOUT;
            strcpy(msg->object_id, workid);
            tw_event_send(e);
        }
    }
    return;
}


void handle_work_checkout_event(
    awe_server_state * qs,
    tw_bf * b,
    awe_msg * m,
    tw_lp * lp)
{
    tw_event *e;
    awe_msg *msg;
   
    e = codes_event_new(m->src, g_tw_lookahead, lp);
    msg = tw_event_data(e);
    msg->event_type = WORK_CHECKOUT;
    
    /*if queue is empty, msg->object_id is "", otherwise msg->object-id is the dequeued workid*/
    memset(msg->object_id, 0, sizeof(msg->object_id));
    if (!g_queue_is_empty(work_queue)) {
        char workid[MAX_LENGTH_ID];
        strcpy(workid, g_queue_pop_head(work_queue));
        printf("[%lf][awe_server]WC;work=%s;client=%lu\n", tw_now(lp), workid, m->src);
        assert (strlen(workid) > 10);
        strcpy(msg->object_id, workid);
        tw_event_send(e);
    } else{
        tw_lpid *clientid = NULL;
        clientid = malloc(sizeof(tw_lpid));
        *clientid = m->src;
        g_queue_push_tail(client_req_queue, clientid);
    }
    return;
}

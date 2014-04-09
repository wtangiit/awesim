#include "util.h"
#include "awe_types.h"
#include "lp_awe_server.h"

#include <string.h>
#include <assert.h>
#include <ross-types.h>
#include "ross.h"

#include "codes/codes.h"
#include "codes/codes_mapping.h"
#include "codes/configuration.h"
#include "codes/lp-type-lookup.h"


/* define state*/
typedef struct awe_client_state awe_client_state;
struct awe_client_state {
    char current_work[MAX_LENGTH_ID];
    int  total_processed;
    double busy_time;
    tw_stime start_ts;    /* time that we started sending requests */
    tw_stime end_ts;      /* time that last request finished */
};

/* ROSS expects four functions per LP:
 * - an LP initialization function, called for each LP
 * - an event processing function
 * - a *reverse* event processing function (rollback), and
 * - a finalization/cleanup function when the simulation ends
 */
static void lpf_awe_client_init(awe_client_state * ns, tw_lp * lp);
static void lpf_awe_client_event(awe_client_state * ns, tw_bf * b, awe_msg * m, tw_lp * lp);
static void lpf_awe_client_rev_event(awe_client_state * ns, tw_bf * b, awe_msg * m, tw_lp * lp);
static void lpf_awe_client_finalize(awe_client_state * ns, tw_lp * lp);

/*event handlers*/
static void handle_kick_off_event(awe_client_state * ns, tw_bf * b, awe_msg * m, tw_lp * lp);
static void handle_timer_request_event(awe_client_state * ns, tw_bf * b, awe_msg * m, tw_lp * lp);
static void handle_work_checkout_event(awe_client_state * ns, tw_bf * b, awe_msg * m, tw_lp * lp);
static void handle_work_done_event(awe_client_state * ns, tw_bf * b, awe_msg * m, tw_lp * lp);
static void send_work_checkout_request(tw_lp *lp);
static void schedule_futuer_event(tw_lp *lp, awe_event_type event_type, tw_stime interval, void* userdata);

/* set up the function pointers for ROSS, as well as the size of the LP state
 * structure (NOTE: ROSS is in charge of event and state (de-)allocation) */
tw_lptype awe_client_lp = {
     (init_f) lpf_awe_client_init,
     (event_f) lpf_awe_client_event,
     (revent_f) lpf_awe_client_rev_event,
     (final_f) lpf_awe_client_finalize, 
     (map_f) codes_mapping,
     sizeof(awe_client_state),
};

/*start implementations*/
const tw_lptype* awe_client_get_lp_type()
{
    return(&awe_client_lp);
}

void init_awe_client() {
    /*parse workload file and init work_map*/
    return;
}


void register_lp_awe_client()
{
    /* lp_type_register should be called exactly once per process per 
     * LP type */
    lp_type_register("awe_client", awe_client_get_lp_type());
}

void lpf_awe_client_init(
    awe_client_state * cs,
    tw_lp * lp)
{
    tw_event *e;
    awe_msg *m;
    tw_stime kickoff_time;
    
    memset(cs, 0, sizeof(*cs));
            
    /* skew each kickoff event slightly to help avoid event ties later on */
    kickoff_time = g_tw_lookahead + tw_rand_unif(lp->rng); ;

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
void lpf_awe_client_event(
    awe_client_state * cs,
    tw_bf * b,
    awe_msg * m,
    tw_lp * lp)
{
   switch (m->event_type)
    {
        case KICK_OFF:
            handle_kick_off_event(cs, b, m, lp);
            break;
        case TIMER_REQUEST:
            handle_timer_request_event(cs, b, m, lp);
            break;
        case WORK_CHECKOUT:
            handle_work_checkout_event(cs, b, m, lp);
            break;
        case WORK_DONE:
            handle_work_done_event(cs, b, m, lp);
            break;
        default:
	    printf("\n Invalid message type %d ", m->event_type);
        break;
    }
}

/* reverse event processing entry point
 * - simply forward the message to the appropriate handler */
void lpf_awe_client_rev_event(
    awe_client_state * cs,
    tw_bf * b,
    awe_msg * m,
    tw_lp * lp)
{
    return;
}

/* once the simulation is over, do some output */
void lpf_awe_client_finalize(
    awe_client_state * cs,
    tw_lp * lp)
{
    cs->end_ts = tw_now(lp);
    printf("[awe_client-gid=%lu,lid=%lu]:start_time=%lf, end_time=%lf, makespan=%lf, processed=%d, busy_time=%lf, busy_rate=%lf\n", 
            lp->gid,
            lp->id, 
            cs->start_ts, 
            cs->end_ts,
            cs->end_ts - cs->start_ts,
            cs->total_processed,
            cs->busy_time,
            cs->busy_time / (cs->end_ts - cs->start_ts) );
    
    return;
}

/* handle initial event (initialize job submission) */
void handle_kick_off_event(
    awe_client_state * cs,
    tw_bf * b,
    awe_msg * m,
    tw_lp * lp)
{
    schedule_futuer_event(lp, TIMER_REQUEST, g_tw_lookahead, NULL);
    return;
}

void handle_timer_request_event(awe_client_state * cs, tw_bf * b, awe_msg * m, tw_lp * lp) {
    if (tw_now(lp) > finish_stime) {
        return;
    }
    send_work_checkout_request(lp);
    /* printf("[%lf][awe_client-gid=%lu,lid=%lu]: send checkout request to server\n", tw_now(lp), lp->gid, lp->id); */
    return;
}

void handle_work_checkout_event(awe_client_state * cs, tw_bf * b, awe_msg * m, tw_lp * lp) {
    
    if (tw_now(lp) > finish_stime) {
        return;
    }
    if (strlen(m->object_id)>0) {
        char* workid = m->object_id;
        Workunit* work = g_hash_table_lookup(work_map, workid);
        printf("[%lf][awe_client %lu]WC;workid=%s;cmd=%s;runtime=%lf\n", tw_now(lp), lp->gid, m->object_id, work->cmd, work->stats.runtime);
        schedule_futuer_event(lp, WORK_DONE, work->stats.runtime, workid);
        finish_stime += work->stats.runtime;
    }
}

void handle_work_done_event(awe_client_state * cs, tw_bf * b, awe_msg * m, tw_lp * lp) {
    cs->total_processed += 1;
    Workunit* work = g_hash_table_lookup(work_map, m->object_id);
    cs->busy_time += work->stats.runtime;
    printf("[%lf][awe_client %lu]WD;workid=%s\n", tw_now(lp), lp->gid, m->object_id);
    schedule_futuer_event(lp, TIMER_REQUEST, g_tw_lookahead, NULL);
}

void send_work_checkout_request(tw_lp *lp) {
    tw_event *e;
    awe_msg *msg;
    tw_lpid server_id = get_awe_server_lp_id();
    e = codes_event_new(server_id, g_tw_lookahead, lp);
    msg = tw_event_data(e);
    msg->event_type = WORK_CHECKOUT;
    msg->src = lp->gid;
    char str[15];
    sprintf(str, "%lu", lp->gid);
    strcpy(msg->object_id, str);
    tw_event_send(e);
    return;
}

void schedule_futuer_event(tw_lp *lp, awe_event_type event_type, tw_stime interval, void* userdata) {
    tw_event *e;
    awe_msg *msg;
    e = codes_event_new(lp->gid, interval, lp);
    msg = tw_event_data(e);
    msg->event_type = event_type;
    if (userdata) {
        strcpy(msg->object_id, (char*)userdata);
    }
    /* event is ready to be processed, send it off */
    tw_event_send(e);
}

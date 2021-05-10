////////////////////////////////////////////////////////////////////////////////
// ActorPong
////////////////////////////////////////////////////////////////////////////////

#include "ponger.h"
#include "pinger.h"

static void ponger_on_start(ActorCell *actor, Msg *msg);
static bool ponger_on_receive(ActorCell *actor, Msg *msg);
static void ponger_on_stop(ActorCell *actor, Msg *msg);

ACTOR_IMPL(Ponger, { char ignored; }, ponger_on_start, ponger_on_receive, ponger_on_stop);

MSG_IMPL(Pong);

void ponger_on_start(ActorCell *actor, Msg *msg) { debug("Ponger started."); }

bool ponger_on_receive(ActorCell *actor, Msg *msg) {
  if (msg->type == &Ping) {
    const PingParams *ping = msg->payload;
    //debugf("Pong %d", ping->num);
    PongParams pong = {.num = ping->num};
    actors_send(actor, msg->from, &Pong, &pong);
  }

  return true;
}

void ponger_on_stop(ActorCell *actor, Msg *msg) { debug("Ponger stopped."); }

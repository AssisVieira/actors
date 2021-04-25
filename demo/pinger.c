////////////////////////////////////////////////////////////////////////////////
// ActorPing
////////////////////////////////////////////////////////////////////////////////

#include "pinger.h"

#include "ponger.h"

static void pinger_on_start(ActorCell *actor, Msg *msg);
static bool pinger_on_receive(ActorCell *actor, Msg *msg);
static void pinger_on_stop(ActorCell *actor, Msg *msg);

ACTOR_IMPL(
    Pinger,
    {
      int numPings;
      ActorCell *ponger;
    },
    pinger_on_start, pinger_on_receive, pinger_on_stop);

MSG_IMPL(Ping);

void pinger_on_start(ActorCell *actor, Msg *msg) {
  PingerState *state = actor->state;
  state->numPings = 0;
  // state->ponger = actors_child_new(actor, "Ponger", &Ponger, NULL);

  actors_send(actor, actor, &Pong, &(PongParams){.num = state->numPings});

  debug("Pinger started.");
}

bool pinger_on_receive(ActorCell *actor, Msg *msg) {
  PingerState *state = actor->state;
  PingerParams *params = actor->params;

  if (msg->type == &Pong) {
    PongParams *pong = msg->payload;

    if (params->debug) {
      debugf("Ping %d", pong->num);
    }

    if (state->numPings >= params->maxPings) {
      return false;
    }

    state->numPings++;

    actors_send(actor, actor, &Pong, &(PongParams){.num = state->numPings});
  }

  return true;
}

void pinger_on_stop(ActorCell *actor, Msg *msg) { debug("Pinger stopped."); }

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
      int currPonger;
      ActorCell **pongers;
    },
    pinger_on_start, pinger_on_receive, pinger_on_stop);

MSG_IMPL(Ping);

void pinger_on_start(ActorCell *actor, Msg *msg) {
  PingerState *state = actor->state;
  PingerParams *params = actor->params;
  state->numPings = 0;
  state->currPonger = 0;

  state->pongers = malloc(sizeof(ActorCell *) * params->numPongers);

  for (int i  = 0; i < params->numPongers; i++) {
    char name[32] = {0}; 
    snprintf(name, sizeof(name), "Ponger %d", i);
    state->pongers[i] = actors_child_new(actor, name, &Ponger, NULL);
    actors_send(actor, state->pongers[i], &Ping, &(PingParams){.num = state->numPings});
  }

  debug("Pinger started.");
}

bool pinger_on_receive(ActorCell *actor, Msg *msg) {
  PingerState *state = actor->state;
  PingerParams *params = actor->params;

  if (msg->type == &Pong) {
    //PongParams *pong = msg->payload;

    //debugf("Ping %d", pong->num);

    if (state->numPings >= params->maxPings) {
      return false;
    }

    state->numPings++;

    do {
      state->currPonger = (state->currPonger + 1) % params->numPongers;
    } while (state->pongers[state->currPonger] == NULL);

    actors_send(actor, state->pongers[state->currPonger], &Ping, &(PingParams){.num = state->numPings});
  } else if (msg->type == &Stopped) {
    for (int i  = 0; i < params->numPongers; i++) {
      if (state->pongers[i] == msg->from) {
        state->pongers[i] = NULL;
      }
    }
  }

  return true;
}

void pinger_on_stop(ActorCell *actor, Msg *msg) { 
  PingerState *state = actor->state;
  free(state->pongers);
  debug("Pinger stopped."); 
}

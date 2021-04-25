////////////////////////////////////////////////////////////////////////////////
// ActorSystem
////////////////////////////////////////////////////////////////////////////////

#include "actorsystem.h"

#include "actorcell.h"
#include "dispatcher.h"
#include "msg.h"

static void system_on_start(ActorCell *actor, Msg *msg);
static bool system_on_receive(ActorCell *actor, Msg *msg);
static void system_on_stop(ActorCell *actor, Msg *msg);

static void actors_sig_term_handler(int signum, siginfo_t *info, void *ptr);
static int actors_setup_signals();

static ActorSystem *ACTOR_SYSTEM = NULL;

ACTOR_IMPL(
    System, { char ignore; }, system_on_start, system_on_receive,
    system_on_stop);

void system_on_start(ActorCell *actor, Msg *msg) {
  debug("System Actor started.");
}

bool system_on_receive(ActorCell *actor, Msg *msg) {
  if (msg->type == &Stopped) {
    if (actors_num_children(actor) == 0) {
      debug("[System] Stopped all children!");
      return false;
    }
  }

  return true;
}

void system_on_stop(ActorCell *actor, Msg *msg) {
  pthread_mutex_lock(&ACTOR_SYSTEM->waitMutex);
  ACTOR_SYSTEM->stop = true;
  pthread_cond_signal(&ACTOR_SYSTEM->waitCond);
  debug("System stopped.");
  pthread_mutex_unlock(&ACTOR_SYSTEM->waitMutex);
}

static void actors_sig_term_handler(int signum, siginfo_t *info, void *ptr) {
  ACTOR_SYSTEM->stopChildren = true;
}

static int actors_setup_signals() {
  static struct sigaction sigact;

  memset(&sigact, 0, sizeof(sigact));
  sigact.sa_sigaction = actors_sig_term_handler;
  sigact.sa_flags = SA_SIGINFO;

  if (sigaction(SIGTERM, &sigact, NULL)) {
    return -1;
  }

  if (sigaction(SIGINT, &sigact, NULL)) {
    return -1;
  }

  return 0;
}

ActorCell *actors_child_new(ActorCell *parent, const char *name,
                            const Actor *actor, const void *params) {
  return actorcell_create(parent, name, actor, params,
                          ACTOR_SYSTEM->dispatcher);
}

ActorCell *actors_create(int cores) {
  debug("Starting actor system.");

  ActorSystem *actorSystem = malloc(sizeof(ActorSystem));
  actorSystem->dispatcher = dispatcher_create(cores);
  actorSystem->stop = false;
  actorSystem->stopChildren = false;
  actorSystem->stopChildrenDone = false;
  actorSystem->waitCond = (pthread_cond_t)PTHREAD_COND_INITIALIZER;
  actorSystem->waitMutex = (pthread_mutex_t)PTHREAD_MUTEX_INITIALIZER;

  ACTOR_SYSTEM = actorSystem;

  actors_setup_signals();

  return actors_child_new(NULL, "System", &System, &(SystemParams){0});
}

void actors_send(ActorCell *from, ActorCell *to, const MsgType *type,
                 const void *payload) {
  assert(to != NULL);
  actorcell_send(from, to, type, payload);
}

int actors_wait_children(ActorCell *system) {
  while (!ACTOR_SYSTEM->stop) {
    pthread_mutex_lock(&ACTOR_SYSTEM->waitMutex);

    struct timespec spec;
    clock_gettime(CLOCK_REALTIME, &spec);
    spec.tv_sec += 1;

    pthread_cond_timedwait(&ACTOR_SYSTEM->waitCond, &ACTOR_SYSTEM->waitMutex,
                           &spec);

    if (ACTOR_SYSTEM->stopChildren) {
      if (!ACTOR_SYSTEM->stopChildrenDone) {
        ActorCellList *node = system->children;
        while (node != NULL) {
          actors_send(system, node->actor, &Stop, NULL);
          node = node->next;
        }
        ACTOR_SYSTEM->stopChildrenDone = true;
      }
    }

    pthread_mutex_unlock(&ACTOR_SYSTEM->waitMutex);
  }

  actors_free(system);

  return 0;
}

int actors_num_children(ActorCell *actor) {
  return actorcell_num_children(actor);
}

void actors_free(ActorCell *system) {
  dispatcher_free(ACTOR_SYSTEM->dispatcher);
  free(ACTOR_SYSTEM);
  debug("Actor system stopped.");
}

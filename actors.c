////////////////////////////////////////////////////////////////////////////////
// Actor Model in C.
// Author: Assis Vieira <assis.sv@gmail.com>
// Based on Akka Actor Model: akka.io.
////////////////////////////////////////////////////////////////////////////////

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <time.h>
#include <stdint.h>
#include <unistd.h>
#include <signal.h>
#include <errno.h>

////////////////////////////////////////////////////////////////////////////////
// Debug
////////////////////////////////////////////////////////////////////////////////

static bool DEBUG_ENABLED = false;

#define debug(FMT, ...) \
if (DEBUG_ENABLED) {\
  char pthreadName[32] = {0};\
  pthread_getname_np(pthread_self(), pthreadName, sizeof(pthreadName));\
  printf("[%s] " FMT "\n", pthreadName, ##__VA_ARGS__);\
}

////////////////////////////////////////////////////////////////////////////////
// Msg
////////////////////////////////////////////////////////////////////////////////

typedef struct ActorCell ActorCell;

typedef struct MsgType {
  const char *name;
  size_t size;
} MsgType;

typedef struct Msg {
  const MsgType *type;
  void *payload;
  ActorCell *from;
} Msg;

Msg *msg_create(ActorCell *from, const MsgType *type, const void *payload) {
  Msg *msg = malloc(sizeof(Msg));
  msg->payload = malloc(type->size);
  msg->type = type;
  msg->from = from;

  memcpy(msg->payload, payload, type->size);

  return msg;
}

void msg_free(Msg *msg) {
  free(msg->payload);
  free(msg);
}

const MsgType Stop = { .name = "Stop", .size = 0, };
const MsgType Stopped = { .name = "Stopped", .size = 0, };

////////////////////////////////////////////////////////////////////////////////
// Actor
////////////////////////////////////////////////////////////////////////////////

typedef struct ActorCell ActorCell;
typedef struct ActorRef ActorRef;

typedef struct Context {
  ActorCell *me;
  ActorRef *ref;
  void *params;
  void *state;
} Context;

typedef struct Actor {
  void (*onStart)(Context *ctx);
  void * (*onReceive)(Context *ctx, Msg *msg);
  void (*onStop)(Context *ctx);
} Actor;

////////////////////////////////////////////////////////////////////////////////
// Queue
////////////////////////////////////////////////////////////////////////////////

typedef struct Queue {
  atomic_int reader;
  atomic_int writer;
  int max;
  void **items;
} Queue;

Queue *queue_create(int max) {
  Queue *queue = NULL;

  if (max <= 0) return NULL;

  queue = malloc(sizeof(Queue));
  queue->items = malloc(sizeof(void *) * (max + 1));

  if (queue == NULL) {
    return NULL;
  }

  queue->reader = 0;
  queue->writer = 0;
  queue->max = max + 1;

  return queue;
}

void queue_free(Queue *queue) {
  free(queue->items);
  free(queue);
}

bool queue_add(Queue *queue, void *item) {
  if (queue == NULL || item == NULL) return false;
  bool r = false;
  const int newWriter = (queue->writer + 1) % queue->max;
  if (newWriter != queue->reader) {
    queue->items[queue->writer] = item;
    queue->writer = newWriter;
    r = true;
  }
  return r;
}

bool queue_is_empty(Queue *queue) {
  bool empty = (queue->reader == queue->writer) ? true : false;
  return empty;
}

void * queue_get(Queue *queue) {
  void *item = NULL;
  if (queue->reader != queue->writer) {
    item = queue->items[queue->reader];
    queue->reader = (queue->reader + 1) % queue->max;
  }
  return item;
}

////////////////////////////////////////////////////////////////////////////////
// MailBox
////////////////////////////////////////////////////////////////////////////////

typedef struct ActorCell ActorCell;
void actorcell_start(ActorCell *actorCell);
bool actorcell_receive(ActorCell *actorCell, Msg *msg);
const char *actorcell_name(const ActorCell *actorCell);

typedef struct MailBox {
  ActorCell *actorCell;
  Queue *queue;
  atomic_bool idle;
  void * (*state)(struct MailBox *mailbox);
  bool affinity;
  int worker;
  pthread_mutex_t mutex;
} MailBox;

void * mailbox_process_start(MailBox *mailbox);
void * mailbox_process_next_message(MailBox *mailbox);

MailBox *mailbox_create(int size, bool affinity) {
  MailBox *mailbox = malloc(sizeof(MailBox));
  mailbox->actorCell = NULL;
  mailbox->idle = true;
  mailbox->queue = queue_create(size);
  mailbox->state = mailbox_process_start;
  mailbox->affinity = affinity;
  mailbox->worker = -1;
  mailbox->mutex = (pthread_mutex_t) PTHREAD_MUTEX_INITIALIZER;
  return mailbox;
}

void mailbox_clear(MailBox *mailbox) {
  Msg *msg = NULL;
  while ((msg = queue_get(mailbox->queue))) {
    msg_free(msg);
  }
}

void mailbox_free(MailBox *mailbox) {
  mailbox_clear(mailbox);
  queue_free(mailbox->queue);
  free(mailbox);
}

bool mailbox_has_message(const MailBox *mailbox) {
  return ! queue_is_empty(mailbox->queue);
}

const char *mailbox_name(const MailBox *mailbox) {
  return actorcell_name(mailbox->actorCell);
}

bool mailbox_is_idle(const MailBox *mailbox) {
  return mailbox->idle;
}

bool mailbox_set_scheduled(MailBox *mailbox) {
  bool expected = true;
  return atomic_compare_exchange_strong(&mailbox->idle, &expected, false);
}

bool mailbox_set_idle(MailBox *mailbox) {
  bool expected = false;
  return atomic_compare_exchange_strong(&mailbox->idle, &expected, true);
}

void mailbox_set_actorcell(MailBox *mailbox, ActorCell *actorCell) {
  mailbox->actorCell = actorCell;
}

void mailbox_enqueue(MailBox *mailbox, Msg *msg) {
  pthread_mutex_lock(&mailbox->mutex);
  queue_add(mailbox->queue, msg);
  pthread_mutex_unlock(&mailbox->mutex);
}

void * mailbox_process_empty(MailBox *mailbox) {
  debug("%s mailbox empty.", actorcell_name(mailbox->actorCell));
  if (queue_is_empty(mailbox->queue)) {
    return mailbox_process_empty;
  }
  return mailbox_process_next_message;
}

void * mailbox_process_next_message(MailBox *mailbox) {
  Msg *msg = queue_get(mailbox->queue);

  if (msg == NULL) {
    return mailbox_process_empty;
  }

  bool keepGoing = actorcell_receive(mailbox->actorCell, msg);

  msg_free(msg);

  if (keepGoing)
    return mailbox_process_next_message;

  return NULL;
}

void * mailbox_process_start(MailBox *mailbox) {
  actorcell_start(mailbox->actorCell);
  return mailbox_process_next_message;
}

/**
 * Executes the mailbox's state machine.
 * Returns true if the mailbox must be processed again, otherwise returns false.
 */
bool mailbox_process(MailBox *mailbox) {
  if (mailbox->state == NULL) return false;
  mailbox->state = mailbox->state(mailbox);
  return (mailbox->state == NULL || 
      mailbox->state == mailbox_process_empty) ? false : true;
}

void *mailbox_state(MailBox *mailbox) {
  return mailbox->state;
}

////////////////////////////////////////////////////////////////////////////////
// Worker
////////////////////////////////////////////////////////////////////////////////

typedef struct Executor Executor;

void executor_execute(Executor *executor, MailBox *mailbox);
void actorsystem_register_execution(MailBox *mailbox);

typedef struct Worker {
  pthread_t thread;
  Queue *queue;
  atomic_bool stop;
  pthread_cond_t condNotEmpty;
  pthread_mutex_t mutex;
  int throughput;
  int throughputDeadlineNS;
  Executor *executor;
  int core;
  char *name;
} Worker;

void *worker_run(void *arg);

int worker_set_core_affinity(int core) {
  int numCores = sysconf(_SC_NPROCESSORS_ONLN);

  if (core < 0 || core >= numCores)
    return EINVAL;

  cpu_set_t cpuset;
  CPU_ZERO(&cpuset);
  CPU_SET(core, &cpuset);

  pthread_t currentThread = pthread_self();

  return pthread_setaffinity_np(currentThread, sizeof(cpu_set_t), &cpuset);
}

Worker *worker_create(Executor *executor, const char *name, int core, int throughput, int throughputDeadlineNS) {
  Worker *worker = malloc(sizeof(Worker));
  worker->executor = executor;
  worker->stop = false;
  worker->core = core;
  worker->queue = queue_create(1000);
  worker->mutex = (pthread_mutex_t) PTHREAD_MUTEX_INITIALIZER;
  worker->condNotEmpty = (pthread_cond_t) PTHREAD_COND_INITIALIZER;
  worker->throughputDeadlineNS = throughputDeadlineNS;
  worker->throughput = throughput;
  worker->name = strdup(name);

  pthread_create(&worker->thread, NULL, worker_run, worker);

  pthread_setname_np(worker->thread, name);

  return worker;  
}

void worker_free(Worker *worker) {
  pthread_join(worker->thread, NULL);

  queue_free(worker->queue);

  free(worker->name);

  free(worker);
}

void worker_enqueue(Worker *worker, MailBox *mailbox) {
  pthread_mutex_lock(&worker->mutex);

  queue_add(worker->queue, mailbox);

  pthread_cond_signal(&worker->condNotEmpty);

  debug("Notificando %s.", worker->name);

  pthread_mutex_unlock(&worker->mutex);
}

long worker_current_time_ns() {
  long ns;
  time_t sec;
  struct timespec spec;
  const long billion = 1000000000L;

  clock_gettime(CLOCK_REALTIME, &spec);
  sec = spec.tv_sec;
  ns = spec.tv_nsec;

  return (uint64_t) sec * billion + (uint64_t) ns;
}

void *worker_run(void *arg) {
  Worker *worker =  (Worker *) arg;

  if (worker_set_core_affinity(worker->core)) {
    debug("set core affinity fail.");
  }

  while (!worker->stop) {
    pthread_mutex_lock(&worker->mutex);

    MailBox *mailbox = queue_get(worker->queue);

    if (mailbox == NULL) {
      debug("Sem mailboxes.");
      pthread_cond_wait(&worker->condNotEmpty, &worker->mutex);
      debug("Fui notificado.");
    }

    pthread_mutex_unlock(&worker->mutex);

    if (mailbox == NULL) continue;

    int leftThroughput = worker->throughput;
    long deadlineNS = worker_current_time_ns() + worker->throughputDeadlineNS;
    bool keepGoing = true;

    while (keepGoing && 
        leftThroughput > 0 && 
        ((worker->throughputDeadlineNS <= 0) || (worker_current_time_ns() - deadlineNS < 0))) {
      keepGoing = mailbox_process(mailbox);
      leftThroughput--;
    }

    mailbox_set_idle(mailbox);
    
    if (mailbox_state(mailbox) != NULL) {
      debug("MailBox state != NULL");
      actorsystem_register_execution(mailbox);
    }
  }

  return NULL;
}

void worker_stop(Worker *worker) {
  pthread_mutex_lock(&worker->mutex);
  worker->stop = true;
  pthread_cond_signal(&worker->condNotEmpty);
  pthread_mutex_unlock(&worker->mutex);
}

////////////////////////////////////////////////////////////////////////////////
// Executor
////////////////////////////////////////////////////////////////////////////////

typedef struct Executor {
  Worker **workers;
  int numWorkers;
  atomic_int currentWorker;
} Executor;

Executor *executor_create(int numWorkers) {
  Executor *executor = malloc(sizeof(Executor));
  executor->workers = malloc(sizeof(Worker *) * numWorkers);
  executor->numWorkers = numWorkers;
  executor->currentWorker = 0;

  int throughput = 5;
  int throughputDeadlineNS = -1; // not defined

  int numCores = sysconf(_SC_NPROCESSORS_ONLN);

  for (int i = 0; i < numWorkers; i++) {
    char name[32] = {0,};
    snprintf(name, sizeof(name)-1, "worker-%d", i);
    name[31] = '\0';
    executor->workers[i] = worker_create(executor, name, i % numCores, throughput, throughputDeadlineNS);
  }

  return executor;
}

void executor_destroy(Executor *executor) {
  for (int i = 0; i < executor->numWorkers; i++) {
    worker_stop(executor->workers[i]);
  }
  for (int i = 0; i < executor->numWorkers; i++) {
    worker_free(executor->workers[i]);
  }
  free(executor->workers);
  free(executor);
}

void executor_execute(Executor *executor, MailBox *mailbox) {
  int worker = mailbox->worker;
  bool undefinedWorker = (worker < 0) ? true : false;
  const char *mailboxName = mailbox_name(mailbox);

  if (!mailbox->affinity || (mailbox->affinity && undefinedWorker)) {
    int currentWorker = executor->currentWorker;
    int nextWorker = (currentWorker + 1) % executor->numWorkers;

    while (!atomic_compare_exchange_weak(&executor->currentWorker, &currentWorker, nextWorker)) {
      nextWorker = (currentWorker + 1) % executor->numWorkers;
    }

    worker = nextWorker;
  }

  mailbox->worker = worker;

  debug("%s foi agendado em %s.", mailboxName, executor->workers[worker]->name);

  worker_enqueue(executor->workers[worker], mailbox);
}

////////////////////////////////////////////////////////////////////////////////
// Dispatcher
////////////////////////////////////////////////////////////////////////////////

typedef struct Dispatcher {
  Executor *executor;
} Dispatcher;

Dispatcher *dispatcher_create(Executor *executor) {
  Dispatcher *dispatcher = malloc(sizeof(Dispatcher));
  dispatcher->executor = executor;
  return dispatcher;
}

void dispatcher_free(Dispatcher *dispatcher) {
  free(dispatcher);
}

void dispatcher_register_for_execution(Dispatcher *dispatcher, MailBox *mailbox) {
  bool hasMsg = mailbox_has_message(mailbox);
  bool isIdle = mailbox_is_idle(mailbox);
  const char *mailboxName = mailbox_name(mailbox);

  debug("Tentando agendar execução de %s [msg? %d idle? %d]", mailboxName, hasMsg, isIdle);

  if (hasMsg && isIdle) {
    if (mailbox_set_scheduled(mailbox)) {
      executor_execute(dispatcher->executor, mailbox);
    } else {
      debug("Não agendado, pois %s já esta agendado.", mailboxName);
    }
  }
}

void dispatcher_dispatch(Dispatcher *dispatcher, MailBox *mailbox, Msg *msg) {
  debug("%s enviando %s para %s", actorcell_name(msg->from), msg->type->name, mailbox_name(mailbox));
  mailbox_enqueue(mailbox, msg);
  dispatcher_register_for_execution(dispatcher, mailbox);
}

////////////////////////////////////////////////////////////////////////////////
// ActorCell
////////////////////////////////////////////////////////////////////////////////

typedef struct ActorCell ActorCell;
typedef struct ActorRef ActorRef;

ActorRef *actorref_create_weak(ActorCell *actorCell);
void actorref_free(ActorRef *actorRef);

typedef struct ActorCellList {
  ActorCell *actor;
  struct ActorCellList *next;
} ActorCellList;

typedef struct ActorCell {
  MailBox *mailbox;
  Actor actor;
  Context context;
  Dispatcher *dispatcher;
  struct ActorCell *parent;
  struct ActorCellList *children;
  int childrenCount;
  bool stopping;
  char *name;
} ActorCell;

int actorcell_add_child(ActorCell *parent, ActorCell *child) {
  ActorCellList *node = malloc(sizeof(ActorCellList));
  node->next = parent->children;
  node->actor = child;
  parent->children = node;
  return ++parent->childrenCount;
}

int actorcell_remove_child(ActorCell *parent, ActorCell *child) {
  ActorCellList *node = parent->children;
  ActorCellList *prev = NULL;
  while (node != NULL) {
    if (node->actor == child) {
      if (prev == NULL) {
        parent->children = node->next;
      } else {
        prev->next = node->next;
      }
      free(node);
      break;
    }
    prev = node;
    node = node->next;
  }
  return --parent->childrenCount;
}

int actorcell_children_count(const ActorCell *actor) {
  return actor->childrenCount;
}

ActorCell *actorcell_create(ActorCell *parent, const char *name, const Actor *actor, const void *params, size_t size, Dispatcher *dispatcher, bool affinity) {
  ActorCell *actorCell = malloc(sizeof(ActorCell));
  actorCell->actor = *actor;
  actorCell->mailbox = mailbox_create(1000, affinity);
  actorCell->dispatcher = dispatcher;
  actorCell->context.state = NULL;
  actorCell->context.me = actorCell;
  actorCell->context.ref = actorref_create_weak(actorCell);
  actorCell->parent = parent;
  actorCell->children = NULL;
  actorCell->childrenCount = 0;
  actorCell->stopping = false;
  actorCell->name = strdup(name);

  actorCell->context.params = malloc(size);
  memcpy(actorCell->context.params, params, size);

  mailbox_set_actorcell(actorCell->mailbox, actorCell);

  if (parent != NULL) {
    actorcell_add_child(parent, actorCell);
  }

  executor_execute(actorCell->dispatcher->executor, actorCell->mailbox);

  return actorCell;
}

void actorcell_free(ActorCell *actorCell) {
  actorref_free(actorCell->context.ref);
  free(actorCell->context.params);
  mailbox_free(actorCell->mailbox);
  free(actorCell->name);
  free(actorCell->children);
  free(actorCell);
}

void actorcell_start(ActorCell *actorCell) {
  actorCell->actor.onStart(&actorCell->context);
}

void actorcell_send(ActorCell *from, ActorCell *to, const MsgType *type, const void *payload) {
  Msg *msg = msg_create(from, type, payload);
  dispatcher_dispatch(to->dispatcher, to->mailbox, msg);
}

void actorcell_stop(ActorCell *actorCell, Msg *msg) {
  actorCell->stopping = true;

  ActorCellList *child = actorCell->children;

  while (child != NULL) {
    actorcell_send(actorCell, child->actor, &Stop, NULL);
    child = child->next;
  }
}

void actorcell_stopped(ActorCell *actorCell, Msg *msg) {
  actorcell_remove_child(actorCell, msg->from);
}

bool actorcell_receive(ActorCell *actorCell, Msg *msg) {
  if (msg->type == &Stop) {
    actorcell_stop(actorCell, msg);
  } else if (msg->type == &Stopped) {
    actorcell_stopped(actorCell, msg);
  } else {
    actorCell->actor.onReceive = actorCell->actor.onReceive(&actorCell->context, msg);
    if (actorCell->actor.onReceive == NULL) {
      actorcell_stop(actorCell, NULL);
    } 
  }

  debug("actorCell->children = %p", actorCell->children);
  debug("actorCell->childrenCount = %d", actorCell->childrenCount);

  if ((actorCell->stopping || actorCell->parent == NULL) && actorCell->children == NULL) {
    actorCell->actor.onStop(&actorCell->context);
    if (actorCell->parent != NULL) {
      actorcell_send(actorCell, actorCell->parent, &Stopped, NULL);
    }
    debug("ActorCell stopped (keepGoing = false)");
    return false; 
  }

  return true;
}

const char *actorcell_name(const ActorCell *actorCell) {
  return actorCell->name;
}

MailBox *actorcell_mailbox(ActorCell *actorCell) {
  return actorCell->mailbox;
}

////////////////////////////////////////////////////////////////////////////////
// ActorRef
////////////////////////////////////////////////////////////////////////////////

typedef struct ActorRef {
  ActorCell *actorCell;
  bool weak;
} ActorRef;

ActorRef *actorref_create(ActorRef *parent, const char *name, const Actor *actor, 
    const void *params, size_t size, Dispatcher *dispatcher, bool affinity) {
  ActorCell *parentCell = (parent != NULL) ? parent->actorCell : NULL;
  ActorRef *actorRef = malloc(sizeof(ActorRef));
  actorRef->weak = false;
  actorRef->actorCell = 
    actorcell_create(parentCell, name, actor, params, size, dispatcher, affinity);
  return actorRef;
}

ActorRef *actorref_create_weak(ActorCell *actorCell) {
  ActorRef *actorRef = malloc(sizeof(ActorRef));
  actorRef->weak = true;
  actorRef->actorCell = actorCell;
  return actorRef;
}

void actorref_free(ActorRef *actorRef) {
  if (!actorRef->weak) {
    actorcell_free(actorRef->actorCell);
  }
  free(actorRef);
}

void actorref_send(ActorRef *from, ActorRef *to, const MsgType *type, const void *payload) {
  actorcell_send(from->actorCell, to->actorCell, type, payload);
}

////////////////////////////////////////////////////////////////////////////////
// ActorSystem
////////////////////////////////////////////////////////////////////////////////

typedef struct ActorSystem {
  Executor *executor;
  Dispatcher *dispatcher;
  atomic_bool stop;
  atomic_bool stopChildren;
  atomic_bool stopChildrenDone;
  pthread_cond_t waitCond;
  pthread_mutex_t waitMutex;
} ActorSystem;

static ActorSystem *ACTOR_SYSTEM = NULL;

void system_on_start(Context *context);
void * system_on_receive(Context *context, Msg *msg);
void system_on_stop(Context *context);

void actorsystem_register_execution(MailBox *mailbox) {
  dispatcher_register_for_execution(ACTOR_SYSTEM->dispatcher, mailbox);
}

Actor System = {
  .onStart = system_on_start,
  .onReceive = system_on_receive,
  .onStop = system_on_stop,
};

void system_on_start(Context *context) { 
  debug("System Actor started.");
}

void * system_on_receive(Context *context, Msg *msg) {
  return system_on_receive;
}

void system_on_stop(Context *context) {
  pthread_mutex_lock(&ACTOR_SYSTEM->waitMutex);
  ACTOR_SYSTEM->stop = true;
  pthread_cond_signal(&ACTOR_SYSTEM->waitCond);
  debug("System stopped.");
  pthread_mutex_unlock(&ACTOR_SYSTEM->waitMutex);
}

void actorsystem_sig_term_handler(int signum, siginfo_t *info, void *ptr) {
  ACTOR_SYSTEM->stopChildren = true;
}

int actorsystem_setup_signals() {
  static struct sigaction sigact;

  memset(&sigact, 0, sizeof(sigact));
  sigact.sa_sigaction = actorsystem_sig_term_handler;
  sigact.sa_flags = SA_SIGINFO;

  if (sigaction(SIGTERM, &sigact, NULL)) {
    return -1;
  }

  if (sigaction(SIGINT, &sigact, NULL)) {
    return -1;
  }

  return 0;
}

ActorRef *actorsystem_actor_ref(ActorRef *parent, const char *name, const Actor *actor, const void *params, size_t size, bool affinity) {
  return actorref_create(parent, name, actor, params, size, ACTOR_SYSTEM->dispatcher, affinity);
}

ActorRef * actorsystem_create(int cores) {
  debug("Starting actor system.");

  ActorSystem *actorSystem = malloc(sizeof(ActorSystem));
  actorSystem->executor = executor_create(cores);
  actorSystem->dispatcher = dispatcher_create(actorSystem->executor);
  actorSystem->stop = false;
  actorSystem->stopChildren = false;
  actorSystem->stopChildrenDone = false;
  actorSystem->waitCond = (pthread_cond_t) PTHREAD_COND_INITIALIZER;
  actorSystem->waitMutex = (pthread_mutex_t) PTHREAD_MUTEX_INITIALIZER;

  ACTOR_SYSTEM = actorSystem;

  actorsystem_setup_signals();

  return actorsystem_actor_ref(NULL, "System", &System, NULL, 0, true);
}

void actorsystem_send(ActorCell *from, ActorCell *to, 
    const MsgType *type, const void *payload) {
  actorcell_send(from, to, type, payload);
}

int actorsystem_wait(ActorRef *system) {
  while (!ACTOR_SYSTEM->stop) {
    pthread_mutex_lock(&ACTOR_SYSTEM->waitMutex);
    
    struct timespec spec;
    clock_gettime(CLOCK_REALTIME, &spec);
    spec.tv_sec += 1;
    
    pthread_cond_timedwait(&ACTOR_SYSTEM->waitCond, &ACTOR_SYSTEM->waitMutex, &spec);
    
    debug("Fui notificado!");

    if (ACTOR_SYSTEM->stopChildren) {
      if (ACTOR_SYSTEM->stopChildrenDone) {
        debug("Waiting for children stop.");
      } else {
        ActorCellList *node = system->actorCell->children;
        while (node != NULL) {
          debug("Stopping child: %s", actorcell_name(node->actor));
          actorsystem_send(system->actorCell, node->actor, &Stop, NULL);
          node = node->next;
        }
        ACTOR_SYSTEM->stopChildrenDone = true;
      }
    }

    pthread_mutex_unlock(&ACTOR_SYSTEM->waitMutex);
  }
  return 0;
}

void actorsystem_free(ActorRef *system) {
  dispatcher_free(ACTOR_SYSTEM->dispatcher);
  executor_destroy(ACTOR_SYSTEM->executor);
  actorref_free(system);
  free(ACTOR_SYSTEM);
  debug("Actor system stopped.");
}

////////////////////////////////////////////////////////////////////////////////
// ActorPong
////////////////////////////////////////////////////////////////////////////////

typedef struct PingParams {
  int num;
} PingParams;

const MsgType Ping = { .name = "Ping", .size = sizeof(PingParams), };

void ponger_on_start(Context *context);
void * ponger_on_receive(Context *context, Msg *msg);
void ponger_on_stop(Context *context);

Actor Ponger = {
  .onStart = ponger_on_start,
  .onReceive = ponger_on_receive,
  .onStop = ponger_on_stop,
};

typedef struct PongParams {
  int num;
} PongParams;

const MsgType Pong = { .name = "Pong", .size = sizeof(PongParams) };

void ponger_on_start(Context *context) {
  debug("Ponger started.");
}

void * ponger_on_receive(Context *context, Msg *msg) {
  if (msg->type == &Ping) {
    const PingParams *ping = msg->payload;
    debug("Pong %d", ping->num);
    PongParams pong = { .num = ping->num };
    actorsystem_send(context->me, msg->from, &Pong, &pong);
  }

  return ponger_on_receive;
}

void ponger_on_stop(Context *context) {
  debug("Ponger stopped.");
}

////////////////////////////////////////////////////////////////////////////////
// ActorPing
////////////////////////////////////////////////////////////////////////////////

void pinger_on_start(Context *context);
void * pinger_on_receive(Context *context, Msg *msg);
void pinger_on_stop(Context *context);

Actor Pinger = {
  .onStart = pinger_on_start,
  .onReceive = pinger_on_receive,
  .onStop = pinger_on_stop,
};

typedef struct PingerParams {
  int maxPings;
  bool debug;
} PingerParams;

typedef struct PingerState {
  int numPings;
  //ActorRef *ponger;
} PingerState;


void pinger_on_start(Context *context) {
  PingerState *state = malloc(sizeof(PingerState));
  state->numPings = 0;
  //state->ponger = NULL;
  //state->ponger = actorsystem_actor_ref(context->ref, "Ponger", &Ponger, NULL, 0, true);

  //PingParams ping = { .num = state->numPings };
  //actorsystem_send(context->me, state->ponger->actorCell, &Ping, &ping);
  PongParams pong = { .num = state->numPings };
  actorsystem_send(context->me, context->me, &Pong, &pong);

  context->state = state;

  debug("Pinger started.");
}

void * pinger_on_receive(Context *context, Msg *msg) {
  PingerState *state = context->state;
  PingerParams *params = context->params;

  if (msg->type == &Pong) {
    PongParams *pong = msg->payload;

    if (params->debug) {
      debug("Ping %d", pong->num);
    }

    if (state->numPings >= params->maxPings) {
      return NULL;
    }

    state->numPings++;

    //PingParams ping = { .num = state->numPings };
    //actorsystem_send(context->me, state->ponger->actorCell, &Ping, &ping);
    PongParams pong2 = { .num = state->numPings };
    actorsystem_send(context->me, context->me, &Pong, &pong2);
  }

  return pinger_on_receive;
}

void pinger_on_stop(Context *context) {
  PingerState *state = context->state;
  //actorref_free(state->ponger);
  free(state);
  debug("Pinger stopped.");
}

////////////////////////////////////////////////////////////////////////////////
// Main
////////////////////////////////////////////////////////////////////////////////

int main(int argc, char **argv) {
  const int workers = atoi(argv[1]);
  const int pingers = atoi(argv[2]);
  const int pings = atoi(argv[3]);
  const bool debugEnable = strcmp(argv[4], "true") ? false : true;
  ActorRef *pingersRef[32] = {NULL};

  DEBUG_ENABLED = debugEnable;

  debug("Workers: %d", workers);
  debug("Pingers: %d", pingers);
  debug("Pings: %d", pings);
  debug("Debug: %s", debugEnable ? "true" : "false");

  ActorRef *system = actorsystem_create(workers);
 
  for (int i = 0; i < pingers; i++) {
    pingersRef[i] = actorsystem_actor_ref(system,
        "Pinger",
        &Pinger, 
        &(PingerParams){ .maxPings = pings, .debug = debugEnable }, 
        sizeof(PingerParams), 
        true);
  }

  int result = actorsystem_wait(system);

  for (int i = 0; i < pingers; i++) {
    actorref_free(pingersRef[i]);
  }

  actorsystem_free(system);

  return result;
}


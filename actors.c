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
// Msg
////////////////////////////////////////////////////////////////////////////////

typedef struct MsgType {
  const char *name;
  size_t size;
} MsgType;

typedef struct Msg {
  const MsgType *type;
  void *payload;
} Msg;

Msg *msg_create(const MsgType *type, const void *payload) {
  Msg *msg = malloc(sizeof(Msg));
  msg->payload = malloc(type->size);
  msg->type = type;

  memcpy(msg->payload, payload, type->size);

  return msg;
}

void msg_free(Msg *msg) {
  free(msg->payload);
  free(msg);
}

const MsgType Stop = { .name = "Stop", .size = 0, };

////////////////////////////////////////////////////////////////////////////////
// Actor
////////////////////////////////////////////////////////////////////////////////

typedef struct ActorCell ActorCell;

typedef struct Context {
  ActorCell *me;
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
  pthread_mutex_t writerMutex;
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
  queue->writerMutex = (pthread_mutex_t) PTHREAD_MUTEX_INITIALIZER;

  return queue;
}

void queue_free(Queue *queue) {
  free(queue->items);
  free(queue);
}

bool queue_add(Queue *queue, void *item) {
  if (queue == NULL || item == NULL) return false;

  bool r = false;

  pthread_mutex_lock(&queue->writerMutex);

  const int newWriter = (queue->writer + 1) % queue->max;

  if (newWriter != queue->reader) {
    queue->items[queue->writer] = item;
    queue->writer = newWriter;
    r = true;
  }

  pthread_mutex_unlock(&queue->writerMutex);

  return r;
}

bool queue_is_empty(const Queue *queue) {
  return queue->reader == queue->writer;
}

void * queue_get(Queue *queue) {
  if (!queue_is_empty(queue)) {
    void *item = queue->items[queue->reader];
    queue->reader = (queue->reader + 1) % queue->max;
    return item;
  }
  return NULL;
}

////////////////////////////////////////////////////////////////////////////////
// MailBox
////////////////////////////////////////////////////////////////////////////////

typedef struct ActorCell ActorCell;
void actorcell_start(ActorCell *actorCell);
bool actorcell_receive(ActorCell *actorCell, Msg *msg);
void actorcell_stop(ActorCell *actorCell);


typedef struct MailBox {
  ActorCell *actorCell;
  bool shouldProcessMessage;
  Queue *queue;
  atomic_bool idle;
  void * (*state)(struct MailBox *mailbox);
  bool affinity;
  int worker;
} MailBox;

typedef enum MailBoxProcessResult {
  MAILBOX_OK = 0,
  MAILBOX_BLOCKED = -1,
  MAILBOX_EMPTY = -2,
} MailBoxProcessResult;

void * mailbox_process_start(MailBox *mailbox);
void * mailbox_process_next_message(MailBox *mailbox);

MailBox *mailbox_create(int size, bool affinity) {
  MailBox *mailbox = malloc(sizeof(MailBox));
  mailbox->actorCell = NULL;
  mailbox->idle = true;
  mailbox->queue = queue_create(size);
  mailbox->state = mailbox_process_start;
  mailbox->shouldProcessMessage = true;
  mailbox->affinity = affinity;
  return mailbox;
}

void mailbox_free(MailBox *mailbox) {
  queue_free(mailbox->queue);
  free(mailbox);
}

bool mailbox_has_message(const MailBox *mailbox) {
  return ! queue_is_empty(mailbox->queue);
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
  queue_add(mailbox->queue, msg);
}

void * mailbox_process_stop(MailBox *mailbox) {
  actorcell_stop(mailbox->actorCell);
  return NULL;
}

void * mailbox_process_empty(MailBox *mailbox) {
  if (queue_is_empty(mailbox->queue)) {
    return mailbox_process_empty;
  }
  return mailbox_process_next_message;
}

void * mailbox_process_next_message(MailBox *mailbox) {
  if (!mailbox->shouldProcessMessage) {
    return mailbox_process_next_message;
  }

  Msg *msg = queue_get(mailbox->queue);

  if (msg == NULL) {
    return mailbox_process_empty;
  }

  bool keepGoing = false;

  if (msg->type == &Stop) {
    keepGoing = false;
  } else {
    keepGoing = actorcell_receive(mailbox->actorCell, msg);
  }

  msg_free(msg);

  if (keepGoing)
    return mailbox_process_next_message;

  return mailbox_process_stop;
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
  mailbox->state = mailbox->state(mailbox);
  return (mailbox->state == NULL || 
      mailbox->state == mailbox_process_empty) ? false : true;
}


////////////////////////////////////////////////////////////////////////////////
// Worker
////////////////////////////////////////////////////////////////////////////////

typedef struct Executor Executor;
void executor_execute(Executor *executor, MailBox *mailbox);

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

Worker *worker_create(Executor *executor, int core, int throughput, int throughputDeadlineNS) {
  Worker *worker = malloc(sizeof(Worker));
  worker->executor = executor;
  worker->stop = false;
  worker->core = core;
  worker->queue = queue_create(1000);
  worker->mutex = (pthread_mutex_t) PTHREAD_MUTEX_INITIALIZER;
  worker->condNotEmpty = (pthread_cond_t) PTHREAD_COND_INITIALIZER;
  worker->throughputDeadlineNS = throughputDeadlineNS;
  worker->throughput = throughput;

  pthread_create(&worker->thread, NULL, worker_run, worker);

  return worker;  
}

void worker_free(Worker *worker) {
  pthread_mutex_lock(&worker->mutex);
  pthread_cond_signal(&worker->condNotEmpty);
  pthread_mutex_unlock(&worker->mutex);

  pthread_join(worker->thread, NULL);

  queue_free(worker->queue);

  free(worker);
}

void worker_enqueue(Worker *worker, MailBox *mailbox) {
  pthread_mutex_lock(&worker->mutex);
  queue_add(worker->queue, mailbox);
  pthread_cond_signal(&worker->condNotEmpty);
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
    printf("[worker] set core affinity fail.\n");
  }

  while (!worker->stop) {
    MailBox *mailbox = queue_get(worker->queue);

    if (mailbox == NULL) {
      pthread_mutex_lock(&worker->mutex);
      pthread_cond_wait(&worker->condNotEmpty, &worker->mutex);
      pthread_mutex_unlock(&worker->mutex);
      continue;
    }

    int leftThroughput = worker->throughput;
    long deadlineNS = worker_current_time_ns() + worker->throughputDeadlineNS;
    bool keepGoing = true;

    while (keepGoing && 
        leftThroughput > 0 && 
        ((worker->throughputDeadlineNS <= 0) || (worker_current_time_ns() - deadlineNS < 0))) {
      keepGoing = mailbox_process(mailbox);
      leftThroughput--;
    }

    if (keepGoing) {
      executor_execute(worker->executor, mailbox);
    } else {
      mailbox_set_idle(mailbox);
    }
  }

  return NULL;
}

void worker_stop(Worker *worker) {
  worker->stop = true;
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
    executor->workers[i] = worker_create(executor, i % numCores, throughput, throughputDeadlineNS);
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

  if (!mailbox->affinity || (mailbox->affinity && undefinedWorker)) {
    int currentWorker = executor->currentWorker;
    int nextWorker = (currentWorker + 1) % executor->numWorkers;

    while (!atomic_compare_exchange_weak(&executor->currentWorker, &currentWorker, nextWorker)) {
      nextWorker = (currentWorker + 1) % executor->numWorkers;
    }

    worker = nextWorker;
  }

  mailbox->worker = worker;

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
  if (/*mailbox_has_message(mailbox) &&*/ mailbox_is_idle(mailbox)) {
    if (mailbox_set_scheduled(mailbox)) {
      executor_execute(dispatcher->executor, mailbox);
    }
  }
}

void dispatcher_dispatch(Dispatcher *dispatcher, MailBox *mailbox, Msg *msg) {
  mailbox_enqueue(mailbox, msg);
  dispatcher_register_for_execution(dispatcher, mailbox);
}

////////////////////////////////////////////////////////////////////////////////
// ActorCell
////////////////////////////////////////////////////////////////////////////////

typedef struct ActorCell {
  MailBox *mailbox;
  Actor actor;
  Context context;
  Dispatcher *dispatcher;
} ActorCell;

ActorCell *actorcell_create(const Actor *actor, const void *params, size_t size, Dispatcher *dispatcher, bool affinity) {
  ActorCell *actorCell = malloc(sizeof(ActorCell));
  actorCell->actor = *actor;
  actorCell->mailbox = mailbox_create(1000, affinity);
  actorCell->dispatcher = dispatcher;
  actorCell->context.state = NULL;
  actorCell->context.me = actorCell;

  actorCell->context.params = malloc(size);
  memcpy(actorCell->context.params, params, size);

  mailbox_set_actorcell(actorCell->mailbox, actorCell);

  dispatcher_register_for_execution(actorCell->dispatcher, actorCell->mailbox);

  return actorCell;
}

void actorcell_free(ActorCell *actorCell) {
  free(actorCell->context.params);
  mailbox_free(actorCell->mailbox);
  free(actorCell);
}

void actorcell_start(ActorCell *actorCell) {
  actorCell->actor.onStart(&actorCell->context);
}

bool actorcell_receive(ActorCell *actorCell, Msg *msg) {
  actorCell->actor.onReceive = actorCell->actor.onReceive(&actorCell->context, msg);
  return (actorCell->actor.onReceive != NULL) ? true : false;
}

void actorcell_stop(ActorCell *actorCell) {
  actorCell->actor.onStop(&actorCell->context);
}

MailBox *actorcell_mailbox(ActorCell *actorCell) {
  return actorCell->mailbox;
}

void actorcell_send(ActorCell *actorCell, const MsgType *type, const void *payload) {
  Msg *msg = msg_create(type, payload);
  dispatcher_dispatch(actorCell->dispatcher, actorCell->mailbox, msg);
}

////////////////////////////////////////////////////////////////////////////////
// ActorRef
////////////////////////////////////////////////////////////////////////////////

typedef struct ActorRef {
  ActorCell *actorCell;
} ActorRef;

ActorRef *actorref_create(const Actor *actor, const void *params, size_t size, Dispatcher *dispatcher, bool affinity) {
  ActorRef *actorRef = malloc(sizeof(ActorRef));
  actorRef->actorCell = actorcell_create(actor, params, size, dispatcher, affinity);
  return actorRef;
}

void actorref_free(ActorRef *actorRef) {
  actorcell_free(actorRef->actorCell);
  free(actorRef);
}

void actorref_send(ActorRef *actorRef, const MsgType *type, const void *payload) {
  actorcell_send(actorRef->actorCell, type, payload);
}

////////////////////////////////////////////////////////////////////////////////
// ActorSystem
////////////////////////////////////////////////////////////////////////////////

typedef struct ActorSystem {
  Executor *executor;
  Dispatcher *dispatcher;
  atomic_bool stop;
} ActorSystem;

static ActorSystem *ACTOR_SYSTEM = NULL;

void actorsystem_stop() {
  ACTOR_SYSTEM->stop = true;
}

void actorsystem_sig_term_handler(int signum, siginfo_t *info, void *ptr) {
  actorsystem_stop();
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

void actorsystem_create(int cores) {
  ActorSystem *actorSystem = malloc(sizeof(ActorSystem));
  actorSystem->executor = executor_create(cores);
  actorSystem->dispatcher = dispatcher_create(actorSystem->executor);
  actorSystem->stop = false;

  ACTOR_SYSTEM = actorSystem;

  actorsystem_setup_signals();
}

ActorRef *actorsystem_actor_ref(const Actor *actor, const void *params, size_t size, bool affinity) {
  return actorref_create(actor, params, size, ACTOR_SYSTEM->dispatcher, affinity);
}

int actorsystem_main(const Actor *actor, const void *params, size_t size, bool affinity, int cores) {
  printf("Starting actor system.\n");

  actorsystem_create(cores);

  ActorRef *mainActor = actorsystem_actor_ref(actor, params, size, affinity);

  while (!ACTOR_SYSTEM->stop) {
    sleep(1);
  }

  actorref_free(mainActor);

  dispatcher_free(ACTOR_SYSTEM->dispatcher);
  executor_destroy(ACTOR_SYSTEM->executor);
  free(ACTOR_SYSTEM);

  printf("\nActor system stopped.\n");

  return 0;
}

void actorsystem_send(ActorRef *actorRef, const MsgType *type, const void *payload) {
  actorref_send(actorRef, type, payload);
}

void actorsystem_sendme(Context *context, const MsgType *type, const void *payload) {
  actorcell_send(context->me, type, payload);
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
} PingerParams;

typedef struct PingerState {
  int numPings;
} PingerState;

typedef struct PingerPingParams {
  int num;
} PingerPingParams;

const MsgType PingerPing = { .name = "Ping", .size = sizeof(PingerPingParams), };

void pinger_on_start(Context *context) {
  PingerState *state = malloc(sizeof(PingerState));
  state->numPings = 0;

  PingerPingParams msgToSend = { .num = state->numPings };
  actorsystem_sendme(context, &PingerPing, &msgToSend);

  context->state = state;

  printf("Pinger started.\n");
}

void * pinger_on_receive(Context *context, Msg *msg) {
  PingerState *state = context->state;
  PingerParams *params = context->params;
  PingerPingParams *msgRecv = msg->payload;

  printf("Ping %d\n", msgRecv->num);

  state->numPings++;

  if (state->numPings >= params->maxPings) {
    actorsystem_sendme(context, &Stop, NULL);
    return pinger_on_receive;
  }

  PingerPingParams msgToSend = { .num = state->numPings };
  actorsystem_sendme(context, &PingerPing, &msgToSend);

  return pinger_on_receive;
}

void pinger_on_stop(Context *context) {
  free(context->state);
  printf("Pinger stopped.\n");
}

////////////////////////////////////////////////////////////////////////////////
// Main
////////////////////////////////////////////////////////////////////////////////

int main() {
  PingerParams params = {.maxPings = 10000};
  return actorsystem_main(&Pinger, &params, sizeof(PingerParams), true, 4);
}


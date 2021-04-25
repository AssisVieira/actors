////////////////////////////////////////////////////////////////////////////////
// Dispatcher
////////////////////////////////////////////////////////////////////////////////

#include "dispatcher.h"

#include "actorcell.h"
#include "msg.h"
#include "worker.h"

Dispatcher *dispatcher_create(int numWorkers) {
  Dispatcher *dispatcher = malloc(sizeof(Dispatcher));
  dispatcher->workers = malloc(sizeof(Worker *) * numWorkers);
  dispatcher->numWorkers = numWorkers;
  dispatcher->currentWorker = 0;

  int throughput = 10;
  int throughputDeadlineNS = -1;  // not defined

  int numCores = sysconf(_SC_NPROCESSORS_ONLN);

  for (int i = 0; i < numWorkers; i++) {
    char name[32] = {0};
    snprintf(name, sizeof(name) - 1, "worker-%d", i);
    name[31] = '\0';
    dispatcher->workers[i] = worker_create(dispatcher, name, i % numCores,
                                           throughput, throughputDeadlineNS);
  }

  return dispatcher;
}

void dispatcher_free(Dispatcher *dispatcher) {
  for (int i = 0; i < dispatcher->numWorkers; i++) {
    worker_stop(dispatcher->workers[i]);
  }
  for (int i = 0; i < dispatcher->numWorkers; i++) {
    worker_free(dispatcher->workers[i]);
  }
  free(dispatcher->workers);
  free(dispatcher);
}

void dispatcher_register_for_execution(Dispatcher *dispatcher,
                                       ActorCell *actor) {
  bool isEmpty = actorcell_is_empty(actor);
  const char *actorName = actorcell_name(actor);

  debugf("Tentando agendar execução de %s [Tem msg? %s]", actorName,
         (isEmpty) ? "false" : "true");

  if (!isEmpty) {
    if (actorcell_set_scheduled(actor)) {
      dispatcher_execute(dispatcher, actor);
    } else {
      debugf("Não agendado, pois %s já esta agendado.", actorName);
    }
  }
}

void dispatcher_dispatch(Dispatcher *dispatcher, ActorCell *actor) {
  dispatcher_register_for_execution(dispatcher, actor);
}

void dispatcher_execute(Dispatcher *dispatcher, ActorCell *actor) {
  int worker = actorcell_worker(actor);
  bool undefinedWorker = (worker < 0) ? true : false;
  const char *actorName = actorcell_name(actor);
  bool affinity = actorcell_affinity(actor);

  if (!affinity || (affinity && undefinedWorker)) {
    int currentWorker = dispatcher->currentWorker;
    int nextWorker = (currentWorker + 1) % dispatcher->numWorkers;

    while (!atomic_compare_exchange_weak(&dispatcher->currentWorker,
                                         &currentWorker, nextWorker)) {
      nextWorker = (currentWorker + 1) % dispatcher->numWorkers;
    }

    worker = nextWorker;
  }

  actorcell_set_worker(actor, worker);

  debugf("%s foi agendado em %s.", actorName,
         dispatcher->workers[worker]->name);

  worker_enqueue(dispatcher->workers[worker], actor);
}

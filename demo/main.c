////////////////////////////////////////////////////////////////////////////////
// Main
////////////////////////////////////////////////////////////////////////////////

#include "pinger.h"
#include "ponger.h"

int main(int argc, char **argv) {
  const int workers = atoi(argv[1]);
  const int pingers = atoi(argv[2]);
  const int pings = atoi(argv[3]);
  const bool debugEnable = strcmp(argv[4], "true") ? false : true;

  DEBUG_ENABLED = debugEnable;

  ActorCell *system = actors_create(workers);

  for (int i = 0; i < pingers; i++) {
    actors_child_new(system, "Pinger", &Pinger,
                     &(PingerParams){.maxPings = pings, .debug = debugEnable});
  }

  return actors_wait_children(system);
}

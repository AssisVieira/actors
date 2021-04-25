#ifndef PINGER_INCLUDE_H
#define PINGER_INCLUDE_H

#include "actors/actors.h"

ACTOR(Pinger, {
  int maxPings;
  bool debug;
});

MSG(Ping, { int num; });

#endif

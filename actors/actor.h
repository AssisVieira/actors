////////////////////////////////////////////////////////////////////////////////
// Actor
////////////////////////////////////////////////////////////////////////////////

#ifndef ACTOR_INCLUDE_H
#define ACTOR_INCLUDE_H

#include "foundation/foundation.h"

typedef struct ActorCell ActorCell;
typedef struct Msg Msg;

typedef void (*ActorOnStart)(ActorCell *actor, Msg *msg);
typedef bool (*ActorOnReceive)(ActorCell *actor, Msg *msg);
typedef void (*ActorOnStop)(ActorCell *actor, Msg *msg);

typedef struct Actor {
  char *name;
  size_t stateSize;
  size_t paramsSize;
  ActorOnStart onStart;
  ActorOnReceive onReceive;
  ActorOnStop onStop;
} Actor;

/**
 * Declara um ator e uma struct usada pra inicializa-lo.
 *
 * @params typeName   nome do tipo do ator.
 * @params paramsName nome da struct de inicialização.
 */
#define ACTOR(name, params)                            \
  typedef struct name##Params params name##Params; \
  extern const Actor name

#define ACTOR_WITHOUT_PARAMS(name)                          \
  extern const Actor name

/**
 * Define a implementação do ator e a estrutura do contexto do ator.
 */
#define ACTOR_IMPL(typeName, state, fnOnStart, fnOnReceive, fnOnStop) \
  typedef struct typeName##State state typeName##State;               \
  const Actor typeName = {                                            \
      .name = #typeName,                                              \
      .stateSize = sizeof(typeName##State),                           \
      .paramsSize = sizeof(typeName##Params),                         \
      .onStart = fnOnStart,                                           \
      .onReceive = fnOnReceive,                                       \
      .onStop = fnOnStop,                                             \
  }

/**
 * Define a implementação do ator e a estrutura do contexto do ator.
 */
#define ACTOR_IMPL_WITHOUT_STATE(typeName, fnOnStart, fnOnReceive, fnOnStop) \
  typedef struct typeName##State state typeName##State;                      \
  const Actor typeName = {                                                   \
      .name = #typeName,                                                     \
      .stateSize = sizeof(typeName##State),                                  \
      .paramsSize = sizeof(typeName##Params),                                \
      .onStart = fnOnStart,                                                  \
      .onReceive = fnOnReceive,                                              \
      .onStop = fnOnStop,                                                    \
  }
#endif

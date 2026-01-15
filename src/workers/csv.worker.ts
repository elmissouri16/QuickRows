/// <reference lib="webworker" />

type BuildSortLookupMessage = {
  type: "BUILD_SORT_LOOKUP";
  requestId: number;
  order: number[];
};

type WorkerMessage = BuildSortLookupMessage;

const ctx: DedicatedWorkerGlobalScope = self as DedicatedWorkerGlobalScope;

ctx.addEventListener("message", (event) => {
  const message = event.data as WorkerMessage | undefined;
  if (!message || message.type !== "BUILD_SORT_LOOKUP") {
    return;
  }

  const lookup = new Uint32Array(message.order.length);
  for (let i = 0; i < message.order.length; i += 1) {
    lookup[message.order[i]] = i;
  }

  ctx.postMessage(
    {
      type: "SORT_LOOKUP_RESULT",
      requestId: message.requestId,
      lookup,
    },
    [lookup.buffer],
  );
});

const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const vm = require("node:vm");

function loadBackground() {
  const filePath = path.join(__dirname, "..", "background.js");
  const source = fs.readFileSync(filePath, "utf8");
  const listeners = [];
  const sentToTabs = [];

  class FakeWebSocket {
    static instances = [];

    constructor(url) {
      this.url = url;
      this.readyState = FakeWebSocket.CONNECTING;
      this.sent = [];
      FakeWebSocket.instances.push(this);
      setImmediate(() => {
        this.readyState = FakeWebSocket.OPEN;
        this.onopen?.();
      });
    }

    send(payload) {
      this.sent.push(JSON.parse(payload));
    }

    close() {
      this.readyState = FakeWebSocket.CLOSED;
      this.onclose?.();
    }
  }

  FakeWebSocket.CONNECTING = 0;
  FakeWebSocket.OPEN = 1;
  FakeWebSocket.CLOSED = 3;

  const context = {
    console,
    setTimeout,
    clearTimeout,
    setInterval,
    clearInterval,
    crypto: { randomUUID: () => "uuid-fixed" },
    WebSocket: FakeWebSocket,
    globalThis: {
      __PAGE_COMMENT_TEST_MODE: true,
    },
    chrome: {
      runtime: {
        onMessage: {
          addListener(fn) {
            listeners.push(fn);
          },
        },
      },
      storage: {
        local: {
          get(defaults, callback) {
            const response = { ...defaults };
            if (Object.prototype.hasOwnProperty.call(defaults, "serverUrl")) {
              response.serverUrl = "ws://localhost:18080";
            }
            callback(response);
          },
        },
      },
      tabs: {
        query(_queryInfo, callback) {
          callback([]);
        },
        sendMessage(tabId, message) {
          sentToTabs.push({ tabId, message });
          return { catch() {} };
        },
      },
    },
  };

  vm.createContext(context);
  vm.runInContext(source, context, { filename: filePath });

  return {
    listener: listeners[0],
    FakeWebSocket,
    sentToTabs,
  };
}

function dispatch(listener, message, sender) {
  return new Promise((resolve, reject) => {
    try {
      const keepChannelOpen = listener(message, sender, (response) => resolve(response));
      if (!keepChannelOpen) {
        resolve(undefined);
      }
    } catch (error) {
      reject(error);
    }
  });
}

async function waitForTick() {
  await new Promise((resolve) => setImmediate(resolve));
}

async function runTest(name, fn) {
  try {
    await fn();
    console.log(`PASS ${name}`);
  } catch (error) {
    console.error(`FAIL ${name}`);
    console.error(error);
    process.exitCode = 1;
  }
}

runTest("background opens separate sockets for different page identities", async () => {
  const { listener, FakeWebSocket } = loadBackground();

  const responseA = await dispatch(
    listener,
    { type: "submit_comment", page_identity: "page-A", data: { comment: "A" } },
    { tab: { id: 1, url: "https://example.com/a" } }
  );
  await waitForTick();

  const responseB = await dispatch(
    listener,
    { type: "get_history", page_identity: "page-B", page_key: "page-B" },
    { tab: { id: 2, url: "https://example.com/b" } }
  );
  await waitForTick();

  assert.equal(responseA.ok, true);
  assert.equal(responseB.ok, true);
  assert.equal(FakeWebSocket.instances.length, 2);
});

runTest("background reuses socket for the same page identity", async () => {
  const { listener, FakeWebSocket } = loadBackground();

  await dispatch(
    listener,
    { type: "submit_comment", page_identity: "page-A", data: { comment: "A" } },
    { tab: { id: 1, url: "https://example.com/a" } }
  );
  await waitForTick();

  await dispatch(
    listener,
    { type: "get_history", page_identity: "page-A", page_key: "page-A" },
    { tab: { id: 2, url: "https://example.com/a" } }
  );
  await waitForTick();

  assert.equal(FakeWebSocket.instances.length, 1);
});

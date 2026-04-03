#!/usr/bin/env node
import fs from 'fs';

const MCP_BASE_URL = process.env.LARK_MCP_URL || 'http://10.0.1.167:3000';
const SSE_URL = `${MCP_BASE_URL}/sse`;
const TIMEOUT_MS = 120000;

async function connectSSE(signal) {
  return await new Promise((resolve, reject) => {
    const pendingResponses = new Map();

    fetch(SSE_URL, {
      signal,
      headers: { Accept: 'text/event-stream' },
    })
      .then((res) => {
        if (!res.ok) throw new Error(`SSE 连接失败: ${res.status}`);
        const reader = res.body.getReader();
        const decoder = new TextDecoder();
        let buffer = '';
        let endpointResolved = false;
        let currentEvent = '';

        function processLine(line) {
          if (line.startsWith('event:')) {
            currentEvent = line.slice(line.indexOf(':') + 1).trim();
          } else if (line.startsWith('data:')) {
            const data = line.slice(line.indexOf(':') + 1).trim();
            if (!endpointResolved && currentEvent === 'endpoint') {
              endpointResolved = true;
              const fullEndpoint = data.startsWith('http') ? data : `${MCP_BASE_URL}${data}`;
              resolve({
                messageEndpoint: fullEndpoint,
                waitForResponse: (id) => new Promise((res, rej) => pendingResponses.set(id, { resolve: res, reject: rej })),
              });
            } else if (currentEvent === 'message') {
              try {
                const msg = JSON.parse(data);
                if (msg.id !== undefined && pendingResponses.has(msg.id)) {
                  const handler = pendingResponses.get(msg.id);
                  pendingResponses.delete(msg.id);
                  if (msg.error) {
                    handler.reject(new Error(JSON.stringify(msg.error)));
                  } else {
                    handler.resolve(msg.result);
                  }
                }
              } catch {
                // 忽略非 JSON 消息
              }
            }
            currentEvent = '';
          } else if (line === '') {
            currentEvent = '';
          }
        }

        function readChunk() {
          reader.read().then(({ done, value }) => {
            if (done) return;
            buffer += decoder.decode(value, { stream: true });
            const lines = buffer.split('\n');
            buffer = lines.pop() || '';
            for (const line of lines) {
              processLine(line);
            }
            readChunk();
          }).catch((error) => {
            for (const [, handler] of pendingResponses) {
              handler.reject(error);
            }
          });
        }

        readChunk();
      })
      .catch((error) => {
        if (error.name !== 'AbortError') {
          reject(error);
        }
      });
  });
}

async function postJsonRpc(endpoint, method, params, id) {
  const res = await fetch(endpoint, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ jsonrpc: '2.0', id, method, params }),
  });
  if (!res.ok) {
    throw new Error(`POST 失败 (${res.status}): ${await res.text()}`);
  }
}

async function postNotification(endpoint, method, params) {
  await fetch(endpoint, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ jsonrpc: '2.0', method, params }),
  });
}

async function callTool(toolName, args) {
  const ac = new AbortController();
  const timer = setTimeout(() => ac.abort(), TIMEOUT_MS);
  try {
    const { messageEndpoint, waitForResponse } = await connectSSE(ac.signal);
    const initPromise = waitForResponse(1);
    await postJsonRpc(messageEndpoint, 'initialize', {
      protocolVersion: '2024-11-05',
      capabilities: {},
      clientInfo: { name: 'local-lark-doc-import', version: '1.0.0' },
    }, 1);
    await initPromise;
    await postNotification(messageEndpoint, 'notifications/initialized', {});
    const resultPromise = waitForResponse(3);
    await postJsonRpc(messageEndpoint, 'tools/call', {
      name: toolName,
      arguments: args,
    }, 3);
    return await resultPromise;
  } finally {
    clearTimeout(timer);
    ac.abort();
  }
}

async function main() {
  const mdPath = process.argv[2];
  const docName = process.argv[3];
  if (!mdPath || !docName) {
    console.error('用法: node lark_doc_import.mjs <markdown_path> <doc_name>');
    process.exit(1);
  }
  const markdown = fs.readFileSync(mdPath, 'utf8');
  const result = await callTool('docx_builtin_import', {
    data: { markdown, file_name: docName },
    useUAT: true,
  });
  console.log(JSON.stringify(result, null, 2));
}

main().catch((error) => {
  console.error(error?.stack || String(error));
  process.exit(1);
});

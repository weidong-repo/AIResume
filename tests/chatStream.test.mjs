import assert from 'node:assert/strict';
import { mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { pathToFileURL } from 'node:url';
import test, { after } from 'node:test';
import ts from 'typescript';

// 使用项目已有的 TypeScript 编译到临时目录，兼容 Node 18，无需额外测试依赖。
const temporaryDir = await mkdtemp(join(tmpdir(), 'airesume-stream-test-'));
after(() => rm(temporaryDir, { recursive: true, force: true }));
for (const name of ['chatStream', 'aiWorker']) {
  const source = (await readFile(new URL(`../src/worker/${name}.ts`, import.meta.url), 'utf8'))
    .replace("'./chatStream'", "'./chatStream.mjs'");
  const { outputText } = ts.transpileModule(source, {
    compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.ES2022 },
  });
  await writeFile(join(temporaryDir, `${name}.mjs`), outputText);
}
const { readChatCompletionStream } = await import(pathToFileURL(join(temporaryDir, 'chatStream.mjs')).href);

const encoder = new TextEncoder();
const delta = (content) => JSON.stringify({ choices: [{ delta: { content } }] });
// 模拟网络传输：每个数组元素都是一次读取到的数据块，不需要调用真实模型 API。
function stream(chunks) {
  return new ReadableStream({
    start(controller) {
      for (const chunk of chunks) controller.enqueue(chunk);
      controller.close();
    },
  });
}

test('preserves text for every possible byte boundary, including UTF-8 and [DONE]', async () => {
  const bytes = encoder.encode(`data: ${delta('你好👋')}\n\ndata: ${delta(' résumé 日本語')}\n\ndata: [DONE]\n\n`);
  // 遍历所有字节切分位置，覆盖 JSON、多字节字符和结束标记被拆开的情况。
  for (let cut = 1; cut < bytes.length; cut++) {
    const updates = [];
    const body = stream([bytes.slice(0, cut), bytes.slice(cut)]);
    assert.equal(await readChatCompletionStream(body, (text) => updates.push(text)), '你好👋 résumé 日本語');
    assert.deepEqual(updates, ['你好👋', '你好👋 résumé 日本語']);
    assert.equal(body.locked, false);
  }
});

test('handles one-byte chunks, CRLF, metadata, comments and data without a space', async () => {
  const bytes = encoder.encode(`: heartbeat\r\nevent: message\r\nid: 1\r\ndata:${delta('日本語')}\r\n\r\ndata: [DONE]\r\n\r\n`);
  assert.equal(await readChatCompletionStream(stream([...bytes].map((byte) => Uint8Array.of(byte))), () => {}), '日本語');
});

test('joins multiline data and accepts bare CR line endings', async () => {
  const body = stream([encoder.encode('data: {"choices":\rdata: [{"delta":{"content":"ok"}}]}\r\rdata: [DONE]\r\r')]);
  assert.equal(await readChatCompletionStream(body, () => {}), 'ok');
});

test('completes on EOF without [DONE] or a trailing newline', async () => {
  for (const ending of ['', '\n', '\n\n', '\r', '\r\n']) {
    const body = stream([encoder.encode(`data: ${delta('最后一段')}${ending}`)]);
    assert.equal(await readChatCompletionStream(body, () => {}), '最后一段');
    assert.equal(body.locked, false);
  }
});

test('ignores role and usage events without emitting extra content', async () => {
  const updates = [];
  const text = 'data: {"choices":[{"delta":{"role":"assistant"}}]}\n\n'
    + `data: ${delta('answer')}\n\ndata: {"choices":[],"usage":{"total_tokens":5}}\n\ndata: [DONE]\n\n`;
  assert.equal(await readChatCompletionStream(stream([encoder.encode(text)]), (text) => updates.push(text)), 'answer');
  assert.deepEqual(updates, ['answer']);
});

test('stops at [DONE], cancels the stream and releases its reader', async () => {
  let cancelled = false;
  const body = new ReadableStream({
    start(controller) { controller.enqueue(encoder.encode(`data: ${delta('ok')}\n\ndata: [DONE]\n\ndata: invalid\n\n`)); },
    cancel() { cancelled = true; },
  });
  assert.equal(await readChatCompletionStream(body, () => {}), 'ok');
  assert.equal(cancelled, true);
  assert.equal(body.locked, false);
});

test('rejects malformed complete events and releases the reader', async () => {
  const body = stream([encoder.encode('data: {invalid}\n\n')]);
  await assert.rejects(readChatCompletionStream(body, () => {}), SyntaxError);
  assert.equal(body.locked, false);
});

test('propagates network errors and releases the reader', async () => {
  const body = new ReadableStream({ start(controller) { controller.error(new Error('disconnected')); } });
  await assert.rejects(readChatCompletionStream(body, () => {}), /disconnected/);
  assert.equal(body.locked, false);
});

test('worker sends exactly one terminal response for EOF and errors', async () => {
  // 模拟 worker 环境和请求结果，确认正常结束与异常都能通知线程池释放任务。
  const originalSelf = globalThis.self;
  const originalFetch = globalThis.fetch;
  const responses = [];
  globalThis.self = { postMessage: (message) => responses.push(message) };
  try {
    await import(pathToFileURL(join(temporaryDir, 'aiWorker.mjs')).href);
    const cases = [
      [() => new Response(`data: ${delta('ok')}`, { status: 200 }), 'ok'],
      [() => new Response('data: invalid\n\n'), '解析流数据时出错，请稍后重试'],
      [() => { throw new Error('offline'); }, '请求失败，请稍后重试'],
      [() => new Response('', { status: 401 }), '认证失败，请检查 API Key 是否正确'],
      [() => new Response('', { status: 500 }), '请求失败，错误码: 500'],
      [() => new Response(null), '服务器未返回流数据'],
    ];
    for (const [response, expected] of cases) {
      responses.length = 0;
      globalThis.fetch = async () => response();
      await globalThis.self.onmessage({ data: { taskId: 7, messages: [], userApiKey: 'test', model: 'test', API_URL: 'https://example.invalid' } });
      const terminal = responses.filter((message) => message.isComplete);
      assert.deepEqual(terminal, [{ taskId: 7, isComplete: true, result: expected }]);
    }
  } finally {
    // 恢复全局对象，避免模拟环境影响后续测试。
    globalThis.self = originalSelf;
    globalThis.fetch = originalFetch;
  }
});

/**
 * 读取 OpenAI 兼容接口的 SSE 回复。
 * 网络数据块不一定包含完整字符、完整行或完整事件，因此分层缓存后再解析。
 * onContent 接收累计文本，返回值是最终全文，与界面原有的更新方式保持一致。
 *
 * 数据经过三层处理：字节 → UTF-8 文本 → SSE 行与事件 → JSON 中的增量文字。
 * reader.read() 的一次结果只是传输块，不能直接当作一条 JSON 消息。
 * @param body fetch 返回的响应流，元素是字节数组。
 * @param onContent 每次得到非空增量文字时调用；例如依次收到“你好”和“你好，世界”。
 * @returns 收到 [DONE] 或流自然结束后的累计全文。
 * @throws 完整事件中的 JSON 无效、网络读取失败或回调执行失败时向调用方抛出错误。
 */
export async function readChatCompletionStream(
  body: ReadableStream<Uint8Array>,
  onContent: (text: string) => void,
): Promise<string> {
  const reader = body.getReader();
  const decoder = new TextDecoder();
  let buffer = ''; // 尚未处理的文本；不完整的行留到下一次读取。
  let data: string[] = []; // 当前 SSE 事件中的 data 行，空行到达后统一解析。
  let currentText = ''; // 已解析的正文全文，每次在末尾追加 delta.content。
  let complete = false; // 协议结束标记 [DONE]；与底层连接是否到达 EOF 分开记录。

  // 一条事件可能有多行 data，按 SSE 格式用换行拼接成一个载荷。
  const dispatch = () => {
    // 连续空行或纯心跳不包含 data，不应该进入 JSON.parse。
    if (data.length === 0) return;
    const payload = data.join('\n');
    data = [];
    // 先清空当前事件缓存，防止下一条事件混入上一条的数据。
    if (payload.trim() === '[DONE]') {
      complete = true;
      return;
    }
    // 此时载荷已完整；无效 JSON 向上传递，让 worker 结束任务并显示错误。
    const parsed = JSON.parse(payload);
    const { content } = parsed?.choices?.[0]?.delta ?? {};
    // ?. 允许角色声明、usage 等没有正文的事件；?? {} 让解构始终有对象可用。
    // API 响应是外部数据，仍需检查 content 类型，不能只依赖 TypeScript。
    if (typeof content !== 'string' || !content) return;
    currentText += content;
    onContent(currentText);
  };

  const processLine = (line: string) => {
    // SSE 用空行分隔事件。保留空行才能判断何时把多个 data 行组成完整载荷。
    if (!line) return dispatch();
    // 只匹配 data 字段，保留载荷中的冒号；移除冒号后一个可选空格。
    // 没有冒号的裸 data 字段按 SSE 格式表示空值，其他元信息和心跳直接忽略。
    const match = /^data(?:: ?(.*))?$/.exec(line);
    if (!match) return;
    const [, value = ''] = match;
    // match[0] 是整行，match[1] 是捕获的载荷；解构跳过整行并给裸 data 默认空值。
    data.push(value);
  };

  // 消费缓存中的完整行；只有连接结束时，才处理没有换行符的最后一行。
  const drain = (atEnd = false) => {
    while (!complete) {
      const match = /\r\n|[\r\n]/.exec(buffer);
      if (!match) break;
      const { index, 0: separator } = match;
      // index 是换行的位置，match[0] 是实际分隔符；CRLF 的长度为 2，其余为 1。
      // CRLF 也可能跨数据块；末尾只有 CR 时先等待，避免把随后到达的 LF 当成空行。
      if (!atEnd && separator === '\r' && index === buffer.length - 1) break;
      const line = buffer.slice(0, index);
      buffer = buffer.slice(index + separator.length);
      // 只移除这条完整行及其换行符，剩余半行继续留在 buffer 中。
      processLine(line);
    }
    if (atEnd && !complete) {
      if (buffer) processLine(buffer);
      buffer = '';
      // 兼容接口可能没有末尾空行或 [DONE]；在 EOF 时处理剩余事件并结束。
      dispatch();
    }
  };

  let ended = false; // reader 是否自然读到 EOF；用于判断 finally 中是否还要取消流。
  try {
    while (!complete) {
      const { done, value } = await reader.read();
      ended = done;
      // 读取中保留不完整 UTF-8 字节；EOF 时关闭流模式，刷新解码器剩余字节。
      buffer += decoder.decode(value, { stream: !done });
      drain(done);
      // done=true 时 value 通常是 undefined，decode 会刷新字节缓存，drain 会收尾。
      if (done) break;
    }
    return currentText;
  } finally {
    // 无论成功、JSON 异常、网络异常还是回调异常，都执行资源清理。
    try {
      // 收到 [DONE] 或解析失败时主动取消；已自然到达 EOF 则无需取消。
      if (!ended) await reader.cancel();
    } finally {
      reader.releaseLock(); // 取消操作即使失败，也必须释放读取锁。
    }
  }
}

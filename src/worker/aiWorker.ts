import { readChatCompletionStream } from './chatStream';

// worker 负责 HTTP 请求与消息通知；SSE 的字节、行和事件解析交给 chatStream。
self.onmessage = async (event) => {
  const { taskId, messages, userApiKey, model, API_URL } = event.data;
  // taskId 把回复关联到原任务；isComplete 告知线程池是否可以回收 worker。
  // 默认发送结束通知；流式更新显式传 false，避免错误分支遗漏任务结束状态。
  const send = (result: string, isComplete = true) =>
    self.postMessage({ taskId, isComplete, result });
  const requestData = {
    model,
    messages,
    stream: true, // 流式响应
  };
  try {
    const response = await fetch(API_URL, {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${userApiKey}`, // 认证信息
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(requestData),
    });
    if (response.status === 401) return send('认证失败，请检查 API Key 是否正确');
    if (!response.ok) return send(`请求失败，错误码: ${response.status}`);
    if (!response.body) return send('服务器未返回流数据');

    // onContent 回调收到累计全文后，发出未完成通知，让页面持续更新回复。
    // await 的返回值只在整个解析过程结束后得到，再发送一次完成通知。
    const currentText = await readChatCompletionStream(response.body, (result) => send(result, false));
    send(currentText);
  } catch (error) {
    // 解析失败和请求失败保留不同提示；两种情况都结束任务，避免一直处于生成中。
    const result = error instanceof SyntaxError
      ? '解析流数据时出错，请稍后重试'
      : '请求失败，请稍后重试';
    send(result);
  }
};

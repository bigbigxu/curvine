export default {
  civi: {
    /** Floating action button + drawer header. */
    fab: 'civi — Curvine AI 助手',
    title: 'civi',
    subtitle: 'Curvine AI 助手',
    settings: '设置',
    close: '关闭',
    newChat: '新会话',
    history: '历史会话',
    delete: '删除',
    emptyHistory: '暂无历史会话',

    /** Disabled state hint above the chat area. */
    disabledNotice: 'CIVI 当前已禁用。请打开设置，配置并启用模型端点。',

    /** Suggestion chips shown before the first message. */
    suggestTitle: '自助分析：',
    askTitle: '或者试试这些问题：',
    run: '运行',

    /** Composer. */
    placeholder: '询问集群相关问题…（回车发送）',
    send: '发送',
    stop: '停止',
    thinking: '思考中…',

    /** Tool step visualisation. */
    toolStep: '工具',

    /** Write-tool confirmation cards. */
    confirmTitle: '写操作需要你的确认',
    approve: '批准',
    reject: '拒绝',
    approved: '已批准',
    rejected: '已拒绝',

    /** Settings panel. */
    enabled: '启用',
    enabledOn: '已启用',
    enabledOff: '已禁用',
    endpoint: '端点（OpenAI 兼容）',
    apiKey: 'API Key',
    apiKeySavedHint: '（已保存，输入 *** 保持不变）',
    model: '模型',
    temperature: '温度',
    maxIterations: '最大工具迭代次数',
    rateLimit: '限流（次/分钟）',
    apiToken: 'API Token（可选）',
    apiTokenHint: '设置后，chat / settings / MCP 请求都需要携带 X-CIVI-Token 请求头。',
    allowedTools: '允许的工具',
    needsConfirmation: '（需要用户确认）',
    mcpServers: '外部 MCP 服务器',
    mcpNamePlaceholder: '名称',
    mcpUrlPlaceholder: 'http://host/mcp',
    mcpAdd: '+ 添加 MCP 服务器',
    save: '保存',
    saving: '保存中…',
    saveFailed: '保存失败：{message}',
    unauthorizedHint: '未授权：请先设置 API Token（服务端拒绝无 Token 的写入）',

    /** Errors. */
    httpError: 'HTTP {status}',
    tokenHint: '（请在设置中配置 API Token）'
  }
}

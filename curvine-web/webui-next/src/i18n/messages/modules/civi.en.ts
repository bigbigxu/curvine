export default {
  civi: {
    /** Floating action button + drawer header. */
    fab: 'civi — Curvine AI assistant',
    title: 'civi',
    subtitle: 'Curvine AI Assistant',
    settings: 'Settings',
    close: 'Close',
    newChat: 'New chat',
    history: 'History',
    delete: 'Delete',
    emptyHistory: 'No past conversations yet',

    /** Disabled state hint above the chat area. */
    disabledNotice:
      'CIVI is currently disabled. Open the settings to configure and enable the model endpoint.',

    /** Suggestion chips shown before the first message. */
    suggestTitle: 'Self-service analysis:',
    askTitle: 'Or try asking:',
    run: 'Run',

    /** Composer. */
    placeholder: 'Ask about the cluster… (Enter to send)',
    send: 'Send',
    stop: 'Stop',
    thinking: 'thinking…',

    /** Tool step visualisation. */
    toolStep: 'Tool',

    /** Write-tool confirmation cards. */
    confirmTitle: 'Write action requires your confirmation',
    approve: 'Approve',
    reject: 'Reject',
    approveAll: 'Approve all (this session)',
    approveAllHint: 'Grants this tool for the rest of the conversation; write actions no longer pause for confirmation.',
    approved: 'Approved',
    rejected: 'Rejected',

    /** Settings panel. */
    enabled: 'Enabled',
    enabledOn: 'Enabled',
    enabledOff: 'Disabled',
    endpoint: 'Endpoint (OpenAI compatible)',
    apiKey: 'API key',
    apiKeySavedHint: '(saved, *** to keep)',
    model: 'Model',
    temperature: 'Temperature',
    maxIterations: 'Max tool iterations',
    rateLimit: 'Rate limit (req/min)',
    apiToken: 'API token (optional)',
    apiTokenHint: 'When set, chat / settings / MCP require the X-CIVI-Token header.',
    allowedTools: 'Allowed tools',
    needsConfirmation: '(needs user confirmation)',
    mcpServers: 'External MCP servers',
    mcpNamePlaceholder: 'name',
    mcpUrlPlaceholder: 'http://host/mcp',
    mcpAdd: '+ Add MCP server',
    save: 'Save',
    saving: 'Saving…',
    saveFailed: 'Save failed: {message}',
    unauthorizedHint:
      'unauthorized: set the API token first (server rejects writes without it)',

    /** Errors. */
    httpError: 'HTTP {status}',
    tokenHint: ' (set the API token in settings)'
  }
}

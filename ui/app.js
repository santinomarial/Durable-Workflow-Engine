"use strict";

const POLL_INTERVAL_MS = 5000;
const TERMINAL_STATUSES = new Set(["completed", "failed", "terminated"]);
const VALID_STATUSES = new Set(["", "running", "completed", "failed", "terminated", "attention"]);
const VALID_TABS = new Set(["overview", "history", "graph", "debugger", "raw"]);

const state = {
  executions: [],
  stats: null,
  selected: null,
  execution: null,
  history: [],
  updates: [],
  continuationChain: [],
  debugTrace: null,
  debugIndex: 0,
  debugTimer: null,
  historyTruncated: false,
  statusFilter: "",
  search: "",
  searchTimer: null,
  eventFilter: "",
  historySearch: "",
  activeTab: "overview",
  live: true,
  refreshTimer: null,
  detailGeneration: 0,
  networkRequests: 0,
  initialized: false,
  confirmAction: null,
  authToken: null,
  principal: null,
  deadLetters: [],
  schedules: [],
};

const $ = (id) => document.getElementById(id);

function element(tag, options = {}, children = []) {
  const node = document.createElement(tag);
  if (options.className) node.className = options.className;
  if (options.text !== undefined) node.textContent = String(options.text);
  if (options.attrs) {
    for (const [name, value] of Object.entries(options.attrs)) {
      if (value !== null && value !== undefined) node.setAttribute(name, String(value));
    }
  }
  const items = Array.isArray(children) ? children : [children];
  for (const child of items) {
    if (child !== null && child !== undefined) {
      node.append(child instanceof Node ? child : document.createTextNode(String(child)));
    }
  }
  return node;
}

function beginNetwork(quiet) {
  if (quiet) return;
  state.networkRequests += 1;
  $("global-progress").hidden = false;
}

function endNetwork(quiet) {
  if (quiet) return;
  state.networkRequests = Math.max(0, state.networkRequests - 1);
  $("global-progress").hidden = state.networkRequests === 0;
}

async function api(path, options = {}) {
  const { quiet = false, ...fetchOptions } = options;
  const headers = new Headers(fetchOptions.headers || {});
  headers.set("Accept", "application/json");
  if (state.authToken) headers.set("Authorization", `Bearer ${state.authToken}`);
  if (fetchOptions.body !== undefined) headers.set("Content-Type", "application/json");
  beginNetwork(quiet);
  try {
    const response = await fetch(path, { ...fetchOptions, headers });
    if (!response.ok) {
      let message = response.statusText || `Request failed (${response.status})`;
      try {
        const body = await response.json();
        if (body.detail) message = typeof body.detail === "string" ? body.detail : JSON.stringify(body.detail);
      } catch {
        // The HTTP status remains useful when an upstream returns a non-JSON error.
      }
      const error = new Error(message);
      error.status = response.status;
      error.requestId = response.headers.get("X-Request-ID");
      throw error;
    }
    if (response.status === 204) return null;
    return response.json();
  } catch (error) {
    if (error instanceof TypeError) throw new Error("Unable to reach the workflow engine");
    throw error;
  } finally {
    endNetwork(quiet);
  }
}

function safeStorageGet(key) {
  try { return localStorage.getItem(key); } catch { return null; }
}

function safeStorageSet(key, value) {
  try { localStorage.setItem(key, value); } catch { /* Preferences are optional. */ }
}

function safeSessionGet(key) {
  try { return sessionStorage.getItem(key); } catch { return null; }
}

function safeSessionSet(key, value) {
  try {
    if (value === null) sessionStorage.removeItem(key);
    else sessionStorage.setItem(key, value);
  } catch { /* Session persistence is optional. */ }
}

function roleAtLeast(minimum) {
  const levels = { viewer: 10, operator: 20, admin: 30 };
  return Boolean(state.principal && levels[state.principal.role] >= levels[minimum]);
}

function formatDate(value, withSeconds = false) {
  if (!value) return "—";
  const date = new Date(value);
  if (Number.isNaN(date.valueOf())) return String(value);
  return new Intl.DateTimeFormat(undefined, {
    dateStyle: "medium",
    timeStyle: withSeconds ? "medium" : "short",
  }).format(date);
}

function relativeTime(value) {
  if (!value) return "—";
  const timestamp = new Date(value).valueOf();
  if (Number.isNaN(timestamp)) return "—";
  const seconds = Math.round((timestamp - Date.now()) / 1000);
  const ranges = [
    [60, "second"],
    [60, "minute"],
    [24, "hour"],
    [7, "day"],
    [4.345, "week"],
    [12, "month"],
    [Number.POSITIVE_INFINITY, "year"],
  ];
  let valueInUnit = seconds;
  for (const [range, unit] of ranges) {
    if (Math.abs(valueInUnit) < range) {
      return new Intl.RelativeTimeFormat(undefined, { numeric: "auto" }).format(Math.round(valueInUnit), unit);
    }
    valueInUnit /= range;
  }
  return "—";
}

function formatDuration(from, to) {
  if (!from || !to) return "—";
  const milliseconds = Math.max(0, new Date(to).valueOf() - new Date(from).valueOf());
  if (!Number.isFinite(milliseconds)) return "—";
  if (milliseconds < 1000) return `${milliseconds} ms`;
  if (milliseconds < 60000) return `${(milliseconds / 1000).toFixed(milliseconds < 10000 ? 1 : 0)} s`;
  if (milliseconds < 3600000) return `${Math.round(milliseconds / 60000)} min`;
  return `${(milliseconds / 3600000).toFixed(1)} hr`;
}

function humanize(value) {
  return String(value || "Unknown")
    .replace(/([a-z0-9])([A-Z])/g, "$1 $2")
    .replace(/[_-]+/g, " ")
    .replace(/^./, (character) => character.toUpperCase());
}

function shortId(value) {
  if (!value) return "—";
  const text = String(value);
  return text.length > 15 ? `${text.slice(0, 8)}…${text.slice(-5)}` : text;
}

function conciseValue(value, limit = 110) {
  const rendered = typeof value === "string" ? value : JSON.stringify(value);
  if (rendered === undefined) return "—";
  return rendered.length > limit ? `${rendered.slice(0, limit)}…` : rendered;
}

function statusBadge(status) {
  return element("span", {
    className: `status-badge status-${status}`,
    text: humanize(status),
  });
}

function updateURL() {
  const url = new URL(window.location.href);
  if (state.selected) url.searchParams.set("workflow", state.selected);
  else url.searchParams.delete("workflow");
  if (state.statusFilter) url.searchParams.set("status", state.statusFilter);
  else url.searchParams.delete("status");
  if (state.activeTab !== "overview") url.searchParams.set("tab", state.activeTab);
  else url.searchParams.delete("tab");
  history.replaceState(null, "", url);
}

function readURL() {
  const params = new URLSearchParams(window.location.search);
  const status = params.get("status") || "";
  const tab = params.get("tab") || "overview";
  state.statusFilter = VALID_STATUSES.has(status) ? status : "";
  state.activeTab = VALID_TABS.has(tab) ? tab : "overview";
  state.selected = params.get("workflow");
  $("status-filter").value = state.statusFilter;
}

function setHealth(healthy, message) {
  const pill = $("health-pill");
  pill.classList.remove("is-pending", "is-healthy", "is-error");
  pill.classList.add(healthy ? "is-healthy" : "is-error");
  $("health-label").textContent = message;
}

function renderStats() {
  const stats = state.stats;
  if (!stats) return;
  $("stat-total").textContent = stats.total.toLocaleString();
  $("stat-running").textContent = stats.running.toLocaleString();
  $("stat-completed").textContent = stats.completed.toLocaleString();
  $("stat-attention").textContent = (stats.failed + stats.terminated).toLocaleString();
  $("stat-attention-detail").textContent = `${stats.failed.toLocaleString()} failed · ${stats.terminated.toLocaleString()} terminated`;
  document.querySelectorAll("[data-status-filter]").forEach((card) => {
    card.classList.toggle("is-active", card.dataset.statusFilter === state.statusFilter);
  });
}

function matchesStatus(execution) {
  if (!state.statusFilter) return true;
  if (state.statusFilter === "attention") return execution.status === "failed" || execution.status === "terminated";
  return execution.status === state.statusFilter;
}

function filteredExecutions() {
  const query = state.search.trim().toLowerCase();
  return state.executions.filter((execution) => {
    if (!matchesStatus(execution)) return false;
    if (!query) return true;
    return [execution.id, execution.workflow_type, execution.queue_name, execution.status, JSON.stringify(execution.search_attributes || {})]
      .some((value) => String(value).toLowerCase().includes(query));
  });
}

function showListState(title, description, retry = false) {
  const container = $("list-state");
  container.replaceChildren(
    element("strong", { text: title }),
    element("span", { text: description }),
  );
  if (retry) {
    const button = element("button", { className: "button button-secondary", text: "Try again", attrs: { type: "button" } });
    button.addEventListener("click", () => loadDashboard());
    container.append(button);
  }
  container.hidden = false;
}

function renderExecutionList() {
  const list = $("execution-list");
  const executions = filteredExecutions();
  list.replaceChildren();
  $("list-state").hidden = true;
  $("execution-count").textContent = `${executions.length.toLocaleString()} of ${state.executions.length.toLocaleString()} loaded`;

  if (!executions.length) {
    const hasFilter = Boolean(state.search || state.statusFilter);
    showListState(
      hasFilter ? "No matching executions" : "No workflows yet",
      hasFilter ? "Change the search or status filter." : "Start a workflow to create the first durable execution.",
    );
    return;
  }

  for (const execution of executions) {
    const selected = execution.id === state.selected;
    const button = element("button", {
      className: "execution-item",
      attrs: {
        type: "button",
        role: "option",
        "aria-selected": selected,
        "data-execution-id": execution.id,
        tabindex: selected ? "0" : "-1",
      },
    }, [
      element("span", { className: "execution-item-header" }, [
        element("strong", { text: execution.workflow_type }),
        statusBadge(execution.paused_at ? "paused" : execution.status),
      ]),
      element("code", { className: "execution-id", text: execution.id }),
      element("span", { className: "execution-meta" }, [
        element("span", { text: `v${execution.definition_version} · ${execution.queue_name}` }),
        element("time", { text: relativeTime(execution.created_at), attrs: { datetime: execution.created_at, title: formatDate(execution.created_at, true) } }),
      ]),
    ]);
    button.addEventListener("click", () => selectExecution(execution.id));
    list.append(button);
  }
}

function renderMetadata(execution) {
  const items = [
    ["Created", formatDate(execution.created_at, true), execution.created_at],
    ["Closed", execution.closed_at ? formatDate(execution.closed_at, true) : "Still open", execution.closed_at],
    ["Task queue", execution.queue_name],
    ["History loaded", `${state.history.length.toLocaleString()}${state.historyTruncated ? "+" : ""} events`],
  ];
  if (execution.parent_workflow_id) items.push(["Parent workflow", shortId(execution.parent_workflow_id)]);
  if (execution.continued_from) items.push(["Continued from", shortId(execution.continued_from)]);
  if (execution.continued_to) items.push(["Continued to", shortId(execution.continued_to)]);
  if (execution.schedule_id) items.push(["Schedule", shortId(execution.schedule_id)]);
  if (execution.scheduled_at) items.push(["Scheduled for", formatDate(execution.scheduled_at, true), execution.scheduled_at]);
  $("metadata").replaceChildren(...items.map(([label, value, dateValue]) => {
    const dd = element("dd", { text: value, attrs: { title: value } });
    if (dateValue) dd.setAttribute("data-date", dateValue);
    return element("div", {}, [element("dt", { text: label }), dd]);
  }));
  const attributes = Object.entries(execution.search_attributes || {}).sort(([left], [right]) => left.localeCompare(right));
  $("search-attributes").replaceChildren(...attributes.map(([key, value]) => element("span", {
    className: "attribute-chip",
    attrs: { title: `${key} = ${JSON.stringify(value)}` },
  }, [element("strong", { text: key }), element("span", { text: conciseValue(value, 70) })])));
}

function renderContinuationChain(chain) {
  const container = $("continuation-chain");
  container.hidden = chain.length < 2;
  container.replaceChildren();
  if (chain.length < 2) return;
  container.append(element("span", { className: "chain-label", text: "Run chain" }));
  chain.forEach((run, index) => {
    const button = element("button", {
      className: `chain-run${run.id === state.selected ? " is-current" : ""}`,
      text: `${index + 1} · v${run.definition_version}`,
      attrs: { type: "button", title: `${run.id} · ${run.status}` },
    });
    button.addEventListener("click", () => selectExecution(run.id));
    container.append(button);
    if (index < chain.length - 1) container.append(element("span", { className: "chain-arrow", text: "→" }));
  });
}

function relatedTerminal(history, scheduledEvent, terminalTypes) {
  return history.find((event) => event.seq > scheduledEvent.seq
    && event.entity_id === scheduledEvent.entity_id
    && terminalTypes.includes(event.event_type));
}

function deriveOperationalState(execution, history) {
  if (execution.status === "completed") {
    return { label: "Terminal state", title: "Workflow completed", detail: `Result: ${conciseValue(execution.result)}` };
  }
  if (execution.status === "failed") {
    return { label: "Needs attention", title: "Workflow failed", detail: `Failure: ${conciseValue(execution.failure)}` };
  }
  if (execution.status === "terminated") {
    const event = [...history].reverse().find((item) => item.event_type === "WorkflowExecutionTerminated");
    return { label: "Terminal state", title: "Workflow terminated", detail: event?.attributes?.reason || "Terminated by an operator." };
  }
  if (execution.paused_at) {
    return { label: "Dispatch frozen", title: "Workflow paused", detail: execution.pause_reason || "Pending work and deadlines will resume on operator request." };
  }
  if (execution.cancellation_requested_at) {
    return { label: "Control request", title: "Cancellation requested", detail: execution.cancellation_reason || "Waiting for the workflow to observe cancellation." };
  }

  const pendingChild = [...history].reverse().find((event) => event.event_type === "ChildWorkflowStarted"
    && !relatedTerminal(history, event, ["ChildWorkflowCompleted", "ChildWorkflowFailed", "ChildWorkflowTerminated"]));
  if (pendingChild) {
    return { label: "Workflow composition", title: `Waiting for child ${pendingChild.attributes?.workflow_type || "workflow"}`, detail: `Child execution ${shortId(pendingChild.entity_id)} is durably linked to this parent.` };
  }

  const last = history.at(-1);
  if (!last) return { label: "Current state", title: "Starting execution", detail: "Waiting for the first durable transition." };
  if (last.event_type === "ActivityFailed" && last.attributes?.final === false) {
    return {
      label: "Retry policy",
      title: `Retrying activity after attempt ${last.attributes.attempt}`,
      detail: last.attributes.next_visible_at ? `Next attempt becomes eligible ${relativeTime(last.attributes.next_visible_at)}.` : "A retry task is pending.",
    };
  }
  const pendingActivity = [...history].reverse().find((event) => event.event_type === "ActivityScheduled"
    && !relatedTerminal(history, event, ["ActivityCompleted", "ActivityFailed", "ActivityTimedOut"]));
  if (pendingActivity) {
    return { label: "Current state", title: `Waiting for ${pendingActivity.attributes?.activity_type || "activity"}`, detail: `Activity ${shortId(pendingActivity.entity_id)} is scheduled on ${execution.queue_name}.` };
  }
  const pendingTimer = [...history].reverse().find((event) => event.event_type === "TimerStarted"
    && !relatedTerminal(history, event, ["TimerFired", "TimerCanceled"]));
  if (pendingTimer) {
    const purpose = pendingTimer.attributes?.purpose;
    const signal = pendingTimer.attributes?.signal_name;
    return {
      label: purpose === "signal-timeout" ? "Signal wait" : "Durable timer",
      title: signal ? `Waiting for “${signal}” or timeout` : "Waiting for timer",
      detail: `${pendingTimer.attributes?.delay_seconds ?? "Configured"} second durable delay · ${shortId(pendingTimer.entity_id)}`,
    };
  }
  if (last.event_type === "SignalReceived") {
    return { label: "External input", title: `Signal “${last.attributes?.name || "unknown"}” received`, detail: "The execution is ready for deterministic replay." };
  }
  return { label: "Current state", title: "Processing workflow task", detail: `${humanize(last.event_type)} is the latest persisted event.` };
}

function renderOperationalState(execution, history) {
  const current = deriveOperationalState(execution, history);
  $("operational-state-label").textContent = current.label;
  $("operational-state-title").textContent = current.title;
  $("operational-state-detail").textContent = current.detail;
}

function activityRows(history) {
  const scheduled = new Map();
  const rows = [];
  for (const event of history) {
    if (event.event_type === "ActivityScheduled") {
      scheduled.set(event.entity_id, event);
      continue;
    }
    if (!["ActivityCompleted", "ActivityFailed", "ActivityTimedOut"].includes(event.event_type)) continue;
    const schedule = scheduled.get(event.entity_id);
    const isRetry = event.event_type === "ActivityFailed" && event.attributes?.final === false;
    const outcome = event.event_type === "ActivityCompleted" ? "completed"
      : isRetry ? "retrying"
        : event.event_type === "ActivityTimedOut" ? "timed-out" : "failed";
    rows.push({
      entityId: event.entity_id,
      activityType: schedule?.attributes?.activity_type || "Unknown activity",
      attempt: event.attributes?.attempt || 1,
      outcome,
      duration: formatDuration(schedule?.created_at, event.created_at),
      detail: event.attributes?.failure ?? event.attributes?.result ?? event.attributes?.timeout_type ?? "—",
      event,
    });
  }
  for (const [entityId, schedule] of scheduled) {
    if (!rows.some((row) => row.entityId === entityId)) {
      rows.push({
        entityId,
        activityType: schedule.attributes?.activity_type || "Unknown activity",
        attempt: 1,
        outcome: "pending",
        duration: formatDuration(schedule.created_at, new Date().toISOString()),
        detail: "Waiting for a worker outcome",
        event: schedule,
      });
    }
  }
  return rows.sort((a, b) => a.event.seq - b.event.seq);
}

function renderActivities(history) {
  const rows = activityRows(history);
  const body = $("activity-attempts");
  body.replaceChildren();
  $("activity-empty").hidden = rows.length > 0;
  document.querySelector(".table-scroller").hidden = rows.length === 0;
  const uniqueActivities = new Set(rows.map((row) => row.entityId)).size;
  const retries = rows.filter((row) => row.attempt > 1 || row.outcome === "retrying").length;
  $("activity-summary").replaceChildren(
    element("span", {}, [element("strong", { text: uniqueActivities }), ` ${uniqueActivities === 1 ? "activity" : "activities"}`]),
    element("span", {}, [element("strong", { text: rows.length }), ` ${rows.length === 1 ? "attempt" : "attempts"}`]),
    element("span", {}, [element("strong", { text: retries }), " retry events"]),
  );
  for (const row of rows) {
    const outcomeClass = row.outcome;
    body.append(element("tr", {}, [
      element("td", {}, [row.activityType, element("code", { text: ` ${shortId(row.entityId)}` })]),
      element("td", { text: row.attempt }),
      element("td", {}, element("span", { className: `outcome ${outcomeClass}`, text: humanize(row.outcome) })),
      element("td", { text: row.duration }),
      element("td", { text: conciseValue(row.detail, 150), attrs: { title: conciseValue(row.detail, 1000) } }),
    ]));
  }
}

function eventGlyph(eventType) {
  if (eventType.includes("Activity")) return "A";
  if (eventType.includes("Timer")) return "T";
  if (eventType.includes("Signal")) return "S";
  if (eventType.includes("Marker")) return "M";
  return "W";
}

function eventSubtitle(event) {
  const attributes = event.attributes || {};
  return attributes.activity_type || attributes.name || attributes.marker_type
    || (event.entity_id ? shortId(event.entity_id) : event.external_id ? shortId(event.external_id) : "Durable transition");
}

function renderEventFilters(history) {
  const select = $("event-filter");
  const types = [...new Set(history.map((event) => event.event_type))].sort();
  select.replaceChildren(element("option", { text: "All event types", attrs: { value: "" } }));
  for (const type of types) select.append(element("option", { text: humanize(type), attrs: { value: type } }));
  if (types.includes(state.eventFilter)) select.value = state.eventFilter;
  else state.eventFilter = "";
}

function renderHistory() {
  const query = state.historySearch.trim().toLowerCase();
  const events = state.history.filter((event) => {
    if (state.eventFilter && event.event_type !== state.eventFilter) return false;
    if (!query) return true;
    return `${event.event_type} ${event.entity_id || ""} ${event.external_id || ""} ${JSON.stringify(event.attributes)}`.toLowerCase().includes(query);
  });
  const list = $("history");
  list.replaceChildren();
  $("history-results").textContent = state.historyTruncated
    ? `${events.length.toLocaleString()} matching loaded events · middle history omitted for safety`
    : `${events.length.toLocaleString()} of ${state.history.length.toLocaleString()} events`;
  $("history-empty").hidden = events.length > 0;

  for (const event of events) {
    const details = element("dl", { className: "event-details" });
    const baseFields = [
      ["Entity ID", event.entity_id],
      ["External ID", event.external_id],
      ["Command ID", event.command_id],
    ];
    for (const [key, value] of [...baseFields, ...Object.entries(event.attributes || {})]) {
      if (value === null || value === undefined) continue;
      details.append(element("div", { className: "event-detail" }, [
        element("dt", { text: humanize(key) }),
        element("dd", { text: typeof value === "string" ? value : JSON.stringify(value, null, 2) }),
      ]));
    }
    if (!details.children.length) {
      details.append(element("div", { className: "event-detail" }, [
        element("dt", { text: "Attributes" }), element("dd", { text: "No additional attributes" }),
      ]));
    }
    const summary = element("summary", {}, [
      element("span", { className: "event-title" }, [
        element("span", { className: "event-glyph", text: eventGlyph(event.event_type), attrs: { "aria-hidden": "true" } }),
        element("span", {}, [
          element("strong", { text: humanize(event.event_type) }),
          element("small", { text: eventSubtitle(event) }),
        ]),
      ]),
      element("time", { className: "event-time", text: formatDate(event.created_at, true), attrs: { datetime: event.created_at } }),
    ]);
    list.append(element("li", { className: "history-event" }, [
      element("span", { className: "event-seq", text: `#${event.seq}` }),
      element("details", { className: "event-card" }, [summary, details]),
    ]));
  }
}

function graphDescription(event) {
  const attributes = event.attributes || {};
  switch (event.event_type) {
    case "WorkflowExecutionStarted": return `${attributes.workflow_type || state.execution?.workflow_type} v${attributes.definition_version || state.execution?.definition_version}`;
    case "ActivityScheduled": return attributes.activity_type || "Activity command";
    case "TimerStarted": return attributes.signal_name ? `Signal timeout: ${attributes.signal_name}` : `${attributes.delay_seconds ?? "?"} second delay`;
    case "MarkerRecorded": return attributes.marker_type || "Deterministic value";
    case "SignalReceived": return attributes.name || "External signal";
    case "WorkflowCancellationRequested": return attributes.reason || "Operator request";
    case "WorkflowExecutionCompleted": return "Result persisted";
    case "WorkflowExecutionFailed": return "Failure persisted";
    case "WorkflowExecutionTerminated": return attributes.reason || "Operator termination";
    default: return eventSubtitle(event);
  }
}

function renderGraph(history) {
  const visibleTypes = new Set([
    "WorkflowExecutionStarted", "ActivityScheduled", "TimerStarted", "MarkerRecorded",
    "SignalReceived", "WorkflowCancellationRequested", "WorkflowExecutionCompleted",
    "WorkflowExecutionFailed", "WorkflowExecutionTerminated",
  ]);
  const events = history.filter((event) => visibleTypes.has(event.event_type));
  const graph = $("graph");
  graph.replaceChildren();
  for (const event of events) {
    const isInput = ["SignalReceived", "WorkflowCancellationRequested"].includes(event.event_type);
    const isTerminal = event.event_type.startsWith("WorkflowExecution") && event.event_type !== "WorkflowExecutionStarted";
    const failed = ["WorkflowExecutionFailed", "WorkflowExecutionTerminated"].includes(event.event_type);
    const classes = ["graph-node", isInput ? "input" : "", isTerminal ? "terminal" : "", failed ? "failed" : ""].filter(Boolean).join(" ");
    graph.append(element("div", { className: classes, attrs: { role: "listitem" } }, [
      element("span", { className: "graph-index", text: `#${event.seq}` }),
      element("span", { className: "graph-copy" }, [
        element("strong", { text: humanize(event.event_type) }),
        element("small", { text: graphDescription(event) }),
      ]),
      element("time", { text: formatDate(event.created_at, true), attrs: { datetime: event.created_at } }),
    ]));
  }
}

function renderDebugFrame() {
  const frames = state.debugTrace?.frames || [];
  if (!frames.length) {
    $("debug-position").textContent = "No frames";
    $("debug-frame").replaceChildren(element("p", { text: "No committed history is available." }));
    $("debug-state").replaceChildren();
    $("debug-active").replaceChildren();
    return;
  }
  state.debugIndex = Math.max(0, Math.min(state.debugIndex, frames.length - 1));
  const frame = frames[state.debugIndex];
  $("debug-position").textContent = `Frame ${state.debugIndex + 1} / ${frames.length} · sequence ${frame.seq}${state.debugTrace.truncated ? " · prefix only" : ""}`;
  $("debug-slider").max = String(frames.length - 1);
  $("debug-slider").value = String(state.debugIndex);
  $("debug-previous").disabled = state.debugIndex === 0;
  $("debug-next").disabled = state.debugIndex === frames.length - 1;
  $("debug-frame").replaceChildren(
    element("span", { className: `debug-category ${frame.category}`, text: frame.category }),
    element("div", {}, [
      element("p", { className: "eyebrow", text: `Sequence ${frame.seq} · ${humanize(frame.event_type)}` }),
      element("h4", { text: frame.summary }),
      element("p", { text: frame.caused_by_seq ? `Resolves the command recorded at sequence ${frame.caused_by_seq}.` : frame.command_id !== null ? `Deterministic command ordinal ${frame.command_id}.` : "Committed replay checkpoint." }),
    ]),
  );
  const snapshot = frame.snapshot;
  const counters = [
    ["Commands", snapshot.commands], ["Waiting", snapshot.waiting_entities],
    ["Succeeded", snapshot.succeeded_entities], ["Failed", snapshot.failed_entities],
    ["Signals", snapshot.signals_received], ["Updates pending", snapshot.pending_updates],
  ];
  $("debug-state").replaceChildren(...counters.map(([label, value]) => element("div", {}, [
    element("span", { text: label }), element("strong", { text: value }),
  ])));
  $("debug-active-count").textContent = `${snapshot.waiting_entities} waiting`;
  $("debug-active").replaceChildren(...snapshot.active_entities.map((entity) => element("div", { className: "debug-entity" }, [
    element("span", { className: `debug-entity-kind ${entity.kind}`, text: entity.kind }),
    element("div", {}, [element("strong", { text: entity.label }), element("small", { text: `${shortId(entity.entity_id)} · from sequence ${entity.scheduled_seq}` })]),
    element("span", { className: "status-badge running", text: entity.status }),
  ])));
  if (!snapshot.active_entities.length) $("debug-active").append(element("p", { className: "inline-empty", text: snapshot.terminal_status ? `Replay reached ${snapshot.terminal_status}.` : "No activity, timer, or child is waiting at this frame." }));
}

function stopDebugPlayback() {
  if (state.debugTimer) clearInterval(state.debugTimer);
  state.debugTimer = null;
  $("debug-play").textContent = "Play";
}

function toggleDebugPlayback() {
  if (state.debugTimer) { stopDebugPlayback(); return; }
  const frames = state.debugTrace?.frames || [];
  if (frames.length < 2) return;
  if (state.debugIndex >= frames.length - 1) state.debugIndex = 0;
  $("debug-play").textContent = "Pause";
  state.debugTimer = setInterval(() => {
    state.debugIndex += 1;
    renderDebugFrame();
    if (state.debugIndex >= frames.length - 1) stopDebugPlayback();
  }, 650);
}

async function compareDebugTrace(event) {
  event.preventDefault();
  const otherId = $("debug-compare-id").value.trim();
  if (!otherId || !state.selected) return;
  const output = $("debug-comparison");
  output.hidden = false;
  output.textContent = "Comparing committed command streams…";
  try {
    const comparison = await api(`/api/workflows/${encodeURIComponent(state.selected)}/debug-compare/${encodeURIComponent(otherId)}`);
    if (comparison.compatible) {
      output.className = "debug-comparison is-compatible";
      output.textContent = `Compatible through ${comparison.matched_commands} committed commands${comparison.truncated ? " in the inspected prefix" : ""}.`;
    } else {
      const divergence = comparison.divergence;
      output.className = "debug-comparison is-divergent";
      output.textContent = `First divergence at command ${divergence.command_index}: ${divergence.reason}. This run: ${divergence.left_event_type || "ended"} at #${divergence.left_seq ?? "—"}; other: ${divergence.right_event_type || "ended"} at #${divergence.right_seq ?? "—"}.`;
    }
  } catch (error) {
    output.className = "debug-comparison is-divergent";
    output.textContent = error.message;
  }
}

async function loadDebugTrace(quiet = false) {
  const workflowId = state.selected;
  if (!workflowId) return;
  const trace = await api(`/api/workflows/${encodeURIComponent(workflowId)}/debug-trace`, { quiet });
  if (state.selected !== workflowId) return;
  const wasEmpty = state.debugTrace === null;
  state.debugTrace = trace;
  state.debugIndex = wasEmpty
    ? Math.max(0, trace.frames.length - 1)
    : Math.min(state.debugIndex, Math.max(0, trace.frames.length - 1));
  renderDebugFrame();
}

function rawSnapshot() {
  return JSON.stringify({
    execution: state.execution,
    history: state.history,
    history_truncated: state.historyTruncated,
  }, null, 2);
}

function setActiveTab(tab, updateLocation = true) {
  if (!VALID_TABS.has(tab)) return;
  state.activeTab = tab;
  document.querySelectorAll(".tab").forEach((button) => {
    const selected = button.dataset.tab === tab;
    button.classList.toggle("is-active", selected);
    button.setAttribute("aria-selected", String(selected));
    button.tabIndex = selected ? 0 : -1;
  });
  for (const name of VALID_TABS) $("panel-" + name).hidden = name !== tab;
  if (tab === "raw") $("raw-json").textContent = rawSnapshot();
  if (updateLocation) updateURL();
}

function renderDetail(execution, history) {
  $("detail-empty").hidden = true;
  $("detail-loading").hidden = true;
  $("detail-error").hidden = true;
  $("detail").hidden = false;
  $("workflow-type").textContent = execution.workflow_type;
  $("workflow-id").textContent = execution.id;
  $("workflow-version").textContent = `Version ${execution.definition_version}`;
  $("workflow-status").replaceWith(statusBadge(execution.paused_at ? "paused" : execution.status));
  const badge = document.querySelector(".detail-title-row .status-badge");
  badge.id = "workflow-status";
  $("history-count").textContent = `${history.length.toLocaleString()}${state.historyTruncated ? "+" : ""}`;

  const terminal = TERMINAL_STATUSES.has(execution.status);
  $("open-signal").disabled = terminal || !roleAtLeast("operator");
  $("open-update").disabled = terminal || !roleAtLeast("operator");
  $("open-attributes").disabled = !roleAtLeast("operator");
  $("request-cancel").disabled = terminal || Boolean(execution.cancellation_requested_at) || !roleAtLeast("operator");
  $("request-cancel").textContent = execution.cancellation_requested_at ? "Cancellation requested" : "Request cancellation";
  $("request-terminate").disabled = terminal || !roleAtLeast("admin");
  $("pause-resume").disabled = terminal || !roleAtLeast("operator");
  $("pause-resume").textContent = execution.paused_at ? "Resume" : "Pause";
  $("retry-workflow").hidden = !terminal;
  $("retry-workflow").disabled = !terminal || !roleAtLeast("admin");

  renderMetadata(execution);
  renderContinuationChain(state.continuationChain);
  renderOperationalState(execution, history);
  renderActivities(history);
  renderUpdates();
  renderEventFilters(history);
  renderHistory();
  renderGraph(history);
  renderDebugFrame();
  $("raw-json").textContent = rawSnapshot();
  setActiveTab(state.activeTab, false);
}

function showDetailLoading() {
  $("detail-empty").hidden = true;
  $("detail-error").hidden = true;
  $("detail").hidden = true;
  $("detail-loading").hidden = false;
}

function showDetailError(error) {
  $("detail-loading").hidden = true;
  $("detail").hidden = true;
  $("detail-empty").hidden = true;
  $("detail-error").hidden = false;
  $("detail-error-message").textContent = error.requestId
    ? `${error.message} · Request ${error.requestId}` : error.message;
}

async function selectExecution(id, options = {}) {
  const { updateLocation = true, quiet = false } = options;
  const changed = id !== state.selected;
  state.selected = id;
  if (changed) {
    stopDebugPlayback();
    state.debugTrace = null;
    state.debugIndex = 0;
    state.eventFilter = "";
    state.historySearch = "";
    $("history-search").value = "";
  }
  renderExecutionList();
  if (updateLocation) updateURL();
  const generation = ++state.detailGeneration;
  if (!quiet || !state.execution || changed) showDetailLoading();
  try {
    const [execution, history, updates, continuationChain] = await Promise.all([
      api(`/api/workflows/${encodeURIComponent(id)}`, { quiet }),
      loadExecutionHistory(id, quiet),
      api(`/api/workflows/${encodeURIComponent(id)}/updates?limit=100`, { quiet }),
      api(`/api/workflows/${encodeURIComponent(id)}/continuation-chain`, { quiet }),
    ]);
    if (generation !== state.detailGeneration || state.selected !== id) return;
    state.execution = execution;
    state.history = history.items;
    state.updates = updates;
    state.continuationChain = continuationChain;
    state.historyTruncated = history.truncated;
    renderDetail(execution, history.items);
    if (state.activeTab === "debugger") await loadDebugTrace(quiet);
  } catch (error) {
    if (generation !== state.detailGeneration) return;
    showDetailError(error);
  }
}

function renderUpdates() {
  $("workflow-updates").replaceChildren(...state.updates.map((update) => element("article", {
    className: "update-card",
  }, [
    element("div", {}, [element("strong", { text: update.name }), element("code", { text: update.update_id })]),
    statusBadge(update.status),
    element("span", { text: conciseValue(update.status === "completed" ? update.result : update.status === "rejected" ? update.failure : update.payload, 120) }),
  ])));
  $("updates-empty").hidden = state.updates.length > 0;
}

async function loadExecutionHistory(id, quiet) {
  const items = [];
  let afterSequence = 0;
  for (let pageNumber = 0; pageNumber < 5; pageNumber += 1) {
    const page = await api(
      `/api/workflows/${encodeURIComponent(id)}/history?after_seq=${afterSequence}&limit=1000`,
      { quiet },
    );
    items.push(...page.items);
    if (page.next_after_seq === null) return { items, truncated: false };
    afterSequence = page.next_after_seq;
  }
  const tail = await api(`/api/workflows/${encodeURIComponent(id)}/history-tail?limit=1000`, { quiet });
  const merged = new Map(items.map((event) => [event.seq, event]));
  for (const event of tail) merged.set(event.seq, event);
  return { items: [...merged.values()].sort((left, right) => left.seq - right.seq), truncated: true };
}

async function loadHealth(quiet = false) {
  try {
    await api("/api/health", { quiet });
    setHealth(true, "Engine healthy");
  } catch (error) {
    setHealth(false, "Engine unavailable");
    throw error;
  }
}

async function loadDashboard(options = {}) {
  const { quiet = false, refreshDetail = true } = options;
  $("refresh").disabled = true;
  $("refresh").classList.add("is-refreshing");
  if (!state.initialized && !quiet) showListState("Loading executions", "Connecting to durable storage…");
  try {
    const executionParams = new URLSearchParams({ limit: "1000" });
    if (state.statusFilter) executionParams.set("status", state.statusFilter);
    if (state.search.trim()) executionParams.set("query", state.search.trim());
    const [stats, executions] = await Promise.all([
      api("/api/stats", { quiet }),
      api(`/api/workflows?${executionParams}`, { quiet }),
      loadHealth(quiet),
    ]);
    state.stats = stats;
    state.executions = executions;
    state.initialized = true;
    renderStats();
    renderExecutionList();
    $("last-updated").textContent = `Updated ${new Intl.DateTimeFormat(undefined, { timeStyle: "medium" }).format(new Date())}`;
    if (state.selected && refreshDetail) await selectExecution(state.selected, { updateLocation: false, quiet });
  } catch (error) {
    if (error.status === 401) {
      requireAuthentication("Your session is missing or no longer valid.");
      return;
    }
    if (!state.initialized) showListState("Could not load workflows", error.message, true);
    if (!quiet) toast("Refresh failed", error.message, "error");
  } finally {
    $("refresh").disabled = false;
    $("refresh").classList.remove("is-refreshing");
  }
}

function schedulePolling() {
  if (state.refreshTimer) window.clearTimeout(state.refreshTimer);
  state.refreshTimer = null;
  if (!state.live || !state.principal) return;
  state.refreshTimer = window.setTimeout(async () => {
    if (!document.hidden) await loadDashboard({ quiet: true });
    schedulePolling();
  }, POLL_INTERVAL_MS);
}

function toast(title, message, type = "success") {
  const region = $("toast-region");
  const close = element("button", { text: "×", attrs: { type: "button", "aria-label": "Dismiss notification" } });
  const item = element("div", { className: `toast ${type === "error" ? "is-error" : ""}`, attrs: { role: type === "error" ? "alert" : "status" } }, [
    element("span", { className: "toast-mark", text: type === "error" ? "!" : "✓", attrs: { "aria-hidden": "true" } }),
    element("span", {}, [element("strong", { text: title }), element("small", { text: message })]),
    close,
  ]);
  const remove = () => item.remove();
  close.addEventListener("click", remove);
  region.append(item);
  window.setTimeout(remove, type === "error" ? 8000 : 4500);
}

async function copyText(text) {
  if (navigator.clipboard && window.isSecureContext) {
    await navigator.clipboard.writeText(text);
    return;
  }
  const input = element("textarea", { attrs: { "aria-hidden": "true" } });
  input.value = text;
  input.style.position = "fixed";
  input.style.opacity = "0";
  document.body.append(input);
  input.select();
  const copied = document.execCommand("copy");
  input.remove();
  if (!copied) throw new Error("Clipboard access is unavailable");
}

function parseJSON(text, label) {
  try { return JSON.parse(text.trim() || "null"); }
  catch (error) { throw new Error(`${label} must be valid JSON: ${error.message}`); }
}

function setSubmitting(button, submitting, label) {
  if (submitting) {
    button.dataset.originalLabel = button.textContent;
    button.textContent = label;
    button.disabled = true;
  } else {
    button.textContent = button.dataset.originalLabel || button.textContent;
    button.disabled = false;
  }
}

function randomId() {
  if (crypto.randomUUID) return crypto.randomUUID();
  const bytes = new Uint8Array(16);
  crypto.getRandomValues(bytes);
  bytes[6] = (bytes[6] & 15) | 64;
  bytes[8] = (bytes[8] & 63) | 128;
  const hex = [...bytes].map((value) => value.toString(16).padStart(2, "0")).join("");
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}

function showDialog(id, focusId) {
  const dialog = $(id);
  if (!dialog.open) dialog.showModal();
  window.setTimeout(() => $(focusId)?.focus(), 0);
}

function closeDialog(id) {
  const dialog = $(id);
  if (dialog.open) dialog.close();
}

function renderSession() {
  const signedIn = Boolean(state.principal);
  $("session-label").textContent = signedIn ? `${state.principal.key_id} · ${state.principal.role}` : "Sign in";
  $("session-avatar").textContent = signedIn ? state.principal.role.slice(0, 1) : "?";
  $("session-button").setAttribute("aria-label", signedIn ? "Manage authenticated session" : "Sign in to workflow operations");
  $("sign-out").hidden = !signedIn;
  $("open-start").disabled = !roleAtLeast("operator");
  $("open-dead-letter").disabled = !roleAtLeast("operator");
  $("open-schedules").disabled = !signedIn;
  $("schedule-submit").disabled = !roleAtLeast("operator");
}

function requireAuthentication(message = "Sign in to inspect and operate workflows.") {
  state.authToken = null;
  state.principal = null;
  safeSessionSet("dwe-api-token", null);
  renderSession();
  schedulePolling();
  $("auth-error").textContent = message;
  $("auth-error").hidden = false;
  showDialog("auth-dialog", "auth-token");
}

async function authenticateSession() {
  state.principal = await api("/api/session");
  renderSession();
}

async function signIn(event) {
  event.preventDefault();
  const token = $("auth-token").value.trim();
  const errorNode = $("auth-error");
  const button = $("auth-submit");
  errorNode.hidden = true;
  if (!token) {
    errorNode.textContent = "API token is required.";
    errorNode.hidden = false;
    return;
  }
  state.authToken = token;
  try {
    setSubmitting(button, true, "Signing in…");
    await authenticateSession();
    safeSessionSet("dwe-api-token", token);
    $("auth-token").value = "";
    closeDialog("auth-dialog");
    toast("Signed in", `${state.principal.key_id} has the ${state.principal.role} role.`);
    await loadDashboard();
    schedulePolling();
  } catch (error) {
    state.authToken = null;
    state.principal = null;
    safeSessionSet("dwe-api-token", null);
    renderSession();
    errorNode.textContent = error.status === 401 ? "That API token is not valid." : error.message;
    errorNode.hidden = false;
  } finally {
    setSubmitting(button, false, "Sign in");
  }
}

function signOut() {
  state.authToken = null;
  state.principal = null;
  state.executions = [];
  state.stats = null;
  state.execution = null;
  state.history = [];
  state.updates = [];
  state.historyTruncated = false;
  state.selected = null;
  safeSessionSet("dwe-api-token", null);
  closeDialog("auth-dialog");
  renderSession();
  renderExecutionList();
  $("detail").hidden = true;
  $("detail-empty").hidden = false;
  updateURL();
  requireAuthentication("You have signed out of workflow operations.");
}

async function startWorkflow(event) {
  event.preventDefault();
  const errorNode = $("start-error");
  errorNode.hidden = true;
  const button = $("start-submit");
  try {
    const workflowType = $("start-type").value.trim();
    const queueName = $("start-queue").value.trim();
    const definitionVersion = Number.parseInt($("start-version").value, 10);
    if (!workflowType) throw new Error("Workflow type is required.");
    if (!queueName) throw new Error("Task queue is required.");
    if (!Number.isInteger(definitionVersion) || definitionVersion < 1) throw new Error("Definition version must be 1 or greater.");
    const input = parseJSON($("start-input").value, "Workflow input");
    const searchAttributes = parseJSON($("start-attributes").value, "Search attributes");
    if (!searchAttributes || Array.isArray(searchAttributes) || typeof searchAttributes !== "object") throw new Error("Search attributes must be a JSON object.");
    setSubmitting(button, true, "Starting…");
    const started = await api("/api/workflows", {
      method: "POST",
      body: JSON.stringify({ workflow_type: workflowType, definition_version: definitionVersion, queue_name: queueName, input, search_attributes: searchAttributes }),
    });
    closeDialog("start-dialog");
    toast("Workflow started", `${workflowType} is now running.`);
    state.statusFilter = "";
    $("status-filter").value = "";
    state.selected = started.workflow_id;
    updateURL();
    await loadDashboard({ refreshDetail: false });
    await selectExecution(started.workflow_id, { updateLocation: false });
  } catch (error) {
    errorNode.textContent = error.message;
    errorNode.hidden = false;
  } finally {
    setSubmitting(button, false, "Start workflow");
  }
}

async function saveSearchAttributes(event) {
  event.preventDefault();
  if (!state.selected || !state.execution) return;
  const errorNode = $("attributes-error");
  const button = $("attributes-submit");
  errorNode.hidden = true;
  try {
    const attributes = parseJSON($("attributes-json").value, "Search attributes");
    if (!attributes || Array.isArray(attributes) || typeof attributes !== "object") throw new Error("Search attributes must be a JSON object.");
    const unset = Object.keys(state.execution.search_attributes || {}).filter((key) => !(key in attributes));
    setSubmitting(button, true, "Saving…");
    await api(`/api/workflows/${encodeURIComponent(state.selected)}/search-attributes`, {
      method: "PATCH",
      body: JSON.stringify({ set: attributes, unset }),
    });
    closeDialog("attributes-dialog");
    toast("Attributes updated", "Indexed visibility metadata is available immediately.");
    await loadDashboard();
  } catch (error) {
    errorNode.textContent = error.message;
    errorNode.hidden = false;
  } finally {
    setSubmitting(button, false, "Save attributes");
  }
}

async function sendSignal(event) {
  event.preventDefault();
  if (!state.selected) return;
  const errorNode = $("signal-error");
  errorNode.hidden = true;
  const button = $("signal-submit");
  try {
    const name = $("signal-name").value.trim();
    if (!name) throw new Error("Signal name is required.");
    const payload = parseJSON($("signal-payload").value, "Signal payload");
    setSubmitting(button, true, "Sending…");
    const response = await api(`/api/workflows/${encodeURIComponent(state.selected)}/signals`, {
      method: "POST",
      body: JSON.stringify({ signal_id: randomId(), name, payload }),
    });
    closeDialog("signal-dialog");
    if (response.accepted) toast("Signal accepted", `“${name}” was appended to durable history.`);
    else toast("Signal already received", "The idempotency key was already recorded.");
    $("signal-form").reset();
    $("signal-payload").value = "{}";
    await loadDashboard();
  } catch (error) {
    errorNode.textContent = error.message;
    errorNode.hidden = false;
  } finally {
    setSubmitting(button, false, "Send signal");
  }
}

async function requestWorkflowUpdate(event) {
  event.preventDefault();
  if (!state.selected) return;
  const errorNode = $("update-error");
  const button = $("update-submit");
  errorNode.hidden = true;
  try {
    const name = $("update-name").value.trim();
    if (!name) throw new Error("Update name is required.");
    const payload = parseJSON($("update-payload").value, "Update payload");
    const updateId = randomId();
    setSubmitting(button, true, "Requesting…");
    const response = await api(`/api/workflows/${encodeURIComponent(state.selected)}/updates`, {
      method: "POST",
      body: JSON.stringify({ update_id: updateId, name, payload }),
    });
    closeDialog("update-dialog");
    toast("Update requested", response.accepted ? "Workflow code will validate and resolve this durable request." : "This update ID was already recorded.");
    $("update-form").reset();
    $("update-payload").value = "{}";
    await loadDashboard();
  } catch (error) {
    errorNode.textContent = error.message;
    errorNode.hidden = false;
  } finally {
    setSubmitting(button, false, "Request update");
  }
}

function prepareConfirmation(action) {
  if (!state.execution || (action !== "retry" && TERMINAL_STATUSES.has(state.execution.status))) return;
  state.confirmAction = action;
  const terminating = action === "terminate";
  const retrying = action === "retry";
  $("confirm-title").textContent = retrying ? "Retry as a new execution?" : terminating ? "Terminate this workflow?" : "Request cancellation?";
  $("confirm-description").textContent = retrying
    ? "The original history remains immutable. A linked execution will start with the same input, version, queue, and search attributes."
    : terminating
      ? "Termination is immediate and final. The execution cannot resume after this event is persisted."
      : "The request is durable. The workflow will observe it during deterministic replay and may run cleanup logic.";
  $("confirm-submit").textContent = retrying ? "Retry as new" : terminating ? "Terminate workflow" : "Request cancellation";
  $("confirm-submit").className = terminating ? "button button-danger" : "button button-primary";
  $("confirm-reason").closest("label").hidden = retrying;
  $("confirm-reason").value = "";
  $("confirm-error").hidden = true;
  showDialog("confirm-dialog", retrying ? "confirm-submit" : "confirm-reason");
}

async function confirmControlAction(event) {
  event.preventDefault();
  if (!state.selected || !state.confirmAction) return;
  const action = state.confirmAction;
  const errorNode = $("confirm-error");
  const button = $("confirm-submit");
  errorNode.hidden = true;
  try {
    setSubmitting(button, true, action === "retry" ? "Starting retry…" : action === "terminate" ? "Terminating…" : "Requesting…");
    const response = await api(`/api/workflows/${encodeURIComponent(state.selected)}/${action}`, {
      method: "POST",
      ...(action === "retry" ? {} : { body: JSON.stringify({ reason: $("confirm-reason").value.trim() || "operator request" }) }),
    });
    closeDialog("confirm-dialog");
    if (action === "retry") {
      state.selected = response.workflow_id;
      state.statusFilter = "";
      $("status-filter").value = "";
      toast("Retry started", "A linked execution is now running with the original durable inputs.");
      updateURL();
      await loadDashboard({ refreshDetail: false });
      await selectExecution(response.workflow_id, { updateLocation: false });
      return;
    }
    toast(
      action === "terminate" ? "Workflow terminated" : "Cancellation requested",
      response.accepted ? "The control event was persisted." : "This control event had already been recorded.",
    );
    await loadDashboard();
  } catch (error) {
    errorNode.textContent = error.message;
    errorNode.hidden = false;
  } finally {
    setSubmitting(button, false, "Confirm");
  }
}

async function togglePause() {
  if (!state.selected || !state.execution) return;
  const action = state.execution.paused_at ? "resume" : "pause";
  const button = $("pause-resume");
  try {
    setSubmitting(button, true, action === "pause" ? "Pausing…" : "Resuming…");
    const response = await api(`/api/workflows/${encodeURIComponent(state.selected)}/${action}`, {
      method: "POST",
      body: JSON.stringify({ reason: "operator request" }),
    });
    const detail = action === "pause"
      ? "New task dispatch and pending deadlines are frozen."
      : "Task dispatch and pending deadlines are active.";
    toast(action === "pause" ? "Workflow paused" : "Workflow resumed", response.accepted ? detail : `The workflow was already ${action === "pause" ? "paused" : "running"}.`);
    await loadDashboard();
  } catch (error) {
    toast(`${humanize(action)} failed`, error.message, "error");
  } finally {
    setSubmitting(button, false, action === "pause" ? "Pause" : "Resume");
  }
}

function renderDeadLetters() {
  const rows = $("dead-letter-rows");
  rows.replaceChildren(...state.deadLetters.map((task) => {
    const open = element("button", { className: "button button-secondary", text: "Open", attrs: { type: "button" } });
    open.addEventListener("click", async () => {
      closeDialog("dead-letter-dialog");
      state.selected = task.workflow_id;
      updateURL();
      await selectExecution(task.workflow_id, { updateLocation: false });
    });
    return element("tr", {}, [
      element("td", {}, [
        element("strong", { text: task.workflow_type }),
        element("code", { text: shortId(task.workflow_id), attrs: { title: task.workflow_id } }),
      ]),
      element("td", { text: humanize(task.task_type) }),
      element("td", { text: task.attempt }),
      element("td", { text: task.queue_name }),
      element("td", { text: conciseValue(task.outcome, 90) }),
      element("td", {}, open),
    ]);
  }));
  $("dead-letter-empty").hidden = state.deadLetters.length > 0;
}

async function loadDeadLetters() {
  const errorNode = $("dead-letter-error");
  errorNode.hidden = true;
  try {
    state.deadLetters = await api("/api/dead-letter?limit=250");
    renderDeadLetters();
  } catch (error) {
    errorNode.textContent = error.message;
    errorNode.hidden = false;
  }
}

function renderSchedules() {
  const list = $("schedule-list");
  list.replaceChildren(...state.schedules.map((schedule) => {
    const toggle = element("button", {
      className: "button button-secondary",
      text: schedule.paused_at ? "Resume" : "Pause",
      attrs: { type: "button", disabled: roleAtLeast("operator") ? null : "disabled" },
    });
    toggle.addEventListener("click", async () => {
      const action = schedule.paused_at ? "resume" : "pause";
      try {
        setSubmitting(toggle, true, action === "pause" ? "Pausing…" : "Resuming…");
        await api(`/api/schedules/${encodeURIComponent(schedule.id)}/${action}`, { method: "POST" });
        toast(`Schedule ${action}d`, `${schedule.name} was ${action}d.`);
        await loadSchedules();
      } catch (error) {
        toast(`Schedule ${action} failed`, error.message, "error");
      } finally {
        setSubmitting(toggle, false, humanize(action));
      }
    });
    return element("article", { className: "schedule-card" }, [
      element("div", {}, [element("strong", { text: schedule.name }), element("small", { text: `${schedule.workflow_type} · v${schedule.definition_version} · ${schedule.queue_name}` })]),
      element("div", {}, [element("code", { className: "schedule-expression", text: schedule.cron_expression }), element("small", { text: `${schedule.timezone} · ${humanize(schedule.overlap_policy)}` })]),
      element("div", {}, [element("strong", { text: schedule.paused_at ? "Paused" : relativeTime(schedule.next_run_at) }), element("small", { text: schedule.paused_at ? `Since ${formatDate(schedule.paused_at)}` : formatDate(schedule.next_run_at, true) })]),
      toggle,
    ]);
  }));
  $("schedule-empty").hidden = state.schedules.length > 0;
}

async function loadSchedules() {
  try {
    state.schedules = await api("/api/schedules");
    renderSchedules();
  } catch (error) {
    $("schedule-error").textContent = error.message;
    $("schedule-error").hidden = false;
  }
}

async function createWorkflowSchedule(event) {
  event.preventDefault();
  const errorNode = $("schedule-error");
  const button = $("schedule-submit");
  errorNode.hidden = true;
  try {
    const input = parseJSON($("schedule-input").value, "Schedule input");
    const searchAttributes = parseJSON($("schedule-attributes").value, "Schedule search attributes");
    if (!searchAttributes || Array.isArray(searchAttributes) || typeof searchAttributes !== "object") throw new Error("Schedule search attributes must be a JSON object.");
    const definitionVersion = Number.parseInt($("schedule-version").value, 10);
    if (!Number.isInteger(definitionVersion) || definitionVersion < 1) throw new Error("Schedule version must be 1 or greater.");
    setSubmitting(button, true, "Creating…");
    await api("/api/schedules", {
      method: "POST",
      body: JSON.stringify({
        name: $("schedule-name").value.trim(),
        cron_expression: $("schedule-cron").value.trim(),
        workflow_type: $("schedule-type").value.trim(),
        definition_version: definitionVersion,
        queue_name: $("schedule-queue").value.trim(),
        timezone: $("schedule-timezone").value.trim(),
        overlap_policy: $("schedule-overlap").value,
        input,
        search_attributes: searchAttributes,
      }),
    });
    toast("Schedule created", "The first occurrence has been durably planned.");
    $("schedule-name").value = "";
    await loadSchedules();
  } catch (error) {
    errorNode.textContent = error.message;
    errorNode.hidden = false;
  } finally {
    setSubmitting(button, false, "Create schedule");
  }
}

function applyTheme(theme) {
  document.documentElement.dataset.theme = theme;
  document.querySelector('meta[name="theme-color"]').content = theme === "dark" ? "#050505" : "#f4f4f0";
  $("theme-toggle").setAttribute("aria-label", theme === "dark" ? "Use light theme" : "Use dark theme");
  safeStorageSet("dwe-theme", theme);
}

function moveListSelection(direction) {
  const executions = filteredExecutions();
  if (!executions.length) return;
  const current = executions.findIndex((item) => item.id === state.selected);
  const next = current < 0 ? 0 : Math.min(executions.length - 1, Math.max(0, current + direction));
  selectExecution(executions[next].id);
  document.querySelector(`[data-execution-id="${CSS.escape(executions[next].id)}"]`)?.focus();
}

function bindEvents() {
  $("refresh").addEventListener("click", () => loadDashboard());
  $("execution-search").addEventListener("input", (event) => {
    state.search = event.target.value;
    renderExecutionList();
    if (state.searchTimer) window.clearTimeout(state.searchTimer);
    state.searchTimer = window.setTimeout(() => loadDashboard({ quiet: true, refreshDetail: false }), 250);
  });
  $("status-filter").addEventListener("change", (event) => {
    state.statusFilter = VALID_STATUSES.has(event.target.value) ? event.target.value : "";
    renderStats();
    renderExecutionList();
    updateURL();
  });
  document.querySelectorAll("[data-status-filter]").forEach((card) => {
    card.addEventListener("click", () => {
      state.statusFilter = card.dataset.statusFilter;
      $("status-filter").value = state.statusFilter;
      renderStats();
      renderExecutionList();
      updateURL();
      $("execution-search").focus();
    });
  });
  $("live-toggle").addEventListener("change", (event) => {
    state.live = event.target.checked;
    safeStorageSet("dwe-live", String(state.live));
    schedulePolling();
    toast(state.live ? "Live updates on" : "Live updates paused", state.live ? "The console refreshes every five seconds." : "Use Refresh to fetch new state.");
  });
  $("theme-toggle").addEventListener("click", () => applyTheme(document.documentElement.dataset.theme === "dark" ? "light" : "dark"));
  $("session-button").addEventListener("click", () => {
    $("auth-error").hidden = true;
    showDialog("auth-dialog", "auth-token");
  });
  $("auth-form").addEventListener("submit", signIn);
  $("sign-out").addEventListener("click", signOut);
  $("open-start").addEventListener("click", () => {
    $("start-error").hidden = true;
    showDialog("start-dialog", "start-type");
  });
  $("start-form").addEventListener("submit", startWorkflow);
  $("open-signal").addEventListener("click", () => {
    $("signal-target").textContent = `${state.execution.workflow_type} · ${shortId(state.selected)}`;
    $("signal-error").hidden = true;
    showDialog("signal-dialog", "signal-name");
  });
  $("open-update").addEventListener("click", () => {
    $("update-target").textContent = `${state.execution.workflow_type} · ${shortId(state.selected)}`;
    $("update-error").hidden = true;
    showDialog("update-dialog", "update-name");
  });
  $("update-form").addEventListener("submit", requestWorkflowUpdate);
  $("open-attributes").addEventListener("click", () => {
    $("attributes-json").value = JSON.stringify(state.execution?.search_attributes || {}, null, 2);
    $("attributes-error").hidden = true;
    showDialog("attributes-dialog", "attributes-json");
  });
  $("attributes-form").addEventListener("submit", saveSearchAttributes);
  $("pause-resume").addEventListener("click", togglePause);
  $("retry-workflow").addEventListener("click", () => prepareConfirmation("retry"));
  $("open-dead-letter").addEventListener("click", async () => {
    showDialog("dead-letter-dialog", "refresh-dead-letter");
    await loadDeadLetters();
  });
  $("refresh-dead-letter").addEventListener("click", loadDeadLetters);
  $("open-schedules").addEventListener("click", async () => {
    $("schedule-error").hidden = true;
    showDialog("schedules-dialog", "schedule-name");
    await loadSchedules();
  });
  $("refresh-schedules").addEventListener("click", loadSchedules);
  $("schedule-form").addEventListener("submit", createWorkflowSchedule);
  $("signal-form").addEventListener("submit", sendSignal);
  $("request-cancel").addEventListener("click", () => prepareConfirmation("cancel"));
  $("request-terminate").addEventListener("click", () => prepareConfirmation("terminate"));
  $("confirm-form").addEventListener("submit", confirmControlAction);
  document.querySelectorAll("[data-close-dialog]").forEach((button) => {
    button.addEventListener("click", () => closeDialog(button.dataset.closeDialog));
  });
  document.querySelectorAll(".dialog").forEach((dialog) => {
    dialog.addEventListener("click", (event) => {
      if (event.target === dialog) dialog.close();
    });
  });
  document.querySelectorAll(".tab").forEach((tab) => {
    tab.addEventListener("click", async () => {
      setActiveTab(tab.dataset.tab);
      if (tab.dataset.tab === "debugger") await loadDebugTrace();
    });
    tab.addEventListener("keydown", async (event) => {
      if (!["ArrowLeft", "ArrowRight"].includes(event.key)) return;
      event.preventDefault();
      const tabs = [...document.querySelectorAll(".tab")];
      const index = tabs.indexOf(tab);
      const next = tabs[(index + (event.key === "ArrowRight" ? 1 : -1) + tabs.length) % tabs.length];
      setActiveTab(next.dataset.tab);
      if (next.dataset.tab === "debugger") await loadDebugTrace();
      next.focus();
    });
  });
  $("history-search").addEventListener("input", (event) => { state.historySearch = event.target.value; renderHistory(); });
  $("event-filter").addEventListener("change", (event) => { state.eventFilter = event.target.value; renderHistory(); });
  $("debug-previous").addEventListener("click", () => { stopDebugPlayback(); state.debugIndex -= 1; renderDebugFrame(); });
  $("debug-next").addEventListener("click", () => { stopDebugPlayback(); state.debugIndex += 1; renderDebugFrame(); });
  $("debug-play").addEventListener("click", toggleDebugPlayback);
  $("debug-slider").addEventListener("input", (event) => { stopDebugPlayback(); state.debugIndex = Number(event.target.value); renderDebugFrame(); });
  $("debug-compare-form").addEventListener("submit", compareDebugTrace);
  $("retry-detail").addEventListener("click", () => state.selected && selectExecution(state.selected));
  $("copy-workflow-id").addEventListener("click", async () => {
    try { await copyText(state.selected); toast("Workflow ID copied", state.selected); }
    catch (error) { toast("Copy failed", error.message, "error"); }
  });
  $("copy-raw").addEventListener("click", async () => {
    try { await copyText(rawSnapshot()); toast("JSON copied", "Execution snapshot copied to the clipboard."); }
    catch (error) { toast("Copy failed", error.message, "error"); }
  });
  $("download-raw").addEventListener("click", () => {
    const blob = new Blob([rawSnapshot()], { type: "application/json" });
    const url = URL.createObjectURL(blob);
    const link = element("a", { attrs: { href: url, download: `workflow-${state.selected}.json` } });
    document.body.append(link);
    link.click();
    link.remove();
    URL.revokeObjectURL(url);
  });
  $("execution-list").addEventListener("keydown", (event) => {
    if (event.key === "ArrowDown" || event.key === "ArrowUp") {
      event.preventDefault();
      moveListSelection(event.key === "ArrowDown" ? 1 : -1);
    }
  });
  document.addEventListener("keydown", (event) => {
    const typing = ["INPUT", "TEXTAREA", "SELECT"].includes(document.activeElement?.tagName);
    if (event.key === "/" && !typing && !document.querySelector("dialog[open]")) {
      event.preventDefault();
      $("execution-search").focus();
    }
    if (event.key.toLowerCase() === "r" && !typing && !event.metaKey && !event.ctrlKey && !document.querySelector("dialog[open]")) {
      event.preventDefault();
      loadDashboard();
    }
  });
  document.addEventListener("visibilitychange", () => {
    if (!document.hidden && state.live) loadDashboard({ quiet: true });
    schedulePolling();
  });
  window.addEventListener("popstate", () => {
    readURL();
    renderStats();
    renderExecutionList();
    setActiveTab(state.activeTab, false);
    if (state.selected) selectExecution(state.selected, { updateLocation: false });
  });
}

async function initialize() {
  readURL();
  const storedTheme = safeStorageGet("dwe-theme");
  const preferredTheme = window.matchMedia("(prefers-color-scheme: light)").matches ? "light" : "dark";
  applyTheme(storedTheme === "light" || storedTheme === "dark" ? storedTheme : preferredTheme);
  state.live = safeStorageGet("dwe-live") !== "false";
  state.authToken = safeSessionGet("dwe-api-token");
  $("live-toggle").checked = state.live;
  bindEvents();
  renderSession();
  setActiveTab(state.activeTab, false);
  await loadHealth();
  if (state.authToken) {
    try {
      await authenticateSession();
      await loadDashboard();
    } catch (error) {
      requireAuthentication(error.status === 401 ? "Your saved session is no longer valid." : error.message);
    }
  } else {
    requireAuthentication();
  }
  schedulePolling();
}

initialize();

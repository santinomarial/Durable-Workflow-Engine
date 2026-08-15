const state = { selected: null, executions: [] };
const $ = (id) => document.getElementById(id);

async function api(path, options = {}) {
  const response = await fetch(path, { headers: { "Content-Type": "application/json" }, ...options });
  if (!response.ok) throw new Error((await response.json()).detail || response.statusText);
  return response.json();
}

function badge(status) { return `<span class="status">${status}</span>`; }

async function loadExecutions() {
  const status = $("status-filter").value;
  state.executions = await api(`/api/workflows${status ? `?status=${status}` : ""}`);
  $("execution-list").innerHTML = state.executions.map(item => `
    <button class="execution" data-id="${item.id}">
      <strong>${item.workflow_type} ${badge(item.status)}</strong>
      <small>${item.id}</small><small>v${item.definition_version} · ${item.queue_name}</small>
    </button>`).join("") || "<p>No executions.</p>";
  document.querySelectorAll(".execution").forEach(button => button.onclick = () => selectExecution(button.dataset.id));
}

function renderGraph(history) {
  const nodes = history.filter(event => ["ActivityScheduled", "TimerStarted", "SignalReceived"].includes(event.event_type));
  $("graph").innerHTML = `<div class="node">Workflow</div>` + nodes.map(event => {
    const label = event.attributes.activity_type || event.attributes.name || event.event_type;
    return `<div class="node" title="${event.entity_id || event.external_id || ""}">${label}</div>`;
  }).join("");
}

async function selectExecution(id) {
  state.selected = id;
  const [execution, history] = await Promise.all([api(`/api/workflows/${id}`), api(`/api/workflows/${id}/history`)]);
  $("empty").classList.add("hidden"); $("detail").classList.remove("hidden");
  $("workflow-type").textContent = execution.workflow_type;
  $("workflow-status").textContent = execution.status;
  $("metadata").innerHTML = [["ID", execution.id], ["Version", execution.definition_version], ["Queue", execution.queue_name], ["Created", execution.created_at], ["Closed", execution.closed_at || "—"]]
    .map(([key, value]) => `<div><dt>${key}</dt><dd>${value}</dd></div>`).join("");
  $("history").innerHTML = history.map(event => `<article class="event"><strong>#${event.seq}</strong><span>${event.event_type}</span><code>${JSON.stringify(event.attributes, null, 2)}</code></article>`).join("");
  renderGraph(history);
}

$("refresh").onclick = loadExecutions;
$("status-filter").onchange = loadExecutions;
$("send-signal").onclick = async () => {
  if (!state.selected) return;
  const name = $("signal-name").value;
  let payload = null; try { payload = JSON.parse($("signal-payload").value || "null"); } catch { alert("Signal payload must be JSON"); return; }
  await api(`/api/workflows/${state.selected}/signals`, { method: "POST", body: JSON.stringify({ signal_id: crypto.randomUUID(), name, payload }) });
  await selectExecution(state.selected); await loadExecutions();
};
$("terminate").onclick = async () => {
  if (!state.selected || !confirm("Terminate this workflow?")) return;
  await api(`/api/workflows/${state.selected}/terminate`, { method: "POST", body: JSON.stringify({ reason: "operator request" }) });
  await selectExecution(state.selected); await loadExecutions();
};

api("/api/health").then(() => { $("health").textContent = "PostgreSQL connected"; $("health-dot").style.background = "#4ee09a"; }).catch(error => { $("health").textContent = error.message; $("health-dot").style.background = "#ff6b6b"; });
loadExecutions();

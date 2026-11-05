// Drives the real dashboard.html in jsdom with a stubbed API and checks that a Diff result survives
// the periodic refresh, updates on a new Diff, and does not leak into another run.
// Usage: node dashboard_diff_check.mjs <path-to-dashboard.html>   (needs the "jsdom" npm package)
import { createRequire } from "node:module";
import fs from "node:fs";

const require = createRequire(process.env.JSDOM_FROM || import.meta.url);
let JSDOM;
try { ({ JSDOM } = require("jsdom")); } catch { console.log("jsdom not installed"); process.exit(77); }
const html = fs.readFileSync(process.argv[2], "utf8");

const RUNS = ["aaaaaaaa-0000-0000-0000-000000000001", "bbbbbbbb-0000-0000-0000-000000000002",
  "cccccccc-0000-0000-0000-000000000003"];
const run = (id) => ({ run_id: id, workflow_name: "refund_workflow", workflow_version: 1, status: "COMPLETED",
  input_data: {}, result_data: {}, fencing_token: 1, lease_owner: null, waiting_signal: null, parent_run_id: null });
const diffFor = (a, b) => ({ first_divergence: "step_" + b.slice(0, 1), steps: [
  { step_name: "fetch_order", identical: true, result_a: 1, result_b: 1 },
  { step_name: "step_" + b.slice(0, 1), identical: false, result_a: "A", result_b: "B" }] });

function route(path) {
  let m;
  if (path.startsWith("/runs?")) return RUNS.map(run);
  if (path === "/workers") return [];
  if ((m = path.match(/^\/runs\/([^/]+)\/diff\/(.+)$/))) {
    if (m[2] === "missing") throw Object.assign(new Error("run not found"), { status: 404 });
    return diffFor(m[1], m[2]);
  }
  if ((m = path.match(/^\/runs\/([^/]+)\/events$/)))
    return [{ sequence_number: 1, event_type: "STEP_STARTED", step_name: "fetch_order", result_data: null }];
  if (path.match(/\/(timers|signals|external-operations)$/)) return [];
  if ((m = path.match(/^\/runs\/([^/]+)$/))) return run(m[1]);
  throw new Error("unexpected " + path);
}

const dom = new JSDOM(html, { runScripts: "dangerously", url: "http://localhost:8000/dashboard", pretendToBeVisual: true,
  beforeParse(w) {
    w.fetch = async (path) => {
      try { const body = route(path); return { ok: true, json: async () => body }; }
      catch (e) { return { ok: false, statusText: e.message, json: async () => ({ detail: e.message }) }; }
    };
  } });
const w = dom.window;
const tick = () => new Promise((r) => setTimeout(r, 20));
const out = () => w.document.getElementById("diffOut")?.textContent ?? "";
let failed = 0;
const check = (name, ok) => { console.log((ok ? "PASS " : "FAIL ") + name); if (!ok) failed++; };

await tick();
await w.select(RUNS[0]); await tick();
w.document.getElementById("diffId").value = RUNS[1];
w.document.getElementById("diffBtn").click(); await tick();
check("diff result is shown after clicking Diff", out().includes("first divergence: step_b"));

for (let i = 0; i < 3; i++) { await w.refresh(); await tick(); }
check("diff result survives 3 automatic refreshes", out().includes("first divergence: step_b"));
check("other-run id box keeps its value across refreshes", w.document.getElementById("diffId").value === RUNS[1]);

w.document.getElementById("diffId").value = RUNS[2];
w.document.getElementById("diffBtn").click(); await tick(); await w.refresh(); await tick();
check("a new Diff replaces the old result", out().includes("step_c") && !out().includes("step_b"));

w.document.getElementById("diffId").value = "missing";
w.document.getElementById("diffBtn").click(); await tick(); await w.refresh(); await tick();
check("a failed Diff shows its error and survives refresh", out().includes("run not found"));

await w.select(RUNS[1]); await tick(); await w.refresh(); await tick();
check("selecting another run does not show the previous comparison", out().trim() === "");
check("other-run id box is empty for the newly selected run", w.document.getElementById("diffId").value === "");

w.close();
process.exit(failed ? 1 : 0);

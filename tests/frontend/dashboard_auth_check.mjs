// The dashboard must ask for the API key once on a 401, send it as X-API-Key afterwards, remember it
// for the tab, and not nag again if the user cancels. Usage: node dashboard_auth_check.mjs <dashboard.html>
import { createRequire } from "node:module";
import fs from "node:fs";

const require = createRequire(process.env.JSDOM_FROM || import.meta.url);
let JSDOM;
try { ({ JSDOM } = require("jsdom")); } catch { console.log("jsdom not installed"); process.exit(77); }
const html = fs.readFileSync(process.argv[2], "utf8");

function boot(answer) {
  const seen = { prompts: 0, withKey: 0, withoutKey: 0 };
  const dom = new JSDOM(html, { runScripts: "dangerously", url: "http://localhost:8000/dashboard",
    pretendToBeVisual: true,
    beforeParse(w) {
      w.prompt = () => { seen.prompts++; return answer; };
      w.fetch = async (path, opts = {}) => {
        const key = (opts.headers || {})["x-api-key"];
        key ? seen.withKey++ : seen.withoutKey++;
        if (key !== "s3cret") return { ok: false, status: 401, statusText: "Unauthorized", json: async () => ({ detail: "missing or invalid API key" }) };
        const body = path.startsWith("/runs?") ? [] : path === "/workers" ? [] : {};
        return { ok: true, status: 200, json: async () => body };
      };
    } });
  return { dom, seen };
}
const tick = (ms = 40) => new Promise((r) => setTimeout(r, ms));
let failed = 0;
const check = (name, ok) => { console.log((ok ? "PASS " : "FAIL ") + name); if (!ok) failed++; };

{ // correct key typed at the prompt
  const { dom, seen } = boot("s3cret"); const w = dom.window; await tick();
  check("asks for the key once when the API answers 401", seen.prompts === 1);
  check("loads data after the key is given", w.document.getElementById("stamp").textContent.startsWith("updated"));
  check("remembers the key for this tab", w.sessionStorage.getItem("apiKey") === "s3cret");
  const before = seen.prompts; await w.refresh(); await w.refresh(); await tick();
  check("does not ask again on later refreshes", seen.prompts === before);
  w.close();
}
{ // user cancels the prompt
  const { dom, seen } = boot(null); const w = dom.window; await tick();
  await w.refresh(); await w.refresh(); await tick();
  check("a cancelled prompt is not repeated on every refresh", seen.prompts === 1);
  check("the page tells the user a key is required", w.document.getElementById("stamp").textContent.includes("API key required"));
  w.close();
}
{ // the header button changes the key
  const { dom, seen } = boot("wrong"); const w = dom.window; await tick();
  check("a wrong key keeps showing the error", w.document.getElementById("stamp").textContent.includes("API key required"));
  w.prompt = () => "s3cret";
  w.document.getElementById("keyBtn").click(); await tick();
  check("the API key button lets the user fix it", w.document.getElementById("stamp").textContent.startsWith("updated"));
  w.close();
}
process.exit(failed ? 1 : 0);

// Progressive enhancement only: every page works without this file.
// No data is ever inlined in a script: this file is static and served from /assets.
// Data from the server is only ever set with textContent (never innerHTML), and URLs it hands
// back are followed only if they are paths of this server.
"use strict";
document.addEventListener("DOMContentLoaded", function () {
  // "/" focuses the lookup box, as in many browsers' find-in-page.
  var lookup = document.getElementById("lookup-key");
  if (lookup) {
    document.addEventListener("keydown", function (e) {
      var t = e.target && e.target.tagName;
      if (e.key === "/" && t !== "INPUT" && t !== "TEXTAREA" && t !== "SELECT") {
        e.preventDefault();
        lookup.focus();
      }
    });
  }
  // Expand / collapse every value tree of a key page.
  var tools = document.querySelector(".tree-tools");
  if (tools && document.querySelector(".tree details")) {
    [["Expand all", true], ["Collapse all", false]].forEach(function (b) {
      var button = document.createElement("button");
      button.type = "button";
      button.textContent = b[0];
      button.addEventListener("click", function () {
        document.querySelectorAll(".tree details").forEach(function (d) { d.open = b[1]; });
      });
      tools.appendChild(button);
    });
  }
  search();
  counts();
});

// A path of this server under `prefix` (never another origin: no `//`, no scheme).
function ownPath(u, prefix) {
  return typeof u === "string" && u.indexOf(prefix) === 0 && u.indexOf("//") === -1 &&
    u.indexOf("\\") === -1;
}

function getJson(url) {
  return fetch(url, { credentials: "same-origin", headers: { Accept: "application/json" } })
    .then(function (r) {
      return r.json().then(function (j) {
        if (!r.ok) { throw new Error(j && j.error ? j.error : "HTTP " + r.status); }
        return j;
      });
    });
}

function cell(tr, text, cls) {
  var td = document.createElement("td");
  if (cls) { td.className = cls; }
  if (text !== null) { td.textContent = text; }
  tr.appendChild(td);
  return td;
}

// Search: the page holds the first request's matches; while `data-api` names the next request,
// issue it, append its matches, and go on until the scan is done, the Stop button is pressed or
// MAX_ROWS matches are on the page.
function search() {
  var status = document.getElementById("search-status");
  var tbody = document.querySelector("#search-results tbody");
  var stop = document.getElementById("search-stop");
  var nav = document.getElementById("search-next");
  var warnings = document.getElementById("search-warnings");
  if (!status || !tbody || !stop || !nav || !ownPath(status.dataset.api, "/api/op/")) { return; }
  var MAX_ROWS = 10000;
  var api = status.dataset.api;
  var rows = tbody.rows.length;
  var first = nav.querySelector("a:last-child");
  var nextPage = first ? first.getAttribute("href") : null;
  var stopped = false;
  nav.hidden = true;
  stop.hidden = false;

  function finish(message) {
    stopped = true;
    stop.hidden = true;
    status.textContent = message;
    nav.textContent = "";
    if (ownPath(nextPage, "/op/")) {
      var a = document.createElement("a");
      a.href = nextPage;
      a.textContent = "Continue from here »";
      nav.appendChild(a);
    } else {
      nav.textContent = "End of the search.";
    }
    nav.hidden = false;
  }

  function addRow(m) {
    var tr = document.createElement("tr");
    var key = cell(tr, null, "key");
    var a = document.createElement("a");
    a.textContent = m.key;
    if (ownPath(m.href, "/op/")) { a.href = m.href; }
    key.appendChild(a);
    cell(tr, m.summary, "val");
    cell(tr, String(m.key_group));
    cell(tr, String(m.subtask));
    tbody.appendChild(tr);
  }

  function warn(list) {
    (list || []).forEach(function (u) {
      var p = document.createElement("p");
      p.className = "error";
      p.setAttribute("role", "alert");
      p.textContent = typeof u === "string" ? "Malformed key-group: " + u :
        "Skipped unreadable key-groups " + u.first_key_group + "–" + u.last_key_group +
        " (subtask " + u.subtask + ", " + u.what + "): " + u.error;
      warnings.appendChild(p);
    });
  }

  stop.addEventListener("click", function () {
    finish("Stopped: " + rows + " match(es) so far.");
  });

  function step() {
    if (stopped) { return; }
    getJson(api).then(function (j) {
      if (stopped) { return; }
      (j.matches || []).forEach(addRow);
      rows += (j.matches || []).length;
      warn(j.unreadable);
      warn(j.malformed);
      nextPage = j.next_page;
      if (j.done) {
        nextPage = null;
        finish(rows + " match(es) · search complete · " + j.progress_text + ".");
      } else if (rows >= MAX_ROWS) {
        finish("Paused at " + rows + " matches (refine the query, or continue): " +
          j.progress_text + ".");
      } else if (!ownPath(j.next_api, "/api/op/")) {
        finish("Search stopped: unexpected answer.");
      } else {
        status.textContent = "Searching… " + rows + " match(es) · " + j.progress_text;
        api = j.next_api;
        step();
      }
    }).catch(function (e) {
      finish("Search failed: " + e.message);
    });
  }
  status.textContent = "Searching… " + rows + " match(es)";
  step();
}

// Counts: while a count runs, poll its JSON once a second and update the cells of the rows
// whose data-state names a state, until it is done, cancelled or failed.
function counts() {
  var box = document.querySelector("[data-count-api]");
  if (!box || !ownPath(box.dataset.countApi, "/api/op/")) { return; }
  var api = box.dataset.countApi;
  function update(j) {
    var byName = {};
    (j.states || []).forEach(function (s) { byName["s:" + s.state] = s; });
    box.querySelectorAll("tr[data-state]").forEach(function (tr) {
      var s = byName["s:" + tr.dataset.state];
      if (!s) { return; }
      var k = tr.querySelector(".count-keys");
      var e = tr.querySelector(".count-entries");
      if (k) { k.textContent = s.keys_text; }
      if (e) { e.textContent = s.entries_text; }
    });
    var total = document.getElementById("count-total");
    if (total) { total.textContent = j.keys_text; }
    var status = document.getElementById("count-status");
    if (status) { status.textContent = j.status_text; }
  }
  function poll() {
    getJson(api).then(function (j) {
      update(j);
      if (j.status === "running" || j.status === "cancelling") {
        setTimeout(poll, 1000);
      } else {
        var actions = document.getElementById("count-actions");
        if (actions) {
          // The "Cancel" link, or the "Cancelling…" note, becomes "Count again".
          var old = actions.querySelector("a[href*='cancel=1'], span");
          if (old && ownPath(api, "/api/op/")) {
            var a = document.createElement("a");
            a.href = api.slice(4) + "?restart=1";
            a.textContent = "Count again";
            old.replaceWith(a);
          }
        }
      }
    }).catch(function (e) {
      var status = document.getElementById("count-status");
      if (status) { status.textContent = "Lost the count's progress: " + e.message; }
    });
  }
  setTimeout(poll, 500);
}

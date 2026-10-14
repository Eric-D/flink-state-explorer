// Progressive enhancement only: every page works without this file.
// No data is ever inlined in a script: this file is static and served from /assets.
"use strict";
document.addEventListener("DOMContentLoaded", function () {
  // "/" focuses the lookup box, as in many browsers' find-in-page.
  var lookup = document.getElementById("lookup-key");
  if (lookup) {
    document.addEventListener("keydown", function (e) {
      var t = e.target && e.target.tagName;
      if (e.key === "/" && t !== "INPUT" && t !== "TEXTAREA") {
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
});

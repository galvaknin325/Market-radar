// Market Radar service worker: lets the site install as an app and open without a connection.
// Data files and pages always try the network first, so you never see stale alerts while online.
const CACHE = "radar-v2";
const SHELL = ["./", "index.html", "i18n.js", "manifest.webmanifest", "icon-192.png", "icon-512.png", "accessibility.html", "terms.html"];

self.addEventListener("install", e => {
  e.waitUntil(caches.open(CACHE).then(c => c.addAll(SHELL)).then(() => self.skipWaiting()));
});

self.addEventListener("activate", e => {
  e.waitUntil(caches.keys()
    .then(keys => Promise.all(keys.filter(k => k !== CACHE).map(k => caches.delete(k))))
    .then(() => self.clients.claim()));
});

self.addEventListener("fetch", e => {
  const url = new URL(e.request.url);
  if (e.request.method !== "GET" || url.origin !== location.origin) return;   // charts etc. go straight to the network
  e.respondWith(
    fetch(e.request)
      .then(res => {
        if (res.ok) { const copy = res.clone(); caches.open(CACHE).then(c => c.put(e.request.url.split("?")[0], copy)); }
        return res;
      })
      .catch(() => caches.match(e.request.url.split("?")[0]).then(r => r || caches.match("index.html")))
  );
});

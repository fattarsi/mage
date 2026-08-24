// Minimal service worker — its only job is to make the app installable to the home screen and to
// keep the app shell reachable. It is deliberately NOT a caching layer for gameplay: the live game
// runs over a WebSocket plus /img, /cardinfo, /api calls that must always hit the network. We only
// network-first the top-level navigation (so launching from the home screen always loads the latest
// app, but still opens if briefly offline).
const SHELL = 'xmage-shell-v2';

self.addEventListener('install', (e) => {
  e.waitUntil(caches.open(SHELL).then((c) => c.add('./')).catch(() => {}));
  self.skipWaiting();
});

self.addEventListener('activate', (e) => {
  e.waitUntil(
    caches.keys().then((keys) => Promise.all(keys.filter((k) => k !== SHELL).map((k) => caches.delete(k))))
  );
  self.clients.claim();
});

self.addEventListener('fetch', (e) => {
  const req = e.request;
  if (req.method !== 'GET') return;              // never touch POST/WS upgrades etc.
  if (req.mode !== 'navigate') return;           // let images / api / everything else go straight to network
  e.respondWith(
    // {cache:'no-store'} bypasses the browser's HTTP cache so a home-screen launch / reload ALWAYS gets
    // the freshly-deployed app shell (only falling back to the cached copy when actually offline).
    fetch(req, { cache: 'no-store' })
      .then((res) => { const copy = res.clone(); caches.open(SHELL).then((c) => c.put('./', copy)); return res; })
      .catch(() => caches.match('./'))
  );
});

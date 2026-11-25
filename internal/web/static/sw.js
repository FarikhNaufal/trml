const CACHE_NAME = 'trml-shell-v9';
const STATIC_ASSETS = [
  '/',
  '/index.html',
  '/style.css?v=9',
  '/app.js?v=9',
  '/manifest.json',
  '/icon.svg',
  '/vendor/basecoat-mira.cdn.min.css?v=9',
  '/vendor/basecoat.min.js?v=9',
  '/vendor/tabs.min.js?v=9',
  '/vendor/toast.min.js?v=9',
  '/vendor/popover.min.js?v=9',
  '/vendor/input-otp.min.js?v=9',
  '/vendor/command.min.js?v=9'
];

self.addEventListener('install', (event) => {
  event.waitUntil(
    caches.open(CACHE_NAME).then((cache) => cache.addAll(STATIC_ASSETS)).catch(() => {})
  );
  self.skipWaiting();
});

self.addEventListener('activate', (event) => {
  event.waitUntil(
    caches.keys().then((keys) =>
      Promise.all(keys.filter((k) => k !== CACHE_NAME).map((k) => caches.delete(k)))
    )
  );
  self.clients.claim();
});

self.addEventListener('fetch', (event) => {
  if (event.request.method !== 'GET') return;

  const url = new URL(event.request.url);

  // Never cache live API telemetry or auth endpoints
  if (url.pathname.startsWith('/api/') || url.pathname === '/health') {
    return;
  }

  // Immutable cache-first for versioned vendored Basecoat assets
  if (url.pathname.startsWith('/vendor/')) {
    event.respondWith(
      caches.match(event.request).then(
        (cached) =>
          cached ||
          fetch(event.request).then((res) => {
            if (res && res.ok) {
              const copy = res.clone();
              caches.open(CACHE_NAME).then((c) => c.put(event.request, copy));
            }
            return res;
          })
      )
    );
    return;
  }

  // Network-first fallback to cache for app shell files
  event.respondWith(
    fetch(event.request)
      .then((res) => {
        if (res && res.ok && url.origin === self.location.origin) {
          const copy = res.clone();
          caches.open(CACHE_NAME).then((c) => c.put(event.request, copy));
        }
        return res;
      })
      .catch(() => caches.match(event.request))
  );
});

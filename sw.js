const CACHE_NAME = 'rada-v2';
const ASSETS = [
  'index.html',
  'logo.png',
  'manifest.json'
];

self.addEventListener('install', (e) => {
  e.waitUntil(
    caches.open(CACHE_NAME)
      .then((cache) => cache.addAll(ASSETS))
      .catch(() => {})
  );
});

self.addEventListener('fetch', (e) => {
  e.respondWith(
    caches.match(e.request)
      .then((cached) => {
        if (cached) return cached;
        return fetch(e.request).catch(() => caches.match('index.html'));
      })
      .catch(() => caches.match('index.html'))
  );
});

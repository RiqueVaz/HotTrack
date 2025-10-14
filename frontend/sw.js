const CACHE_NAME = 'hottrack-cache-v1';
const urlsToCache = [
  '/',
  '/index.html',
  // Adicione aqui outros arquivos importantes se tiver (CSS, JS externos)
];

self.addEventListener('install', event => {
  event.waitUntil(
    caches.open(CACHE_NAME)
      .then(cache => {
        console.log('Cache aberto');
        return cache.addAll(urlsToCache);
      })
  );
});

self.addEventListener('fetch', event => {
  event.respondWith(
    caches.match(event.request)
      .then(response => {
        // Se encontrar no cache, retorna do cache. Senão, busca na rede.
        return response || fetch(event.request);
      })
  );
});

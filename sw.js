self.addEventListener('push', function(event) {
    const data = event.data.json();
    const options = {
        body: data.body,
        icon: 'icons/icon-192x192.png', // Caminho atualizado
    };
    event.waitUntil(
        self.registration.showNotification(data.title, options)
    );
});

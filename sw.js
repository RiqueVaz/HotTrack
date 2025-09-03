self.addEventListener('push', function(event) {
    const data = event.data.json();
    const options = {
        body: data.body,
        icon: 'https://ibb.co/wFvCyfyT', // Caminho atualizado
    };
    event.waitUntil(
        self.registration.showNotification(data.title, options)
    );
});

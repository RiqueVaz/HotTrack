self.addEventListener('push', function(event) {
    const data = event.data.json();
    const options = {
        body: data.body,
        icon: 'https://cdn-icons-png.flaticon.com/512/8061/8061213.png', // Pode personalizar o ícone
    };
    event.waitUntil(
        self.registration.showNotification(data.title, options)
    );
});

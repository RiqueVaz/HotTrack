self.addEventListener('push', function(event) {
    const data = event.data.json();
    const options = {
        body: data.body,
        icon: 'https://cdn-icons-png.flaticon.com/512/14580/14580928.png', // Ícone moderno
    };
    event.waitUntil(
        self.registration.showNotification(data.title, options)
    );
});

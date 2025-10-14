self.addEventListener('push', function(event) {
    const data = event.data.json();
    const options = {
        body: data.body,
        icon: 'https://i.postimg.cc/ZnHB6HB5/icon-512x512.png', // Novo link aqui
    };
    event.waitUntil(
        self.registration.showNotification(data.title, options)
    );
});

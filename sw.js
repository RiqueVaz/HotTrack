self.addEventListener('push', function(event) {
    const data = event.data.json();
    const options = {
        body: data.body,
        icon: 'https://i.postimg.cc/VL8bKCD9/Gemini-Generated-Image-lejmdflejmdflejm-removebg-preview.png', // Ícone moderno
    };
    event.waitUntil(
        self.registration.showNotification(data.title, options)
    );
});

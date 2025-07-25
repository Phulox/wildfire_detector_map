const map = new L.map('map', {
    center: [20, 0], // Center on equator, prime meridian (good world center)
    zoom: 3,         // Zoom level 2 shows whole world without duplicates and eliminates side bars
    minZoom: 3,      // Prevent zooming out further (avoids duplicates) and eliminates side bars
    maxZoom: 10,     // Maximum zoom in level
    worldCopyJump: false,  // Disable world wrapping
    maxBounds: [           // Lock user into 1 world copy
        [-90, -180],       // Southwest corner
        [90, 180]          // Northeast corner
    ],
    maxBoundsViscosity: 1.0         
})

new L.tileLayer('https://api.maptiler.com/maps/topo-v2/{z}/{x}/{y}.png?key=NcQsXOqcUf1QGwQBeXN6',{
    attribution: '<a href="https://www.maptiler.com/copyright/" target="_blank">&copy; MapTiler</a> <a href="https://www.openstreetmap.org/copyright" target="_blank">&copy; OpenStreetMap contributors</a>',
    noWrap: true // Prevent tile repetition horizontally
}).addTo(map)

new L.Control.Geocoder().addTo(map)

// Add this debug version to your JavaScript
let fireMarkers = [];
let addedMarkers = false;

const showActiveFires = document.getElementById('showActFireBtn')

showActiveFires.addEventListener('click', () => {
    console.log('Button clicked!'); // Debug: Check if button click is detected

    // option to remove markers from map using toggle method
    if(addedMarkers){
        fireMarkers.forEach(marker => map.removeLayer(marker));
        fireMarkers = [];
        showActiveFires.textContent = 'Show Active Fires'
        addedMarkers = false
        return;
    }
    
    fetch('/api/active_fires')
        .then(res => {
            console.log('Response received:', res.status); // Debug: Check response status
            return res.json();
        })
        .then(json => {
            
            if (!json.success) {
                console.error('API returned error:', json.error);
                alert('Failed to load fire data: ' + (json.error || 'Unknown error'));
                return;
            }

            // Clear previous markers if any
            fireMarkers.forEach(marker => map.removeLayer(marker));
            fireMarkers = [];

            json.data.forEach((fire, index) => {
                
                const marker = L.marker([fire.latitude, fire.longitude], {
                    icon: L.icon({
                        iconUrl: 'https://maps.google.com/mapfiles/ms/icons/red-dot.png',
                        iconSize: [32, 32],
                        iconAnchor: [16, 32],
                        popupAnchor: [0, -32]
                    })
                }).addTo(map);

                marker.bindPopup(`
                    <strong>🔥 Active Fire</strong><br>
                    Brightness: ${fire.brightness}<br>
                    Confidence: ${fire.confidence}<br>
                    Date: ${fire.acq_date}<br>
                    Time: ${fire.acq_time}<br>
                    Satellite: ${fire.satellite}<br>
                    Day/Night: ${fire.daynight}<br>
                    FRP: ${fire.frp}
                `);
                fireMarkers.push(marker);
            });

            showActiveFires.textContent = 'Remove Active Fires';
            addedMarkers = true;
            


            if (fireMarkers.length > 0) {
                const group = L.featureGroup(fireMarkers);
                map.fitBounds(group.getBounds().pad(0.2));
                console.log('Map bounds adjusted'); // Debug: Check if bounds changed
            } else {
                console.log('No fires to display');
                alert('No active fires found in the last 24 hours');
            }

        })
        .catch(err => {
            console.error('Fetch error:', err);
            alert('Error loading fire data: ' + err.message);
        });
});


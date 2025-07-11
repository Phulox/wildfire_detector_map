const map = L.map('map', {
    center: [20, 0], // Center on equator, prime meridian (good world center)
    zoom: 2,         // Zoom level 2 shows whole world without duplicates
    minZoom: 2,      // Prevent zooming out further (avoids duplicates)
    maxZoom: 10,     // Maximum zoom in level
    worldCopyJump: false,  // Disable world wrapping
    maxBounds: [           // Lock user into 1 world copy
        [-90, -180],       // Southwest corner
        [90, 180]          // Northeast corner
    ],
    maxBoundsViscosity: 1.0         
})

L.tileLayer('https://api.maptiler.com/maps/topo-v2/{z}/{x}/{y}.png?key=NcQsXOqcUf1QGwQBeXN6',{
    attribution: '<a href="https://www.maptiler.com/copyright/" target="_blank">&copy; MapTiler</a> <a href="https://www.openstreetmap.org/copyright" target="_blank">&copy; OpenStreetMap contributors</a>',
    noWrap: true // Prevent tile repetition horizontally
}).addTo(map);
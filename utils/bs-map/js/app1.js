var basemap = new L.TileLayer(baseUrl, {maxZoom: 17, attribution: baseAttribution, subdomains: subdomains, opacity: opacity});

var center = new L.LatLng(55.7, 37.6);

var map = new L.Map('map', {center: center, zoom: 10, maxZoom: maxZoom, layers: [basemap]});


if(typeof(String.prototype.strip) === "undefined") {
    String.prototype.strip = function() {
        return String(this).replace(/^\s+|\s+$/g, '');
    };
}

$(document).ready( function() {
    var metro_lines = new L.geoJson(
        null, {style: 
            function(feature) { 
                var color = "#ff0000";
                if (Object.keys(feature.properties['TRAVELS']).length == 0) // empty
                    {
                     color = "#000000"}
                else {
                     color = feature.properties.color
                }
                return( {color: color}); 
            } 
        });
    metro_lines.addTo(map);

    $.ajax({
        dataType: "json",
        url: metroLinesURL,
        success: function(data) {
            $(data.features).each(function(key, data) {
                // console.log(key, data);
                metro_lines.addData(data);
            });
        }
    }).error(function() {});
    
});

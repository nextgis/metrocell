var basemap = new L.TileLayer(baseUrl, {maxZoom: 17, attribution: baseAttribution, subdomains: subdomains, opacity: opacity});

var center = new L.LatLng(55.7, 37.6);

var map = new L.Map('map', {center: center, zoom: 10, maxZoom: maxZoom, layers: [basemap]});


if(typeof(String.prototype.strip) === "undefined") {
    String.prototype.strip = function() {
        return String(this).replace(/^\s+|\s+$/g, '');
    };
 }


var info = L.control();

info.onAdd = function (map) {
    this._div = L.DomUtil.create('div', 'info'); 
    this.update();
    return this._div;
};


get_travel_info = function(travel){
    if (Object.keys(travel).length == 0){
        s = '---';
    } else{
        s = '<b>Operators: <br />' + Object.keys(travel)[0] + '</b><br />';
    }
    return s;
    };

info.update = function (props) {
    this._div.innerHTML = '<h4>Info</h4>' +  (props ?
        '<b>Line code: ' + props.CODE + '</b><br />' + get_travel_info(props.TRAVELS)
        : 'Hover a line');
};

info.addTo(map);



function highlightFeature(e, style) {
    var layer = e.target;

    if (style === "undefined"){
       style = {
            weight: 5,
            color: '#666',
            dashArray: '',
            fillOpacity: 0.7
        }
        };
    layer.setStyle(style);
    
    info.update(layer.feature.properties);

    if (!L.Browser.ie && !L.Browser.opera) {
        layer.bringToFront();
    }
};

function resetHighlight(e) {
    highlightFeature(e.target);
    info.update();
};

function onEachFeature(feature, layer) {
    layer.on({
        mouseover: highlightFeature,
        mouseout: highlightFeature
    });
};


function default_style(feature) { 
            var color = "#ff0000";
            if (Object.keys(feature.properties['TRAVELS']).length == 0) // empty
                {
                 color = "#000000"}
            else {
                 color = feature.properties.color
            }
            return( {color: color, opacity: 1}); 
        } 

var metro_lines = new L.geoJson(
    null, 
    {
        onEachFeature: onEachFeature,
        style: default_style
        
    });
metro_lines.addTo(map);


$(document).ready( function() {
    $.ajax({
        dataType: "json",
        url: metroLinesURL,
        success: function(data) {
            $(data.features).each(function(key, data) {
                metro_lines.addData(data);
            });
        }
    }).error(function() {});
    
});

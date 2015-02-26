var basemap = new L.TileLayer(baseUrl, {maxZoom: 17, attribution: baseAttribution, subdomains: subdomains, opacity: opacity});

var center = new L.LatLng(55.7, 37.6);

var map = new L.Map('map', {center: center, zoom: 10, maxZoom: maxZoom, layers: [basemap]});


if(typeof(String.prototype.strip) === "undefined") {
    String.prototype.strip = function() {
        return String(this).replace(/^\s+|\s+$/g, '');
    };
 }

var popupOpts = {
    autoPanPadding: new L.Point(5, 50),
    autoPan: true
};

var points = L.geoCsv (null, {
    firstLineTitles: true,
    fieldSeparator: fieldSeparator,
    pointToLayer: function (feature, latlng) {
        var iconUrl = 'img/metro.gif';
        return new L.Marker(latlng, {
            icon: new L.Icon({
                iconSize: [16, 16],
                iconUrl: iconUrl
            })
        })
    },
    onEachFeature: function (feature, layer) {
        var popup = '<div class="popup-content"><table class="table table-striped table-bordered table-condensed">';
        for (var clave in feature.properties) {
            var title = points.getPropertyTitle(clave).strip();
            var attr = feature.properties[clave];
            if (title == labelColumn) {
                layer.bindLabel(feature.properties[clave], {className: 'map-label'});
            }
            if (attr.indexOf('http') === 0) {
                attr = '<a target="_blank" href="' + attr + '">'+ attr + '</a>';
            }
            if (attr) {
                popup += '<tr><th>'+title+'</th><td>'+ attr +'</td></tr>';
            }
        }
        popup += "</table></popup-content>";
        layer.bindPopup(popup, popupOpts);
    }
});
points.addTo(map);

var info = L.control();

info.onAdd = function (map) {
    this._div = L.DomUtil.create('div', 'info'); 
    this.update();
    return this._div;
};


get_travel_info = function(travel){
    if (travel.length == 0){
        s = '---';
    } else{
        s = '';
        for (i in travel){
            stat = travel[i];
            descr = stat['operator'];
            descr += '/' + stat['net'] + ': ';
            descr +=  stat['count'] + '<br />';
            s += descr;
        };
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
            if ((feature.properties['TRAVELS']).length == 0) // empty
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
            $(data.features).each(function(key, feat) {
                metro_lines.addData(feat);
            });
        }
    });
    $.ajax ({
        type:'GET',
        dataType:'text',
        url: stationsUrl,
        error: function() {
            console.log('Download error');
        },
        success: function(csv) {
            points.addData(csv);
        }
    });
});

// Based on code by:
//  Will Turnman http://bl.ocks.org/WillTurman/4631136 
//  Harry J. Stevens https://bl.ocks.org/HarryStevens/c893c7b441298b36f4568bc09df71a1e
//  Released under same license SPDX https://bl.ocks.org/licenses.txt

function getParamValue(paramName)
{
    var url = window.location.search.substring(1); //get rid of "?" in querystring
    var qArray = url.split('&'); //get key-value pairs
    for (var i = 0; i < qArray.length; i++) 
    {
        var pArr = qArray[i].split('='); //split key and value
        if (pArr[0] == paramName) 
            return pArr[1]; //return value
    }
}

chart(getParamValue("csvpath"), getParamValue("layerType"));

// Check breakpoint
function breakCalc(x) {
    x <= 480 ? y = 'xs' : y = 'md';
    return y;
}

var breakpoint = breakCalc($(window).width());

$(window).resize(function() {
    breakpoint = breakCalc($(window).width());
})

// change the height of the chart depending on the breakpoint
function breakHeight(bp) {
    bp == 'xs' ? y = 250 : y = 500;
    return y;
}

// function to ensure the tip doesn't hang off the side
function tipX(x) {
    var winWidth = $(window).width();
    var tipWidth = $('.tip').width();
    if (breakpoint == 'xs') {
        x > winWidth - tipWidth - 20 ? y = x - tipWidth : y = x;
    } else {
        x > winWidth - tipWidth - 30 ? y = x - 45 - tipWidth : y = x + 10;
    }
    return y;
}

function chart(csvpath, layerType) {

    window.colorrange = ['#66c2a5', '#fc8d62', '#8da0cb', '#e78ac3', '#a6d854', '#ffd92f'];
  
    var format = d3.time.format("%Y");
    var currencyFormat = d3.format("$.3s");
    //var format = d3.time.format("%m/%d/%y");

    var margin = {
        top: 20,
        right: 20,
        bottom: 30,
        left: 15
    };
    var width = document.body.clientWidth - margin.left - margin.right;
    var height = breakHeight(breakpoint) - margin.top - margin.bottom;

    // chart top used for placing the tooltip
    var chartTop = $('.chart').offset().top;

    // tooltip
    var tooltip = d3.select("body")
        .append("div")
        .attr("class", "tip")
        .style("position", "absolute")
        .style("z-index", "20")
        .style("visibility", "hidden")
        .style("top", 40 + chartTop + "px");

    var x = d3.time.scale()
        .range([0, width]);

    var y = d3.scale.linear()
        .range([height - 10, 0]);

    var z = d3.scale.ordinal()
        .range(window.colorrange);

    var xAxis = d3.svg.axis()
        .scale(x)
        .orient("bottom")
        .outerTickSize(0);

    d3.svg.axis()
        .scale(y);

    d3.svg.axis()
        .scale(y);

    var stack = d3.layout.stack()
        .offset("silhouette")
        .values(function(d) {
            return d.values;
        })
        .x(function(d) {
            return d.date;
        })
        .y(function(d) {
            return d.value;
        });

    var nest = d3.nest()
        .key(function(d) {
            return d.key;
        });

    var area = d3.svg.area()
        .interpolate("cardinal")
        .x(function(d) {
            return x(d.date);
        })
        .y0(function(d) {
            return y(d.y0) - 0.2;
        })
        .y1(function(d) {
            return y(d.y0 + d.y) + 0.2;
        });

    var svg = d3.select(".chart").append("svg")
        .attr("width", width + margin.left + margin.right)
        .attr("height", height + margin.top + margin.bottom)
        .attr("background", "fcfcfc")
        .append("g")
        .attr("transform", "translate(" + margin.left + "," + margin.top + ")");

    // generate a legend
    function legend(layers, layerType) {

        $('.chart').prepend('<div class="legend"><div class="title">' + layerType + '</div></div>');
        $('.legend').hide();
        var legend = [];
        layers.forEach(function(d, i) {
            var obj = {};
            if (i < 6) {
                obj.key = d.key;
                obj.color = window.colorrange[i];
                legend.push(obj);
            }
        });

        // others
        if (layers.length > 6) {
            legend.push({
                key: "Other",
                color: "#b3b3b3"
            });
        }

        legend.forEach(function(d, i) {
            $('.legend').append('<div class="item"><div class="swatch" style="background: ' + d.color + '"></div>' + d.key + '</div>');
        });

        $('.legend').fadeIn(300);

    }

    d3.csv(csvpath, function(data) {
        data.forEach(function(d) {
            d.date = format.parse(d.date);
            d.value = +d.value;
        });

        var layers = stack(nest.entries(data));

        legend(layers, layerType);

        var lastYear = d3.max(data, function(d) {
            return d.date.getFullYear();
        });

        x.domain(d3.extent(data, function(d) {
            return d.date;
        }));
        y.domain([0, d3.max(data, function(d) {
            return d.y0 + d.y;
        })]);

        svg.selectAll(".layer")
            .data(layers)
            .enter().append("path")
            .attr("class", "layer")
            .attr("d", function(d) {
                return area(d.values);
            })
            .style("fill", function(d, i) {
                return z(i);
            });

        svg.append("g")
            .attr("class", "x axis")
            .attr("transform", "translate(0," + height + ")")
            .call(xAxis);

        svg.selectAll(".layer")
            .attr("opacity", 1)
            .on("mouseover", function(d, i) {
                svg.selectAll(".layer").transition()
                    .duration(250)
                    .attr("opacity", function(d, j) {
                        return j != i ? 0.6 : 1;
                    });
            })

            .on("mousemove", function(d, i) {
                var color = d3.select(this).style('fill');
                var mousex = d3.mouse(this);
                mousex = mousex[0];
                var invertedx = x.invert(mousex);
                var year = invertedx.getFullYear();
                var month = invertedx.getMonth();
                index = lastYear - year;
                if(month >= 6) {
                    index--;
                    year++;
                }
                
                var gross = d.values[index].value;

                tooltip.style("left", tipX(mousex) + "px")
                    .html("<div class='year'>" + year + "</div><div class='key'><div style='background:" + color + "' class='swatch'>&nbsp;</div>" + d.key + "</div><div class='value'>" + currencyFormat(gross) + ' USD' + "</div>")
                    .style("visibility", "visible");
            })
            .on("mouseout", function(d, i) {
                svg.selectAll(".layer")
                    .transition()
                    .duration(250)
                    .attr("opacity", "1");
                tooltip.style("visibility", "hidden");
            });

        var vertical = d3.select(".chart")
            .append("div")
            .attr("class", "remove")
            .style("position", "absolute")
            .style("z-index", "19")
            .style("width", "1px")
            .style("height", "380px")
            .style("top", "10px")
            .style("bottom", "30px")
            .style("left", "0px")
            .style("background", "#fff");

        d3.select(".chart")
            .on("mousemove", function() {
                var mousex = d3.mouse(this);
                mousex = mousex[0] + 5;
                vertical.style("left", mousex + "px");
            })
            .on("mouseover", function() {
                var mousex = d3.mouse(this);
                mousex = mousex[0] + 5;
                vertical.style("left", mousex + "px");
            });

        // Add 'curtain' rectangle to hide entire graph
        svg.append('rect')
            .attr('x', -1 * width)
            .attr('y', -1 * height)
            .attr('height', height)
            .attr('width', width)
            .attr('class', 'curtain')
            .attr('transform', 'rotate(180)')
            .style('fill', '#fcfcfc');

        // Create a shared transition for anything we're animating
        var t = svg.transition()
            .delay(100)
            .duration(1000)
            .ease('exp')
            .each('end', function() {
                d3.select('line.guide')
                    .transition()
                    .style('opacity', 0)
                    .remove();
            });

        t.select('rect.curtain')
            .attr('width', 0);
        t.select('line.guide')
            .attr('transform', 'translate(' + width + ', 0)');
    });
}
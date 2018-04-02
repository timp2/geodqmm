/*
 * Construct an individual address entry for the addressInput
 * attribute of a batch request.
 */
function constructAddressEntry(input) {
	var addressInput = {};

	// Map geocode index address fields to dqmm input fields.

	if (input.hasOwnProperty("country")) {
		addressInput.country = input.country;
    }
    else {
        addressInput.country = "";
    }

	if (input.hasOwnProperty("state")) {
		addressInput.region = input.state;
	}
	else {
	    addressInput.region = "";
	}

	if (input.hasOwnProperty("county")) {
		addressInput.mixed4 = input.county;
	}

	if (input.hasOwnProperty("city")) {
		addressInput.locality = input.city;
	}
	else {
	    addressInput.locality = "";
	}

	if (input.hasOwnProperty("postal_code")) {
		addressInput.postcode = input.postal_code;
	}
	else {
	    addressInput.postcode = "";
	}

	if (input.hasOwnProperty("district")) {
		addressInput.locality2 = input.district;
	}

	if (input.hasOwnProperty("street")) {
		addressInput.mixed3 = input.street;
	}

	if (input.hasOwnProperty("house_number")) {
		addressInput.mixed2 = input.house_number;
	}

	if (input.hasOwnProperty("address_line")) {
		addressInput.mixed = input.address_line;
	}

    return addressInput;
}


/*
 * Return the WKT version of the geocode as POINT(long lat).
 */
function mkPoint(latitude, longitude) {
	// If either the longitude or latitude are null then there was a problem
	// assigning the geocode and we are not able to form a valid point.
	if (longitude === null || latitude === null) {
		return null;
	}

    return "POINT(" + longitude.toString() + " " + latitude.toString() + ")";
}


/*
 * Request a new token from the token endpoint.
 */
function requestNewToken() {
    var dest = null;

    // Get the token endpoint destination.
    try {
        dest = $.net.http.readDestination("sap.hana.spatial.geocoding.dqmm", "token");
    }
    catch (e1) {
    	$.trace.error("Unable to read the token endpoint destination.");
    	$.trace.error(e1);
    	return null;
    }

    var req = new $.net.http.Request($.net.http.POST, "");
    var client = new $.net.http.Client();
    var resp;

    // Call the token endpoint to retrieve a new token.
    try {
        client.request(req, dest);
        resp = client.getResponse();

        // If the call was not successful then return null.
        if (resp.status !== $.net.http.OK) {
        	$.trace.error("Error returned from token request (" + resp.status + ").");
        	client.close();
        	return null;
        }
    }
    catch (e2) {
    	$.trace.error("Unable to send request to token endpoint.");
    	$.trace.error(e2);
    	return null;
    }
    finally {
        client.close();
    }

    return resp;
}


/*
 * Get a token. First check if the token in the securestore
 * is valid. If not, then request a new token.
 */
function getToken() {
    var tokenExpiration = { name: "expiration" };
    var tokenValue = { name: "value" };
    var expiration = 0;
    var value = null;
    var store = null;

    // Get the stored token.
    try {
        store = new $.security.Store("token.xssecurestore");
        expiration = store.read(tokenExpiration);
        value = store.read(tokenValue);
    }
    catch (e1) {
        $.trace.error("Unable to read token information from the secure store.");
        return null;
    }

    // Check if the token we have is still valid.
    if (expiration !== null) {
        if (Date.now() < expiration) {
            return value;
        }
    }

    // If the token has expired then request a new token.
    var tokenResp = requestNewToken();

    // If there was a problem getting the token then
    // return null.
    if (tokenResp === null) {
        return null;
    }

	var result = JSON.parse(tokenResp.body.asString());

    // Store the new token in the secure store.
    expiration = Date.now() + (result.expires_in * 1000);
    tokenExpiration = { name: "expiration", value: expiration.toString()};
    tokenValue = { name: "value", value: result.access_token};

    try {
        store = new $.security.Store("token.xssecurestore");
        store.store(tokenExpiration);
        store.store(tokenValue);
    }
    catch (e2) {
        $.trace.error("Unable to write the new token to the secure store.");
        $.trace.error(e2);
    }

    return result.access_token;
}


/*
 * Construct the addressInput array for a batch request.
 */
function constructAddressInput(input) {
    var addressInput = [];
    
    // Construct the individual address entries used to
    // populate the addressInput array.
    var entry;
    for ( var i = 0; i < input.entries.length; ++i ) {
    	entry = input.entries[i];
        addressInput.push(constructAddressEntry(entry));
    }

    return addressInput;
}


/*
 * Convert the results from the service into a format
 * appropriate for returning to spatial.
 */
function constructResult(resp) {
    var point;
    var geocodes = [];
    var errors = [];

    var result = JSON.parse(resp.body.asString());
        
    for (var i = 0; i < result.addressOutput.length; ++i ) {
        point = null;
        point = mkPoint(result.addressOutput[i].addr_latitude, result.addressOutput[i].addr_longitude);
        geocodes.push(point);
        if (point === null) {
            errors.push(400);
        }
    }
    
    return { geocodes : geocodes, errors : errors };
}


/*
 * Method called by other doGeocoding functions.
 */
function doGeocoding(input, mode) {
    var result = { geocodes : null, errors : null };

    $.trace.debug("in doGeocoding with " + input.entries.length.toString() + " records");

    var body = { outputFields : ["addr_latitude", "addr_longitude"], addressSettings : { processingMode : mode, geoAssign : "houseNumberOnly" } };
    body.addressInput = constructAddressInput(input);

    // Get an OAuth token.    
    var token = getToken();
    
    // If we were unable to obtain a token then don't even try
    // to process the records.
    if (token === null) {
        return result;
    }

    // Get the DQMm service destination.
    var dest;
    try {
        dest = $.net.http.readDestination("sap.hana.spatial.geocoding.dqmm", "dqmm");
    }
    catch (e1) {
    	$.trace.error("Unable to read the DQMm service destination.");
    	$.trace.error(e1);
    	return { geocodes:null, errors:null };
    }

    // Construct the request with the appropriate headers.
    var req = new $.net.http.Request($.net.http.POST, "/addressCleanse/batch");
    req.contentType = "application/JSON";
    req.headers.set("Authorization", "Bearer " + token);
    req.setBody(JSON.stringify(body));

    // Create client
    var client = new $.net.http.Client();

    // Call the service.
    try {
        client.request(req, dest);

        var resp = client.getResponse();

        // If the token expired while we were processing records then
        // request a new token.
        if (resp.status === $.net.http.UNAUTHORIZED) {
            token = getToken();
            if (token !== null) {
                req.headers.set("Authorization", "Bearer " + token);
                client.request(req, dest);
                resp = client.getResponse();
            }
        }
        
        // If the call was successful then create
        // point values from the result.
        if (resp.status === $.net.http.OK) {
            result = constructResult(resp);
        }
        else {
        	$.trace.error("Error returned from service (" + resp.status + ").");
            $.trace.error(body);
        }
    }
    catch (e3) {
    	$.trace.error("Unable to send request to DQMm service.");
    	$.trace.error(e3);
    }

    client.close();
    
    return result;
}

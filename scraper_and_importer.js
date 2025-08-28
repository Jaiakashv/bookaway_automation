import fetch from "node-fetch";
import fs from 'fs';
import pg from 'pg';
import dotenv from 'dotenv';
import { Console } from "console";

// Load environment variables from .env file (for local testing)
dotenv.config();

const { Client } = pg;
const BASE_URL = "https://www.bookaway.com";
const SEARCH_URL = `${BASE_URL}/_api/search/composite/v1/search-results`;

// List of all supported currencies
const SUPPORTED_CURRENCIES = ['PHP', 'THB', 'MAD', 'TRY', 'VND', 'JOD', 'EUR', 'LKR', 'EGP', 'USD', 'INR'];
const EXCHANGE_RATE_API = 'https://api.exchangerate.host/latest?base=INR&symbols=' + SUPPORTED_CURRENCIES.join(',');

// Cache for exchange rates
let exchangeRates = null;

// The database connection URL from the environment variable (or a default for testing)
const DB_CONN_STRING = process.env.DATABASE_URL || "your_neon_connection_string";

/**
 * Converts an amount from any currency to INR
 * @param {number} amount - The amount to convert
 * @param {string} fromCurrency - The source currency code
 * @param {Object} rates - Exchange rates object
 * @returns {number} The converted amount in INR
 */
function convertToINR(amount, fromCurrency, rates) {
    if (!amount) return 0;
    
    const currency = fromCurrency.toUpperCase();
    
    // If already in INR, return as is
    if (currency === 'INR') return Math.round(amount);
    
    // Get the exchange rate for the currency
    const rate = rates[currency];
    
    if (!rate) {
      console.warn(`No exchange rate found for ${currency}, using 1:1 conversion`);
      return Math.round(amount);
    }
    
    // Convert to INR: amount * rate
    // Since rates are stored as 1 unit of currency = X INR
    const inrAmount = amount * rate;
    return Math.round(inrAmount);
}

/**
 * Fetches the latest exchange rates from ExchangeRate.host API
 * @returns {Promise<Object>} Object containing exchange rates
 */
async function getExchangeRates() {
    if (exchangeRates) {
      return exchangeRates; // Return cached rates if available
    }

    try {
      console.log('Fetching latest exchange rates...');
      const response = await fetch(EXCHANGE_RATE_API);
      if (!response.ok) {
        throw new Error(`Failed to fetch exchange rates: ${response.statusText}`);
      }
      const data = await response.json();
      
      if (!data.rates || !data.success) {
        throw new Error('Invalid exchange rate data received');
      }
      
      // Store the rates and ensure INR is 1
      exchangeRates = { ...data.rates, INR: 1 };
      return exchangeRates;
    } catch (error) {
      console.error('Error fetching exchange rates, using fallback rates:', error.message);
      // Fallback to default rates if API fails
      return {
        PHP: 0.65,      // 1 INR = 0.65 PHP
        THB: 0.37,      // 1 INR = 0.37 THB
        MAD: 0.10,      // 1 INR = 0.10 MAD
        TRY: 0.33,      // 1 INR = 0.33 TRY
        VND: 258.00,    // 1 INR = 258.00 VND
        JOD: 0.0078,    // 1 INR = 0.0078 JOD
        EUR: 0.0093,    // 1 INR = 0.0093 EUR
        LKR: 3.32,      // 1 INR = 3.32 LKR
        EGP: 0.31,      // 1 INR = 0.31 EGP
        USD: 0.011,     // 1 INR = 0.011 USD
        INR: 1          // Base currency
      };
    }
}

// Load and process cookies from cookies.json
let cookies = [];
try {
    const cookiesData = fs.readFileSync('./cookies.json', 'utf8');
    const rawCookies = JSON.parse(cookiesData);
    
    // Filter and format cookies
    cookies = rawCookies
        .filter(cookie => cookie.domain && 
                (cookie.domain.includes('bookaway.com') || 
                  cookie.domain.includes('.bookaway.com')) &&
                !cookie.httpOnly)  // Skip httpOnly cookies as they can't be accessed by JS
        .map(cookie => ({
            name: cookie.name,
            value: decodeURIComponent(cookie.value),  // Decode URL-encoded values
            domain: cookie.domain,
            path: cookie.path || '/',
            secure: cookie.secure || false,
            httpOnly: cookie.httpOnly || false,
            expires: cookie.expirationDate ? new Date(cookie.expirationDate * 1000) : null
        }));
        
    console.log(`Loaded ${cookies.length} cookies from cookies.json`);
    
} catch (err) {
    console.error('Error reading or parsing cookies.json:', err);
    process.exit(1);
}

// Create cookie header string
const cookieHeader = cookies
    .filter(cookie => {
        const isExpired = cookie.expires && cookie.expires < new Date();
        if (isExpired) {
            console.log(`Skipping expired cookie: ${cookie.name}`);
            return false;
        }
        return true;
    })
    .map(cookie => `${cookie.name}=${encodeURIComponent(cookie.value)}`)
    .join('; ');

console.log('Cookie header length:', cookieHeader.length);

const headers = {
    "authority": "www.bookaway.com",
    "accept": "application/json, text/plain, */*",
    "accept-encoding": "gzip, deflate, br, zstd",
    "accept-language": "en-US,en;q=0.9,ta;q=0.8,pt;q=0.7",
    "content-type": "application/json",
    "cookie": cookieHeader,
    "lang": "en",
    "origin": "https://www.bookaway.com",
    "priority": "u=1, i",
    "referer": "https://www.bookaway.com/s/thailand/bangkok-to-chiang-mai?departuredate=2025-08-24&adult=1",
    "sec-ch-ua": '"Google Chrome";v="139", "Not;A=Brand";v="8", "Chromium";v="139"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": "Windows",
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36",
    "x-distribution-channel": "bookaway"
};

// helper: format date to YYYY-MM-DD
function formatDate(date) {
    return date.toISOString().split("T")[0];
}

// helper: today + N days
function getFutureDate(daysAhead) {
    const d = new Date();
    d.setDate(d.getDate() + daysAhead);
    return formatDate(d);
}

// fetch function
async function fetchRoutes(fromSlug, toSlug, daysAhead = 0) {
    const date = getFutureDate(daysAhead);
    
    // Prepare the request payload
    const payload = {
      fromSlug: fromSlug.toLowerCase(),
      toSlug: toSlug.toLowerCase(),
      legs: [{
        fromSlug: fromSlug.toLowerCase(),
        toSlug: toSlug.toLowerCase(),
        date: date
      }],
      departureDate: date,
      filter: {
        passengersAmount: 1,
        passengerTypes: [{
          slug: "adult",
          defaultAge: "30"
        }]
      },
      resultsOrder: false,
      searchRadiusInMeters: 1000,
      supplier: {
        supplier: {
          code: "BAW"
        }
      },
      suppliers: [
        {
          supplier: {
            code: "BAW"
          }
        },
        {
          supplier: {
            code: "TRV",
            supplierId: "64cb7cafdff7a93b3203f82b"
          }
        }
      ]
    };

    // Headers that match the website's request
    const requestHeaders = {
      'accept': 'application/json, text/plain, */*',
      'accept-language': 'en-US,en;q=0.9',
      'content-type': 'application/json',
      'origin': BASE_URL,
      'referer': `${BASE_URL}/s/thailand/bangkok-to-chiang-mai?departuredate=${date}&adult=1`,
      'sec-ch-ua': '"Google Chrome";v="123", "Not:A-Brand";v="8"',
      'sec-ch-ua-mobile': '?0',
      'sec-ch-ua-platform': '"Windows"',
      'sec-fetch-dest': 'empty',
      'sec-fetch-mode': 'cors',
      'sec-fetch-site': 'same-origin',
      'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
      'x-distribution-channel': 'bookaway'
    };
    
    // Add cookies if available
    if (cookies.length > 0) {
      requestHeaders['cookie'] = cookies
        .map(cookie => `${cookie.name}=${cookie.value}`)
        .join('; ');
    }

    try {
      console.log('Sending request to:', SEARCH_URL);
      console.log('Request payload:', JSON.stringify(payload, null, 2));
      
      const res = await fetch(SEARCH_URL, {
        method: 'POST',
        headers: requestHeaders,
        body: JSON.stringify(payload)
      });

      console.log('Response status:', res.status);
      console.log('Response headers:', JSON.stringify([...res.headers.entries()], null, 2));
      
      if (!res.ok) {
        const errorText = await res.text();
        console.error('Error response body:', errorText);
        throw new Error(`HTTP error! status: ${res.status}, body: ${errorText}`);
      }

      const data = await res.json();
      
      // Debug: Log the raw API response structure
      console.log('API Response Keys:', Object.keys(data));
      if (data.trips) {
        console.log(`Found ${data.trips.length} trips`);
        console.log('First trip keys:', data.trips[0] ? Object.keys(data.trips[0]) : 'No trips');
        if (data.trips[0]?.legs) {
          console.log(`First trip has ${data.trips[0].legs.length} legs`);
          console.log('First leg keys:', data.trips[0].legs[0] ? Object.keys(data.trips[0].legs[0]) : 'No legs');
        }
      }
      
      // Check if we have any trips in the response
      if (!data) {
        console.error('No data in response');
        return { success: true, data: [] };
      }
      
      if (!data.trips || !Array.isArray(data.trips)) {
        console.error('No trips array in response, available keys:', Object.keys(data));
        return { success: true, data: [] };
      }
      
      if (data.trips.length === 0) {
        console.log('No trips found in the response');
        return { success: true, data: [] };
      }
      
      console.log(`Found ${data.trips.length} trips in the response`);
      
      // Get exchange rates before processing trips
      const rates = await getExchangeRates();
      
      console.log(`Using exchange rates. 1 INR = ${rates.THB} THB`);
      
      // Extract and format the trips
      const formattedRoutes = [];
      
      data.trips.forEach(trip => {
        // Skip if no legs or empty legs array
        if (!trip.legs || !Array.isArray(trip.legs) || trip.legs.length === 0) {
          console.log('Skipping trip with no legs');
          return;
        }

        // Process each leg of the trip
        trip.legs.forEach(leg => {
          const legType = leg.type?.toLowerCase() || '';
          const priceObj = leg.price || {};
          
          // Process each journey in the leg
          (leg.journeys || []).forEach(journey => {
            const departure = journey.departure || {};
            const arrival = journey.arrival || {};
            
            if (!departure?.date || !arrival?.date) {
              console.log('Skipping journey - missing departure/arrival date');
              return;
            }

            // Format duration from ISO 8601 format (PT14H30M) to readable format (14h 30m)
            const formatDuration = (duration) => {
              if (!duration) return null;
              const timeStr = duration.replace('PT', '');
              const hoursMatch = timeStr.match(/(\d+)H/);
              const minutesMatch = timeStr.match(/(\d+)M/);
              
              const hours = hoursMatch ? hoursMatch[1] : '0';
              const minutes = minutesMatch ? minutesMatch[1] : '0';
              
              return `${hours}h${minutes !== '0' ? ` ${minutes}m` : ''}`.trim();
            };
            
            const duration = formatDuration(journey.duration || leg.duration);
            const journeyPriceObj = journey.price || priceObj;
            
            // Get currency information
            const currency = (journeyPriceObj.originalCurrency || journeyPriceObj.currency || 'THB').toUpperCase();
            const amount = journeyPriceObj.originalAmount || journeyPriceObj.amount || 0;
            
            // Log currency information for debugging
            console.log(`Processing price: ${amount} ${currency}`);
            
            // Format city names for display
            const formatCityName = (slug) => {
              return slug.split('-').map(word => 
                word.charAt(0).toUpperCase() + word.slice(1).toLowerCase()
              ).join(' ');
            };
            
            formattedRoutes.push({
              route_url: `https://www.bookaway.com/s/${fromSlug}/${toSlug}`,
              From: formatCityName(fromSlug),
              To: formatCityName(toSlug),
              Date: date,
              'Departure Time': departure.date ? 
                new Date(departure.date).toISOString().replace('T', ' ').substring(0, 19) : 
                null,
              'Arrival Time': arrival.date ? 
                new Date(arrival.date).toISOString().replace('T', ' ').substring(0, 19) : 
                null,
              'Transport Type': legType === 'minivan' ? 'bus' : legType,
              'Duration': duration,
              'Price': amount,
              'price_inr': convertToINR(amount, currency, rates),
              'currency': currency,
              'Operator': journey.companyName || leg.companyName || trip.supplierName || null,
              'provider': 'bookaway'
            });
          }); // End of journeys loop
        }); // End of legs loop
      }); // End of trips loop
      
      console.log(`Formatted ${formattedRoutes.length} routes from trips`);
      
      return {
        success: true,
        data: formattedRoutes,
        timestamp: new Date().toISOString()
      };
    } catch (error) {
      return {
        success: false,
        error: error.message,
        route: `${fromSlug} to ${toSlug}`,
        timestamp: new Date().toISOString()
      };
    }
}

// Function to convert a name to URL-friendly slug
function toSlug(name) {
    return name
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, '-')
      .replace(/(^-|-$)/g, '');
}

// Load routes from routes_id.json
let routesList = [];
try {
    const routesData = fs.readFileSync('./routes_id.json', 'utf8');
    const routes = JSON.parse(routesData);
    
    // Convert route names to slugs
    routesList = routes.map(route => ({
      fromSlug: toSlug(route.from_name),
      toSlug: toSlug(route.to_name),
      fromName: route.from_name,
      toName: route.to_name
    }));
    
    console.log(`Loaded ${routesList.length} routes from routes_id.json`);
} catch (error) {
    console.error('Error loading routes_id.json:', error.message);
    process.exit(1);
}

// Helper functions for parsing and inserting data
const parseDurationMinutes = (v) => {
    if (!v) return null;
    const s = String(v).toLowerCase().trim();
    let total = 0;
    const h = s.match(/(\d+)\s*h/);
    const m = s.match(/(\d+)\s*m/);
    if (h) total += parseInt(h[1], 10) * 60;
    if (m) total += parseInt(m[1], 10);
    if (total > 0) return total;
    const hm = s.match(/^(\d{1,2}):(\d{2})$/);
    if (hm) {
      return parseInt(hm[1], 10) * 60 + parseInt(hm[2], 10);
    }
    const num = parseInt(s, 10);
    return Number.isFinite(num) ? num : null;
};

const parsePriceNumber = (v) => {
    if (!v) return null;
    const num = parseFloat(String(v).replace(/[,\s]/g, ''));
    return Number.isFinite(num) ? num : null;
};

const parseTimestamp = (v) => {
    if (!v) return null;
    const d = new Date(v);
    return isNaN(d.getTime()) ? null : d.toISOString();
};

const parseDate = (v) => {
    if (!v) return null;
    const d = new Date(v);
    if (isNaN(d.getTime())) return null;
    return d.toISOString().split('T')[0];
};

/**
 * Inserts a batch of records into the database.
 * @param {Array<Object>} records - The array of scraped records to insert.
 */
async function insertRecords(records) {
    const client = new Client({
        connectionString: DB_CONN_STRING,
        ssl: { rejectUnauthorized: false }
    });

    try {
        await client.connect();
        console.log("Database connection established.");

        const validTrips = records.filter(t => {
            return (
              t.Price !== undefined &&
              t.Price !== null &&
              parseFloat(t.Price) > 0 &&
              t["Departure Time"] &&
              t["Arrival Time"] &&
              t.route_url
            );
        });

        console.log(`Found ${records.length} total records from scraper`);
        console.log(`Importing ${validTrips.length} valid records`);
        console.log(`Skipping ${records.length - validTrips.length} records with missing/invalid data`);

        // Truncate the table before inserting new data
        console.log("Truncating 'bookaway_trips' table...");
        await client.query("TRUNCATE TABLE bookaway_trips RESTART IDENTITY;");
        console.log("Table successfully truncated. ✅");

        const BATCH_SIZE = 400;
        let imported = 0;

        for (let i = 0; i < validTrips.length; i += BATCH_SIZE) {
            const batch = validTrips.slice(i, i + BATCH_SIZE);
            const values = batch.map((_, idx) => {
                const base = idx * 13;
                return `($${base + 1},$${base + 2},$${base + 3},$${base + 4},$${base + 5},$${base + 6},$${base + 7},$${base + 8},$${base + 9},$${base + 10},$${base + 11},$${base + 12},$${base + 13})`;
            }).join(',');

            const flatParams = batch.flatMap(t => ([
              t.route_url,
              t.From,
              t.To,
              parseTimestamp(t["Departure Time"]),
              parseTimestamp(t["Arrival Time"]),
              t["Transport Type"],
              parseDurationMinutes(t.Duration) || 0,
              parsePriceNumber(t.Price) || 0,
              t.price_inr || 0,
              t.currency,
              parseDate(t.Date),
              t.Operator || null,
              t.provider
            ]));

            await client.query({
                text: `
                    INSERT INTO bookaway_trips (
                        route_url, origin, destination,
                        departure_time, arrival_time, transport_type,
                        duration_min, price, price_inr, currency,
                        travel_date, operator_name, provider
                    ) VALUES ${values}
                `,
                values: flatParams,
                rowMode: 'array'
            });

            imported += batch.length;
            console.log(`Progress: ${imported}/${validTrips.length} records imported`);
        }

        console.log(`✅ Import completed! Imported ${imported} records`);

    } catch (err) {
        console.error('Database error:', err);
        throw err;
    } finally {
        await client.end();
        console.log("Database connection closed.");
    }
}

// Main execution function
(async () => {
    try {
        console.log("Starting scraper...");
        const daysAhead = 1; 
        let allResults = [];

        for (const r of routesList) {
            console.log(`\n=== Fetching routes from ${r.fromSlug} to ${r.toSlug} ===`);
            const routeResults = await fetchRoutes(r.fromSlug, r.toSlug, daysAhead);
            
            if (routeResults.success) {
                console.log(`Found ${routeResults.data.length} routes`);
                allResults = allResults.concat(routeResults.data);
            } else {
                console.error(`Error fetching ${r.fromSlug} to ${r.toSlug}:`, routeResults.error);
            }
            
            // Add a small delay between requests to avoid rate limiting
            await new Promise(resolve => setTimeout(resolve, 1500));
        }

        if (allResults.length > 0) {
            console.log("Scraping finished. Now importing to database...");
            await insertRecords(allResults);
        } else {
            console.log("No data was scraped. Exiting without importing.");
        }
        
    } catch (error) {
        console.error('An error occurred during the process:', error);
        process.exit(1);
    }
})();
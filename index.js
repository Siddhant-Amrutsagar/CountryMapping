// index.js
import { createClient } from '@clickhouse/client';
import nlp from 'compromise';

const client = createClient({
    url: 'http://localhost:8123',
    password: 'clickhouse',
    database: 'default',
});

if (client) {
    console.log("clickhouse connected")
}

function cleanPlaceName(name) {
    return name.replace(/[.,;:!?]+$/, ''); // removes trailing punctuation
}

// Enhanced place extraction with fallback regex
function extractPlacesWithFallback(address) {
    const cleaned = address
        .replace(/[#]/g, '')
        .replace(/\b[Ss][Tt][.]?\s+(?=[A-Z])/g, 'Saint ')
        .replace(/\s+/g, ' ')
        .trim();

    console.log(cleaned);

    const doc = nlp(cleaned);
    const compromisePlaces = doc.places().out('array');
    console.log("compromisePlaces: ", compromisePlaces);
    const fallbackPlaces = [];

    // Match "St. Paul", "Saint Louis", etc.
    const saintRegex = /\b(St\.?|Saint)\s+[A-Z][a-z]+(?:\s[A-Z][a-z]+)?\b/g;
    let match;
    while ((match = saintRegex.exec(cleaned)) !== null) {
        fallbackPlaces.push(match[0]);
    }

    const cityRegex = /\b[A-Z][a-z]+(?:\s[A-Z][a-z]+)?\b/g;
    const junkWords = /^(PO|P\.O\.|Box|Post|Office|Suite|Street|St|Avenue|Ave|Blvd|Dr|Drive|Unit|Floor|Zip|Code|Phone|Mobile|Toll-Free|Toll|Email|email|E-mail|e-mail|Road|road|Rd|rd|Wing|wing|Flat No|Flat no|Line|line|Sec|sec|Sector|sector|Mr|mr|Mr\.|mr\.|Ms|ms|Ms\.|ms\.|Miss|miss|North|north|South|south|East|east|West|west|\d+)$/i;
    const junkPhrases = /^(PO|PO Box|P\.O\. Box|Post Office Box| SE)$/i;

    while ((match = cityRegex.exec(cleaned)) !== null) {
        const phrase = match[0];
        console.log("phrase: ", phrase);
        
        const words = phrase.split(/\s+/);
        console.log("words: ", words);
        
        const filteredWords = words.filter(w => !junkWords.test(w));

        if (filteredWords.length === 0) continue;

        const cleanedPhrase = filteredWords.join(' ');
        if (junkPhrases.test(cleanedPhrase)) continue;
        if (cleanedPhrase.length <= 2) continue;

        // Add full cleaned phrase
        fallbackPlaces.push(cleanedPhrase);

        // Also add each individual word (after filtering)
        for (const word of filteredWords) {
            if (word.length > 2 && !junkWords.test(word)) {
                fallbackPlaces.push(word);
            }
        }
    }




// Add short codes like "MN", "NY", etc.
const ignoredCodes = new Set(['PO', 'BOX', 'PIN', 'ZIP', 'FAX', 'TEL']);
const codeRegex = /\b[A-Z]{2,3}\b/g;
while ((match = codeRegex.exec(cleaned)) !== null) {
    const code = match[0];
    console.log("code: ", code);
    
    if (!ignoredCodes.has(code)) {
        fallbackPlaces.push(code);
    }
}

const allPlaces = [...new Set([
    ...compromisePlaces,
    ...fallbackPlaces,
])];

return allPlaces.map(cleanPlaceName);
}


async function fetchAndProcessAddresses() {
    const resultSet = await client.query({
        query: `SELECT domainid, fulladdress FROM addresses where match(fulladdress, 'india') LIMIT 10 offset 200`,
        format: 'JSONEachRow'
    });

    const rawRows = await resultSet.json();
    const grouped = {};

    for (const row of rawRows) {
        const { domainid, fulladdress } = row;
        if (!grouped[domainid]) grouped[domainid] = [];
        grouped[domainid].push(fulladdress);
    }

    for (const [domainid, addresses] of Object.entries(grouped)) {
        const allPlaces = new Set();

        addresses.forEach(address => {
            const foundPlaces = extractPlacesWithFallback(address);
            foundPlaces.forEach(p => allPlaces.add(p));
        });

        const placeToCountriesMap = {}; // { Pune: ['India', 'Brazil'], ... }
        const countryScore = {}; // { India: 3, Brazil: 1, ... }

        for (const place of allPlaces) {
            const normalizedPlace = place.trim().toLowerCase().replace(/'/g, "''");

            const query = `
        SELECT country FROM places_Countries WHERE lower(name) = '${normalizedPlace}'
    `;
            const result = await client.query({ query, format: 'JSONEachRow' });
            const rows = await result.json();

            const countries = rows.map(row => row.country);
            if (countries.length > 0) {
                placeToCountriesMap[place] = countries;

                for (const country of countries) {
                    countryScore[country] = (countryScore[country] || 0) + 1;
                }
            }
        }


        const finalPlaceToCountry = {};
        for (const [place, countries] of Object.entries(placeToCountriesMap)) {
            let bestCountry = countries[0];
            let maxScore = countryScore[bestCountry] || 0;

            for (const c of countries) {
                if ((countryScore[c] || 0) > maxScore) {
                    bestCountry = c;
                    maxScore = countryScore[c];
                }
            }
            finalPlaceToCountry[place] = bestCountry;
        }

        const finalCountries = [...new Set(Object.values(finalPlaceToCountry))];

        console.log({
            domainid,
            addresses,
            places: Array.from(allPlaces),
            countryMap: finalPlaceToCountry,
            countries: finalCountries
        });
    }
}



fetchAndProcessAddresses();


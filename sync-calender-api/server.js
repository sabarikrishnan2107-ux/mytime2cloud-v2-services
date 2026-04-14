const express = require('express');
const cors = require('cors');
const ical = require('node-ical');
const axios = require('axios');

const app = express();
const HOST = "0.0.0.0";
const PORT = 5780; // Updated port

app.use(cors());

const UAE_HOLIDAYS_URL = 'https://calendar.google.com/calendar/ical/en.ae%23holiday%40group.v.calendar.google.com/public/basic.ics';

app.get('/health', async (req, res) => {
    res.json({ "status": true });
});

app.get('/holidays/:year', async (req, res) => {
    const selectedYear = parseInt(req.params.year);

    try {
        const response = await axios.get(UAE_HOLIDAYS_URL);
        const data = ical.parseICS(response.data);

        let rawHolidays = [];

        for (let k in data) {
            const event = data[k];
            if (event.type === 'VEVENT') {
                const startDate = new Date(event.start);
                const name = event.summary;

                // 1. Filter by year and exclude non-public holiday events (like Ramadan start)
                if (startDate.getFullYear() === selectedYear &&
                    !name.toLowerCase().includes('ramadan') &&
                    !name.toLowerCase().includes('hajj')) {

                    rawHolidays.push({
                        date: startDate.toISOString().split('T')[0],
                        // Remove "(tentative)" and "Holiday" suffixes for cleaner grouping
                        cleanName: name.replace(/\(tentative\)|Holiday/gi, '').trim()
                    });
                }
            }
        }

        // 2. Sort chronologically
        rawHolidays.sort((a, b) => new Date(a.date) - new Date(b.date));

        // 3. Group into the required Payload format
        const finalPayloads = [];

        rawHolidays.forEach((item) => {
            const lastEntry = finalPayloads[finalPayloads.length - 1];

            // If it's the same holiday name, extend the existing entry
            if (lastEntry && lastEntry.name === item.cleanName) {
                lastEntry.end_date = item.date;
                const diffTime = Math.abs(new Date(lastEntry.end_date) - new Date(lastEntry.start_date));
                lastEntry.total_days = Math.ceil(diffTime / (1000 * 60 * 60 * 24)) + 1;
            } else {
                // New unique holiday
                finalPayloads.push({
                    name: item.cleanName,
                    total_days: 1,
                    start_date: item.date,
                    end_date: item.date,
                    year: selectedYear
                });
            }
        });

        res.json(finalPayloads);

    } catch (error) {
        res.status(500).json({ error: "Failed to process holidays", details: error.message });
    }
});


app.listen(PORT, HOST, () => {
    console.log(`API is live on http://${HOST}:${PORT}/holidays/2026`);
});
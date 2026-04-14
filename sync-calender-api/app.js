const ical = require('node-ical');
const axios = require('axios');

// Google's public ICS URL for UAE Holidays
const UAE_HOLIDAYS_URL = 'https://calendar.google.com/calendar/ical/en.ae%23holiday%40group.v.calendar.google.com/public/basic.ics';

async function getUAEHolidays() {
    try {
        // 1. Fetch the ICS file data
        const response = await axios.get(UAE_HOLIDAYS_URL);
        
        // 2. Parse the ICS data
        const data = ical.parseICS(response.data);
        
        console.log("--- UAE Public Holidays 2026 ---");
        
        const holidays = [];

        for (let k in data) {
            const event = data[k];
            if (event.type === 'VEVENT') {
                const startDate = new Date(event.start);
                
                // 3. Filter only for the year 2026
                if (startDate.getFullYear() === 2026) {
                    holidays.push({
                        date: startDate.toISOString().split('T')[0],
                        name: event.summary
                    });
                }
            }
        }

        // Sort by date before printing
        holidays.sort((a, b) => new Date(a.date) - new Date(b.date))
                .forEach(h => console.log(`${h.date}: ${h.name}`));

    } catch (error) {
        console.error("Error fetching holidays:", error.message);
    }
}

getUAEHolidays();
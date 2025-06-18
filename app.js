const express = require('express');
const app = express();
const { Builder, By, until } = require('selenium-webdriver');
const chrome = require('selenium-webdriver/chrome');
const fs = require('fs');
const path = require('path');
const xlsx = require('xlsx');
const axios = require('axios');

const CONFIG = {
    INPUT_FILE: path.join(__dirname, 'data/input/VSKP1_data.xlsx'),
    OUTPUT_FILE: path.join(__dirname, 'data/output/VSKP2_data.xlsx'),
    FAILED_FILE: path.join(__dirname, 'data/failed/VSKP1_failed.json'),
    STATUS_FILE: path.join(__dirname, 'data/status.json'),
    URL: 'https://www.apeasternpower.com/viewBillDetailsMain',
    CHECK_INTERNET_URL: 'http://www.google.com',
    MAX_RETRIES: 3,
    RETRY_DELAY: 10000, 
    PORT: 3000
};

// ---------------------- GLOBAL STATE ----------------------
let shouldPause = false;
let shouldStop = false;
let scraperThread = null;
let driver = null;

// ---------------------- HELPER FUNCTIONS ----------------------
async function checkInternetConnection() {
    try {
        await axios.get(CONFIG.CHECK_INTERNET_URL, { timeout: 5000 });
        return true;
    } catch (error) {
        return false;
    }
}

async function waitForInternet() {
    console.log('🌐 Waiting for internet connection...');
    while (!(await checkInternetConnection())) {
        await new Promise(resolve => setTimeout(resolve, 5000));
    }
    console.log('🌐 Internet connection restored');
}

function loadStatus() {
    try {
        if (fs.existsSync(CONFIG.STATUS_FILE) && fs.statSync(CONFIG.STATUS_FILE).size > 0) {
            const data = fs.readFileSync(CONFIG.STATUS_FILE, 'utf8');
            return JSON.parse(data);
        }
    } catch (error) {
        console.log(`⚠ Couldn't read status file: ${error.message}`);
    }
    return { last_processed: 0, total_processed: 0 };
}

function saveStatus(lastProcessed, totalProcessed) {
    try {
        fs.writeFileSync(CONFIG.STATUS_FILE, JSON.stringify({
            last_processed: lastProcessed,
            total_processed: totalProcessed
        }));
    } catch (error) {
        console.log(`⚠ Couldn't save status file: ${error.message}`);
    }
}

function loadExistingData() {
    let existingData = {};
    let existingFailed = new Set();

    // Load successful data
    if (fs.existsSync(CONFIG.OUTPUT_FILE) && fs.statSync(CONFIG.OUTPUT_FILE).size > 0) {
        try {
            const workbook = xlsx.readFile(CONFIG.OUTPUT_FILE);
            const sheetName = workbook.SheetNames[0];
            const worksheet = workbook.Sheets[sheetName];
            existingData = xlsx.utils.sheet_to_json(worksheet);
        } catch (error) {
            console.log(`⚠ Couldn't read existing Excel file: ${error.message}`);
        }
    }

    // Load failed CIDs
    if (fs.existsSync(CONFIG.FAILED_FILE) && fs.statSync(CONFIG.FAILED_FILE).size > 0) {
        try {
            const data = fs.readFileSync(CONFIG.FAILED_FILE, 'utf8');
            existingFailed = new Set(JSON.parse(data));
        } catch (error) {
            console.log(`⚠ Couldn't read failed JSON file: ${error.message}`);
        }
    }

    return { existingData, existingFailed };
}

function saveData(outputData, notScraped) {
    try {
        // Convert outputData to worksheet
        const ws = xlsx.utils.json_to_sheet(outputData);
        const wb = xlsx.utils.book_new();
        xlsx.utils.book_append_sheet(wb, ws, 'Sheet1');
        xlsx.writeFile(wb, CONFIG.OUTPUT_FILE);

        // Save failed CIDs
        if (notScraped && notScraped.size > 0) {
            // Load existing failed CIDs to avoid duplicates
            let existingFailed = new Set();
            if (fs.existsSync(CONFIG.FAILED_FILE) && fs.statSync(CONFIG.FAILED_FILE).size > 0) {
                try {
                    const data = fs.readFileSync(CONFIG.FAILED_FILE, 'utf8');
                    existingFailed = new Set(JSON.parse(data));
                } catch (error) {
                    console.log(`⚠ Couldn't read failed JSON file: ${error.message}`);
                }
            }

            // Merge sets
            const mergedFailed = new Set([...existingFailed, ...notScraped]);
            fs.writeFileSync(CONFIG.FAILED_FILE, JSON.stringify([...mergedFailed], null, 4));
            console.log(`⚠ Failed CIDs saved to ${CONFIG.FAILED_FILE}`);
        }
    } catch (error) {
        console.log(`❌ Error saving data: ${error.message}`);
    }
}

async function checkPause() {
    if (shouldPause) {
        console.log('⏸ Scraping paused. Send a POST request to /resume to resume or /stop to stop');
        while (shouldPause) {
            await new Promise(resolve => setTimeout(resolve, 1000));
            if (shouldStop) {
                console.log('🛑 Stopping as requested during pause');
                return true;
            }
        }
        console.log('▶ Resuming scraping...');
    }
    return false;
}

async function processCID(cid) {
    let retries = 0;
    
    while (retries < CONFIG.MAX_RETRIES) {
        try {
            if (!(await checkInternetConnection())) {
                await waitForInternet();
            }

            await driver.get(CONFIG.URL);
            await new Promise(resolve => setTimeout(resolve, 2000));

            // Enter CID
            await driver.wait(until.elementLocated(By.id('ltscno')), 10000);
            await driver.findElement(By.id('ltscno')).sendKeys(cid);

            // Solve CAPTCHA
            await driver.wait(until.elementLocated(By.id('Billquestion')), 10000);
            const captchaText = await driver.executeScript("return document.getElementById('Billquestion').innerText;");
            await driver.findElement(By.id('Billans')).sendKeys(captchaText.trim());
            await driver.findElement(By.id('Billsignin')).click();
            await new Promise(resolve => setTimeout(resolve, 2000));

            // Check for CAPTCHA error alert
            try {
                const alert = await driver.switchTo().alert();
                const alertText = await alert.getText();
                await alert.accept();
                throw new Error(`CAPTCHA validation failed: ${alertText}`);
            } catch (error) {
                // No alert present, continue
            }

            // Click History
            try {
                await driver.wait(until.elementLocated(By.id('historyDivbtn')), 10000);
                await driver.executeScript("window.scrollBy(0, 280)");
                await new Promise(resolve => setTimeout(resolve, 2000));
                await driver.findElement(By.id('historyDivbtn')).click();
            } catch (error) {
                throw new Error('CAPTCHA failed or no history button');
            }

            // Scrape data
            await driver.wait(until.elementLocated(By.id('consumptionData')), 10000);
            const rows = await driver.findElement(By.id('consumptionData')).findElements(By.tagName('tr'));
            const dataRows = rows.slice(1); // Skip header row
            
            if (dataRows.length === 0) {
                throw new Error('No data rows found');
            }

            // Store data
            const cidData = {};
            for (const row of dataRows) {
                const cells = await row.findElements(By.tagName('td'));
                if (cells.length < 4) continue;
                
                const billMonth = await cells[1].getText();
                let amountText;
                
                try {
                    const input = await cells[3].findElement(By.tagName('input'));
                    amountText = await input.getAttribute('value');
                } catch (error) {
                    amountText = await cells[3].getText();
                }
                
                try {
                    const cleanAmount = amountText.trim().replace(/,/g, '');
                    const amount = /^\d+\.?\d*$/.test(cleanAmount) ? parseFloat(cleanAmount) : 0;
                    cidData[billMonth.trim()] = amount;
                } catch (error) {
                    cidData[billMonth.trim()] = 0;
                }
            }

            return cidData;
        } catch (error) {
            retries++;
            console.log(`⚠ Attempt ${retries}/${CONFIG.MAX_RETRIES} failed for CID ${cid}: ${error.message.slice(0, 100)}`);
            if (retries < CONFIG.MAX_RETRIES) {
                await new Promise(resolve => setTimeout(resolve, CONFIG.RETRY_DELAY));
            } else {
                throw error;
            }
        }
    }
}

async function scrapingWorker() {
    try {
        // Setup browser
        const options = new chrome.Options();
        driver = await new Builder()
            .forBrowser('chrome')
            .setChromeOptions(options)
            .build();

        // Load data
        const workbook = xlsx.readFile(CONFIG.INPUT_FILE);
        const sheetName = workbook.SheetNames[0];
        const worksheet = workbook.Sheets[sheetName];
        const cidList = xlsx.utils.sheet_to_json(worksheet, { header: 1 }).map(row => row[0].toString());
        
        const { existingData, existingFailed } = loadExistingData();
        const status = loadStatus();
        
        // Initialize data structures
        const outputData = Array.isArray(existingData) ? [...existingData] : [];
        const notScraped = new Set(existingFailed);
        
        // Track counts
        const total = cidList.length;
        let successCount = status.total_processed || 0;
        let failedCount = notScraped.size;
        let startIndex = status.last_processed || 0;
        
        console.log(`Starting scraping from index ${startIndex} of ${total} CIDs`);
        console.log(`Previously processed: ${successCount} success, ${failedCount} failed`);

        for (let index = startIndex; index < total; index++) {
            if (shouldStop) {
                console.log('🛑 Stopping as requested');
                break;
            }
            
            if (await checkPause()) {
                shouldStop = true;
                break;
            }
            
            const cid = cidList[index];
            
            // Skip already processed CIDs
            const alreadyProcessed = outputData.some(item => item.CID === cid) || notScraped.has(cid);
            if (alreadyProcessed) continue;
                
            console.log(`🔍 Processing CID ${cid} (${index + 1}/${total})`);
            
            try {
                const cidData = await processCID(cid);
                outputData.push({ CID: cid, ...cidData });
                successCount++;
                console.log(`✅ Successfully scraped CID ${cid}`);
                
            } catch (error) {
                console.log(`❌ Failed to scrape CID ${cid}: ${error.message.slice(0, 100)}...`);
                notScraped.add(cid);
                failedCount++;
            }

            // Save progress
            saveStatus(index + 1, successCount);
            
            // Periodic save every 10 CIDs
            if ((index + 1) % 10 === 0) {
                saveData(outputData, notScraped);
                console.log(`↻ Saved progress: ${successCount} success, ${failedCount} failed`);
            }
        }

        // Final save
        saveData(outputData, notScraped);
        
        console.log('\n🎉 Scraping completed. Results:');
        console.log(`Total CIDs: ${total}`);
        console.log(`Successfully scraped: ${successCount}`);
        console.log(`Failed to scrape: ${failedCount}`);
        console.log(`Success rate: ${(successCount / total * 100).toFixed(2)}%`);
        
    } catch (error) {
        console.log(`❌ Scraping failed with error: ${error.message}`);
    } finally {
        if (driver) {
            await driver.quit();
            console.log('🚪 Browser closed');
        }
    }
}

// ---------------------- API ENDPOINTS ----------------------
app.use(express.json());

app.post('/start', (req, res) => {
    if (scraperThread) {
        return res.status(400).json({ message: 'Scraping is already running' });
    }
    
    shouldPause = false;
    shouldStop = false;
    
    scraperThread = (async () => {
        await scrapingWorker();
        scraperThread = null;
    })();
    
    res.json({ message: '🚀 Scraping started' });
});

app.post('/pause', (req, res) => {
    if (!scraperThread) {
        return res.status(400).json({ message: 'No active scraping to pause' });
    }
    
    shouldPause = true;
    res.json({ message: '⏸ Pause requested. Will pause after current CID completes.' });
});

app.post('/resume', (req, res) => {
    if (!shouldPause) {
        return res.status(400).json({ message: 'Scraping is not paused' });
    }
    
    shouldPause = false;
    res.json({ message: '▶ Resuming scraping...' });
});

app.post('/stop', (req, res) => {
    if (!scraperThread) {
        return res.status(400).json({ message: 'No active scraping to stop' });
    }
    
    shouldStop = true;
    res.json({ message: '🛑 Stop requested. Will stop after current CID completes.' });
});

app.get('/status', (req, res) => {
    if (!scraperThread) {
        return res.status(400).json({ status: 'No scraping session exists' });
    }
    
    let status;
    if (shouldPause) {
        status = '⏸ Scraping is currently paused';
    } else if (shouldStop) {
        status = '🛑 Scraping is stopping...';
    } else {
        status = '▶ Scraping is running';
    }
    
    res.json({ status });
});

// Graceful shutdown
process.on('SIGINT', () => {
    console.log('\n🛑 Received interrupt signal. Stopping gracefully...');
    shouldStop = true;
    if (driver) {
        driver.quit().then(() => process.exit(0));
    } else {
        process.exit(0);
    }
});

// Start server
app.listen(CONFIG.PORT, () => {
    console.log(`Server running on port ${CONFIG.PORT}`);
    console.log('Scraping Control Options:');
    console.log('POST /start - Start scraping');
    console.log('POST /pause - Pause scraping');
    console.log('POST /resume - Resume scraping');
    console.log('POST /stop - Stop scraping');
    console.log('GET /status - Check status');
});
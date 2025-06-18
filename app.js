const express = require('express');
const app = express();
const { Builder, By, until } = require('selenium-webdriver');
const chrome = require('selenium-webdriver/chrome');
const fs = require('fs');
const path = require('path');
const xlsx = require('xlsx');
const axios = require('axios');
const { Worker, isMainThread, parentPort, workerData } = require('worker_threads');
const os = require('os');

const CONFIG = {
    INPUT_FILE: path.join(__dirname, 'data/input/VSKP1_data.xlsx'),
    OUTPUT_FILE: path.join(__dirname, 'data/output/VSKP2_data.xlsx'),
    FAILED_FILE: path.join(__dirname, 'data/failed/VSKP1_failed.json'),
    STATUS_FILE: path.join(__dirname, 'data/status.json'),
    URL: 'https://www.apeasternpower.com/viewBillDetailsMain',
    CHECK_INTERNET_URL: 'http://www.google.com',
    MAX_RETRIES: 3,
    RETRY_DELAY: 10000,
    BATCH_SIZE: 10,
    PORT: process.env.PORT || 3000,
    MAX_WORKERS: Math.max(1, Math.min(4, os.cpus().length - 1)) // Use 1-4 workers based on CPU cores
};

// ---------------------- GLOBAL STATE ----------------------
let shouldPause = false;
let shouldStop = false;
let activeWorkers = 0;
let processingActive = false;
let workerThreads = [];
let failedCount = 0;

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
    while (!(await checkInternetConnection()) && !shouldStop) {
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
    return { last_processed: 0, total_processed: 0, total_failed: 0 };
}

function saveStatus(status) {
    try {
        fs.writeFileSync(CONFIG.STATUS_FILE, JSON.stringify(status));
    } catch (error) {
        console.log(`⚠ Couldn't save status file: ${error.message}`);
    }
}

function loadExistingData() {
    let existingData = {};
    let existingFailed = new Set();

    // Load successful data
    if (fs.existsSync(CONFIG.OUTPUT_FILE)) {
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
    if (fs.existsSync(CONFIG.FAILED_FILE)) {
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
            fs.writeFileSync(CONFIG.FAILED_FILE, JSON.stringify([...notScraped], null, 4));
            console.log(`⚠ Failed CIDs saved to ${CONFIG.FAILED_FILE}`);
        }
    } catch (error) {
        console.log(`❌ Error saving data: ${error.message}`);
    }
}

async function checkPause() {
    if (shouldPause) {
        console.log('⏸ Scraping paused. Send a POST request to /resume to resume or /stop to stop');
        while (shouldPause && !shouldStop) {
            await new Promise(resolve => setTimeout(resolve, 1000));
        }
        if (shouldStop) {
            console.log('🛑 Stopping as requested during pause');
            return true;
        }
        console.log('▶ Resuming scraping...');
    }
    return false;
}

async function processCID(driver, cid) {
    let retries = 0;
    
    while (retries < CONFIG.MAX_RETRIES && !shouldStop) {
        try {
            if (!(await checkInternetConnection())) {
                await waitForInternet();
                if (shouldStop) return null;
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
            if (retries < CONFIG.MAX_RETRIES && !shouldStop) {
                await new Promise(resolve => setTimeout(resolve, CONFIG.RETRY_DELAY));
            } else {
                throw error;
            }
        }
    }
    return null;
}

async function workerThread(workerId) {
    let driver = null;
    try {
        // Setup browser with optimized configuration
        const options = new chrome.Options();
        options.addArguments(
            '--headless=new',
            '--no-sandbox',
            '--disable-dev-shm-usage',
            '--disable-gpu',
            '--window-size=1280,720'
        );
        
        // Use unique temp directory for user data
        const tempDir = fs.mkdtempSync('/tmp/chrome-');
        options.addArguments(`--user-data-dir=${tempDir}`);
        
        driver = await new Builder()
            .forBrowser('chrome')
            .setChromeOptions(options)
            .build();

        console.log(`👷 Worker ${workerId} started`);

        // Load input data
        const workbook = xlsx.readFile(CONFIG.INPUT_FILE);
        const sheetName = workbook.SheetNames[0];
        const worksheet = workbook.Sheets[sheetName];
        const cidList = xlsx.utils.sheet_to_json(worksheet, { header: 1 }).map(row => row[0].toString());
        
        const { existingData, existingFailed } = loadExistingData();
        const status = loadStatus();
        
        // Initialize data structures
        const outputData = Array.isArray(existingData) ? [...existingData] : [];
        const notScraped = new Set(existingFailed);
        
        const total = cidList.length;
        let processedCount = 0;
        let failedCount = status.total_failed || 0;
        
        while (!shouldStop) {
            if (await checkPause()) {
                shouldStop = true;
                break;
            }
            
            // Get next batch of CIDs to process
            const batchStart = status.last_processed;
            const batchEnd = Math.min(batchStart + CONFIG.BATCH_SIZE, total);
            
            if (batchStart >= total) {
                console.log(`ℹ Worker ${workerId}: No more CIDs to process`);
                break;
            }
            
            const batch = cidList.slice(batchStart, batchEnd);
            console.log(`👷 Worker ${workerId} processing batch of ${batch.length} CIDs (${batchStart+1}-${batchEnd} of ${total})`);
            
            const processedIds = [];
            const failedIds = [];
            
            for (const cid of batch) {
                if (shouldStop) break;
                
                // Skip already processed CIDs
                if (outputData.some(item => item.CID === cid) || notScraped.has(cid)) {
                    continue;
                }
                
                console.log(`🔍 Worker ${workerId} processing CID ${cid}`);
                
                try {
                    const cidData = await processCID(driver, cid);
                    if (cidData) {
                        outputData.push({ CID: cid, ...cidData });
                        processedIds.push(cid);
                        processedCount++;
                        console.log(`✅ Worker ${workerId} processed CID ${cid}`);
                    } else {
                        throw new Error('No data returned from scraping');
                    }
                } catch (error) {
                    console.log(`❌ Worker ${workerId} failed to process CID ${cid}: ${error.message.slice(0, 100)}...`);
                    notScraped.add(cid);
                    failedIds.push(cid);
                    failedCount++;
                }
            }
            
            // Update status
            status.last_processed = batchEnd;
            status.total_processed = (status.total_processed || 0) + processedIds.length;
            status.total_failed = failedCount;
            saveStatus(status);
            
            // Save data periodically
            if (processedIds.length > 0 || failedIds.length > 0) {
                saveData(outputData, notScraped);
                console.log(`📊 Worker ${workerId} batch results: ${processedIds.length} success, ${failedIds.length} failed`);
            }
            
            // Small delay between batches
            await new Promise(resolve => setTimeout(resolve, 1000));
        }
        
        console.log(`🏁 Worker ${workerId} finished processing`);
        
    } catch (error) {
        console.log(`❌ Worker ${workerId} crashed: ${error.message}`);
    } finally {
        activeWorkers--;
        if (driver) {
            await driver.quit();
            console.log(`🚪 Worker ${workerId} browser closed`);
        }
    }
}

async function startScraping() {
    if (processingActive) {
        throw new Error('Scraping is already running');
    }
    
    shouldPause = false;
    shouldStop = false;
    processingActive = true;
    failedCount = 0;
    
    // Initialize status if not exists
    if (!fs.existsSync(CONFIG.STATUS_FILE)) {
        saveStatus({ last_processed: 0, total_processed: 0, total_failed: 0 });
    }
    
    // Determine number of workers to use
    const numWorkers = CONFIG.MAX_WORKERS;
    activeWorkers = numWorkers;
    
    console.log(`🚀 Starting scraping with ${numWorkers} workers`);
    
    // Start worker threads
    for (let i = 1; i <= numWorkers; i++) {
        const worker = new Worker(__filename, { 
            workerData: { 
                workerId: i,
                config: CONFIG 
            } 
        });
        
        worker.on('message', (message) => {
            console.log(`Worker ${i}: ${message}`);
        });
        
        worker.on('error', (error) => {
            console.error(`Worker ${i} error:`, error);
        });
        
        worker.on('exit', (code) => {
            if (code !== 0) {
                console.error(`Worker ${i} stopped with exit code ${code}`);
            }
            activeWorkers--;
            if (activeWorkers === 0) {
                processingActive = false;
                console.log('🎉 All workers finished');
            }
        });
        
        workerThreads.push(worker);
    }
}

async function stopScraping() {
    shouldStop = true;
    processingActive = false;
    
    // Wait for all workers to finish
    while (activeWorkers > 0) {
        await new Promise(resolve => setTimeout(resolve, 1000));
    }
    
    workerThreads = [];
}

// ---------------------- API ENDPOINTS ----------------------
app.use(express.json());

app.post('/start', async (req, res) => {
    try {
        await startScraping();
        res.json({ 
            message: `🚀 Scraping started with ${CONFIG.MAX_WORKERS} workers`,
            workers: CONFIG.MAX_WORKERS
        });
    } catch (error) {
        res.status(400).json({ error: error.message });
    }
});

app.post('/pause', (req, res) => {
    if (!processingActive) {
        return res.status(400).json({ message: 'No active scraping to pause' });
    }
    
    shouldPause = true;
    res.json({ message: '⏸ Pause requested' });
});

app.post('/resume', (req, res) => {
    if (!shouldPause) {
        return res.status(400).json({ message: 'Scraping is not paused' });
    }
    
    shouldPause = false;
    res.json({ message: '▶ Resuming scraping...' });
});

app.post('/stop', async (req, res) => {
    if (!processingActive) {
        return res.status(400).json({ message: 'No active scraping to stop' });
    }
    
    await stopScraping();
    res.json({ message: '🛑 Scraping stopped' });
});

app.get('/status', (req, res) => {
    const status = loadStatus();
    
    res.json({
        processing_active: processingActive,
        active_workers: activeWorkers,
        paused: shouldPause,
        stopped: shouldStop,
        last_processed: status.last_processed,
        total_processed: status.total_processed,
        total_failed: status.total_failed
    });
});

// Graceful shutdown
process.on('SIGINT', async () => {
    console.log('\n🛑 Received interrupt signal. Stopping gracefully...');
    await stopScraping();
    process.exit(0);
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

// Worker thread entry point
if (!isMainThread) {
    (async () => {
        try {
            await workerThread(workerData.workerId);
            parentPort.postMessage('Worker finished');
        } catch (error) {
            parentPort.postMessage(`Worker error: ${error.message}`);
        }
    })();
}

import taos from "@tdengine/websocket";


/* -------------------------------------------------------------
    * TDengine connection information
    * Change the connection type to 'cloud' or 'docker' as needed.
 ----------------------------------------------------------------
 */
const TAOS_CONNECTION_TYPE = 'cloud'; // 'cloud' or 'local' (docker)


let TAOS_CONNECTION_URL, TAOS_USER, TAOS_PASSWORD;

const TAOS_DATABASE = 'webinar';
const TAOS_TABLENAME = 'market';

if (TAOS_CONNECTION_TYPE === 'cloud') {
    // TDengine Cloud connection
    TAOS_CONNECTION_URL = process.env.TDENGINE_CLOUD_URL || `wss://YOUR_CLOUD_REGION.cloud.tdengine.com?token=YOUR_TDENGINE_CLOUD_TOKEN`;
} else if (TAOS_CONNECTION_TYPE === 'local') {
    // TDengine local docker connection
    TAOS_CONNECTION_URL = 'ws://localhost:6041';
    TAOS_USER = 'root';
    TAOS_PASSWORD = 'taosdata';
}

/* -------------------------------------------------------------
    * Data generation information
 ----------------------------------------------------------------
 */

// Data generation constants
const MAX_RUN_DURATION = 60 * 60 * 1000;    // 1 hour in milliseconds
const INTERVAL = 250;                       // interval in milliseconds
const NUM_ROWS_PER_INTERVAL = 100;          // rows generated per interval


const COMPANY_METADATA = [
    { ticker: "AAPL.N", sector: "Information Technology", state: "CA", index_fund: ["S&P 500","NASDAQ 100","DJIA","Russell 1000","S&P 400 MidCap","Wilshire 5000"], price_range: [169.21, 260.10], avg_volume: 59450000 },
    { ticker: "AMZN.N", sector: "Consumer Discretionary", state: "WA", index_fund: ["S&P 500","NASDAQ 100","Russell 1000","S&P 400 MidCap","Wilshire 5000"], price_range: [151.61, 242.52], avg_volume: 50520000 },
    { ticker: "NVDA.N", sector: "Information Technology", state: "CA", index_fund: ["S&P 500","NASDAQ 100","Russell 1000","S&P 400 MidCap","Wilshire 5000"], price_range: [86.62, 153.13], avg_volume: 296000000 },
    { ticker: "TSLA.N", sector: "Consumer Discretionary", state: "TX", index_fund: ["S&P 500","NASDAQ 100","Russell 1000","S&P 400 MidCap","Wilshire 5000"], price_range: [167.41, 488.54], avg_volume: 123000000 },
    { ticker: "MSFT.N", sector: "Information Technology", state: "WA", index_fund: ["S&P 500","NASDAQ 100","DJIA","Russell 1000","S&P 400 MidCap","Wilshire 5000"], price_range: [344.79, 468.35], avg_volume: 23000000 },
    { ticker: "GOOGL.N", sector: "Communication Services", state: "CA", index_fund: ["S&P 500","NASDAQ 100","Russell 1000","S&P 400 MidCap","Wilshire 5000"], price_range: [142.66, 208.70], avg_volume: 38320000 },
    { ticker: "JPM.N", sector: "Financials", state: "NY", index_fund: ["S&P 500","DJIA","Russell 1000","S&P 400 MidCap","Wilshire 5000"], price_range: [190.88, 280.25], avg_volume: 11380000 },
    { ticker: "V.N", sector: "Financials", state: "CA", index_fund: ["S&P 500","DJIA","Russell 1000","S&P 400 MidCap","Wilshire 5000"], price_range: [252.70, 366.54], avg_volume: 7600000 },
    { ticker: "DIS.N", sector: "Communication Services", state: "CA", index_fund: ["S&P 500","DJIA","Russell 1000","S&P 400 MidCap","Wilshire 5000"], price_range: [80.10, 118.63], avg_volume: 11000000 },
    { ticker: "WMT.N", sector: "Consumer Staples", state: "AR", index_fund: ["S&P 500","DJIA","Russell 1000","S&P 400 MidCap","Wilshire 5000"], price_range: [59.44, 105.30], avg_volume: 25300000 },
    { ticker: "PFE.N", sector: "Health Care", state: "NY", index_fund: ["S&P 500","DJIA","Russell 1000","S&P 400 MidCap","Wilshire 5000"], price_range: [20.92, 31.54], avg_volume: 55100000 },
    { ticker: "ORCL.N", sector: "Information Technology", state: "TX", index_fund: ["S&P 500","Russell 1000","S&P 400 MidCap","Wilshire 5000"], price_range: [114.55, 198.31], avg_volume: 11200000 },
    { ticker: "NFLX.N", sector: "Communication Services", state: "CA", index_fund: ["S&P 500","NASDAQ 100","Russell 1000","S&P 400 MidCap","Wilshire 5000"], price_range: [587.04, 1164.00], avg_volume: 4100000 },
    { ticker: "INTC.N", sector: "Information Technology", state: "CA", index_fund: ["S&P 500","NASDAQ 100","Russell 1000","S&P 400 MidCap","Wilshire 5000"], price_range: [17.67, 37.16], avg_volume: 104400000 },
    { ticker: "ADBE.N", sector: "Information Technology", state: "CA", index_fund: ["S&P 500","NASDAQ 100","Russell 1000","S&P 400 MidCap","Wilshire 5000"], price_range: [332.01, 587.75], avg_volume: 4100000 },
];

const CLIENTS = [
    "BlackRock", "Vanguard", "State Street",
    "Fidelity", "Goldman Sachs", "Morgan Stanley",
    "Citadel Securities", "Bridgewater", "Berkshire Hathaway",
];


/**
 * Returns the current date and time in ISO 8601 format with a random jitter of ±5 seconds.
 */
function _nowISO() {
    const offset = (Math.random() * 10) - 5; // jitter +/-5 sec
    return new Date(Date.now() + offset * 1000).toISOString();
}


/**
 * Generates a random number following a normal (Gaussian) distribution using the Box-Muller transform.
 */
function _randomDistribution(mean, stddev) {
    let u = 0, v = 0;
    while(u === 0) u = Math.random();
    while(v === 0) v = Math.random();
    let num = Math.sqrt(-2.0 * Math.log(u)) * Math.cos(2.0 * Math.PI * v);
    return num * stddev + mean;
}


/**
 * Generates an array of random market data objects for a given number of rows.
 *
 * @param {number} [num_rows=NUM_ROWS_PER_INTERVAL] - The number of data rows to generate.
 * @returns {Object[]} Array of generated market data objects.
 */
function generateData(num_rows = NUM_ROWS_PER_INTERVAL) {
    const results = [];
    for (let i = 0; i < num_rows; i++) {
        // --- random company
        const metaIdx = Math.floor(Math.random() * COMPANY_METADATA.length);
        const meta = COMPANY_METADATA[metaIdx];

        // --- price bars
        const [lowRange, highRange] = meta.price_range;
        const open = +(Math.random() * (highRange - lowRange) + lowRange).toFixed(2);
        const high = +(open * (1 + Math.random() * 0.03)).toFixed(2);
        const low = +(open * (0.97 + Math.random() * 0.03)).toFixed(2);
        const close = +(Math.random() * (high - low) + low).toFixed(2);

        // --- market metrics
        const avgVol = meta.avg_volume;
        const volume = Math.max(1, Math.round(_randomDistribution(avgVol, avgVol * 0.15)));
        const lotSize = Math.floor(Math.random() * (500 - 50)) + 50;
        const trade_count = Math.floor(volume / lotSize);
        const notional = +(close * volume).toFixed(2);

        // --- dimensions & timestamps
        const index_fund = meta.index_fund[Math.floor(Math.random() * meta.index_fund.length)];
        const client = CLIENTS[Math.floor(Math.random() * CLIENTS.length)];
        const country = "United States";
        const trade_date = (new Date()).toISOString().slice(0, 10);
        const last_update = _nowISO();

        results.push({
            ticker: meta.ticker,
            sector: meta.sector,
            state: meta.state,
            index_fund,
            open,
            high,
            low,
            close,
            volume,
            trade_count,
            notional,
            client,
            country,
            trade_date,
            last_update
        });
    }
    return results;
}


/* -------------------------------------------------------------
    * TDengine connection and data insertion functions
 ----------------------------------------------------------------
 */


/**
 * Connect to a TDengine database using WebSocket APIs.
 */
async function taosCreateConnection(
    url = TAOS_CONNECTION_URL, 
    user = (TAOS_USER || null), 
    password = (TAOS_PASSWORD || null)
) {
    try {
        let conf, conn;
        if (TAOS_CONNECTION_TYPE === 'cloud') {
            conf = new taos.WSConfig(url);
            conf.setTimeOut(15_000);
            conn = await taos.sqlConnect(conf);
            console.log(`Connected to TDengine Cloud at ${url} successfully.`);
            return conn;
        } else if (TAOS_CONNECTION_TYPE === 'local') {
            conf = new taos.WSConfig(url);
            conf.setUser(user);
            conf.setPwd(password);
            conn = await taos.sqlConnect(conf);
            console.log(`Connected to local TDengine at ${url} successfully.`);
            return conn;
        } else {
            throw new Error("Invalid TAOS_CONNECTION_TYPE. Please set it to 'cloud' or 'docker'.");
        }
    } catch (err) {
        console.log(`Failed to connect to ${url}, ErrCode: ${err.code}, ErrMessage: ${err.message}`);
        if (TAOS_CONNECTION_TYPE === 'local') {
            console.log("Please run `/docker.sh` to start the TDengine docker container.");
        }
        process.exit(1);
    }
}


/**
 * Create a TDengine database and table.
 */
async function taosCreateDatabase(conn, databaseName = TAOS_DATABASE) {
    try {
        // create the database with a 1-year retention policy
        await conn.exec(`CREATE DATABASE IF NOT EXISTS ${databaseName} KEEP 365 DURATION 10 BUFFER 16 WAL_LEVEL 1;`);
        // switch to the database
        await conn.exec(`USE ${databaseName};`);
        console.log(`Database ${databaseName} created successfully.`);
    } catch (err) {
        console.error(`Failed to create database ${databaseName}, ErrCode: ${err.code}, ErrMessage: ${err.message}`);
        throw err;
    }
}


/**
 * Create a TDengine table for market data matching the generateData() schema.
 * Uses VARCHAR for all string columns.
 */
async function taosCreateTable(conn, databaseName = TAOS_DATABASE, tableName = TAOS_TABLENAME) {
    try {
        // Drop the table if it already exists
        await conn.exec(`USE ${databaseName};`);
        await conn.exec(`DROP TABLE IF EXISTS ${databaseName}.${tableName};`);

        // Create the table with columns matching generateData()
        const sql = [
            "CREATE TABLE IF NOT EXISTS `" + databaseName + "`.`" + tableName + "` (",
            "    `ts` TIMESTAMP,",
            "    `ticker` VARCHAR(16),",
            "    `sector` VARCHAR(32),",
            "    `state` VARCHAR(8),",
            "    `index_fund` VARCHAR(32),",
            "    `open` FLOAT,",
            "    `high` FLOAT,",
            "    `low` FLOAT,",
            "    `close` FLOAT,",
            "    `volume` BIGINT,",
            "    `trade_count` INT,",
            "    `notional` DOUBLE,",
            "    `client` VARCHAR(32),",
            "    `country` VARCHAR(32),",
            "    `trade_date` TIMESTAMP,",
            "    `last_update` TIMESTAMP",
            ");"
        ].join('\n');
        await conn.exec(sql);
        console.log(`TDengine - Created table ${databaseName}.${tableName} for market data`);
    } catch (err) {
        console.error(`Failed to create table ${tableName}, ErrCode: ${err.code}, ErrMessage: ${err.message}`);
        throw err;
    }
}


/**
 * Insert data into a TDengine table using the WebSocket API and batch API for performance.
 * Inserts market data rows as generated by generateData().
 */
async function toasInsertData(conn, data, databaseName = TAOS_DATABASE, tableName = TAOS_TABLENAME) {
    try {
        const sql = `
            INSERT INTO ${databaseName}.${tableName} 
            (ts, ticker, sector, state, index_fund, open, high, low, close, volume, trade_count, notional, client, country, trade_date, last_update)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        `;

        let stmt = await conn.stmtInit();
        await stmt.prepare(sql);

        let bindParams = stmt.newStmtParam();
        bindParams.setTimestamp(data.map(row => new Date(row.last_update).getTime()));
        bindParams.setVarchar(data.map(row => row.ticker));
        bindParams.setVarchar(data.map(row => row.sector));
        bindParams.setVarchar(data.map(row => row.state));
        bindParams.setVarchar(data.map(row => row.index_fund));
        bindParams.setFloat(data.map(row => row.open));
        bindParams.setFloat(data.map(row => row.high));
        bindParams.setFloat(data.map(row => row.low));
        bindParams.setFloat(data.map(row => row.close));
        bindParams.setInt(data.map(row => row.volume));
        bindParams.setInt(data.map(row => row.trade_count));
        bindParams.setDouble(data.map(row => row.notional));
        bindParams.setVarchar(data.map(row => row.client));
        bindParams.setVarchar(data.map(row => row.country));
        bindParams.setTimestamp(data.map(row => row.trade_date));
        bindParams.setTimestamp(data.map(row => new Date(row.last_update).getTime()));

        await stmt.bind(bindParams);
        await stmt.batch();
        await stmt.exec();
        console.log(`Inserted ${data.length} rows into table: ${databaseName}.${tableName}`);
    } catch (err) {
        console.error(`Failed to insert rows into ${tableName}, ErrCode: ${err.code}, ErrMessage: ${err.message}`);
        throw err;
    }
}


/**
 * Main function to generate and insert random power line data into a TDengine database at regular intervals.
 * 
 * We establish a connection to the TDengine database, creates the necessary database and table,
 * and then starts a loop to generate and insert data at a specified interval. The loop runs until a maximum
 * duration is reached, at which point the connection is closed, and the process exits.
 * 
 * Workflow:
 * 1. Create a TDengine connection >> `taosCreateConnection()`.
 * 2. Create the demo database and table >> `taosCreateDatabase()`, `taosCreateTable()`.
 * 3. Start a data generation and insertion timer loop >> `generateData()`, `toasInsertData()`.
 * 
 */
async function main() {

    try {
        // create a connection to the TDengine database
        let conn = await taosCreateConnection();
        // create the database and table
        TAOS_CONNECTION_TYPE == 'local' && await taosCreateDatabase(conn);
        await taosCreateTable(conn);
        await conn.exec(`USE ${TAOS_DATABASE};`);

        // start the data generation and insertion loop
        const startTime = Date.now();
        console.log("Starting data generation and insertion loop...");

        // insert data at a regular interval -- 250ms default
        const intervalId = setInterval(async () => {
            try {
                // exit if the max run duration is reached -- 1 hour default
                if ((Date.now() - startTime) >= MAX_RUN_DURATION) {
                    console.log("Max run duration reached. Exiting...");
                    clearInterval(intervalId);
                    await conn.close();
                    await taos.destroy();
                    process.exit(0);
                }
                // generate and insert data
                let data = generateData();
                await toasInsertData(conn, data);
            } catch (err) {
                console.error("Error during data insertion:", err);
            }
        }, INTERVAL);
    } catch (err) {
        console.log("Demo Failed! ErrCode: " + err.code + ", ErrMessage: " + err.message);
    }
}

// Run the main function
try {
    await main();
    await taos.destroy();
    
    // Handle graceful shutdown on SIGINT (Ctrl+C)
    process.on('SIGINT', async () => {
        console.log("Received SIGINT. Closing connection...");
        await taos.destroy();
        process.exit(0);
    });
} catch (err) {
    console.error("Fatal error:", err);
}

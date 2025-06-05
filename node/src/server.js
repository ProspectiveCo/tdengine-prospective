import perspective from "@finos/perspective";
import * as taos from "@tdengine/websocket";

/* -------------------------------------------------------------
    * TDengine connection information
    * Change the connection type to 'cloud' or 'local' as needed.
 ----------------------------------------------------------------
 */
const TAOS_CONNECTION_TYPE = 'local'; // 'cloud' or 'local' (docker)


let TAOS_CONNECTION_URL, TAOS_USER, TAOS_PASSWORD;

const TAOS_DATABASE = 'webinar';
const TAOS_TABLENAME = 'market';

if (TAOS_CONNECTION_TYPE === 'cloud') {
    // TDengine Cloud connection
    TAOS_CONNECTION_URL = process.env.TAOS_CONNECTION_URL || `wss://YOUR_CLOUD_REGION.cloud.tdengine.com?token=YOUR_TDENGINE_CLOUD_TOKEN`;
} else if (TAOS_CONNECTION_TYPE === 'local') {
    // TDengine local docker connection
    TAOS_CONNECTION_URL = 'ws://localhost:6041';
    TAOS_USER = 'root';
    TAOS_PASSWORD = 'taosdata';
}

/* -------------------------------------------------------------
    * Perspective table information
 ----------------------------------------------------------------
 */
const PRSP_TABLE_NAME = TAOS_TABLENAME;
const PRSP_TABLE_LIMIT = 100_000;               // Maximum number of rows in the Perspective table
const PRSP_TABLE_REFRESH_INTERVAL = 250;        // Refresh interval in milliseconds


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
            throw new Error("Invalid TAOS_CONNECTION_TYPE. Please set it to 'cloud' or 'local'.");
        }
    } catch (err) {
        console.error(`Failed to connect to ${url}, ErrCode: ${err.code}, ErrMessage: ${err.message}`);
        if (TAOS_CONNECTION_TYPE === 'local') {
            console.log("Please run `/docker.sh` to start the TDengine docker container.");
        }
        process.exit(1);
    }
}

/**
 * Query the TDengine market table and return the result as an array of objects.
 */
async function taosQuery(conn, databaseName = TAOS_DATABASE, tableName = TAOS_TABLENAME) {
    try {
        const sql = `
            SELECT 
                ts, ticker, sector, state, index_fund, open, high, low, close, volume, trade_count, notional, client, country, trade_date, last_update
            FROM ${databaseName}.${tableName}
            ORDER BY ts DESC
            LIMIT ${PRSP_TABLE_LIMIT};
        `;
        const wsRows = await conn.query(sql);
        const data = [];
        while (await wsRows.next()) {
            let row = wsRows.getData();
            data.push({
                ts: new Date(Number(row[0])),
                ticker: row[1],
                sector: row[2],
                state: row[3],
                index_fund: row[4],
                open: row[5],
                high: row[6],
                low: row[7],
                close: row[8],
                volume: row[9],
                trade_count: row[10],
                notional: row[11],
                client: row[12],
                country: row[13],
                trade_date: new Date(Number(row[14])),
                last_update: new Date(Number(row[15])),
            });
        }
        return data;
    } catch (err) {
        console.error(`Failed to query table ${databaseName}.${tableName}, ErrCode: ${err.code}, ErrMessage: ${err.message}`);
        throw err;
    }
}

/**
 * Create a Perspective table to host on the websocket.
 */
async function prspCreatePerspectiveTable() {
    // Schema matches generateData() in producer.js
    const schema = {
        ts: "datetime",
        ticker: "string",
        sector: "string",
        state: "string",
        index_fund: "string",
        open: "float",
        high: "float",
        low: "float",
        close: "float",
        volume: "integer",
        trade_count: "integer",
        notional: "float",
        client: "string",
        country: "string",
        trade_date: "datetime",
        last_update: "datetime",
    };
    const table = await perspective.table(schema, { name: PRSP_TABLE_NAME, limit: PRSP_TABLE_LIMIT, format: "json" });
    return table;
}


/**
 * Main function to orchestrate the workflow.
 */
async function main() {
    // create tdengine connection and perspective websocket server
    const conn = await taosCreateConnection();
    const ws = new perspective.WebSocketServer({ port: 8080 });         // perspective websocket is always hosted on: ws://localhost:8080/websocket

    // create a perspective table
    const table = await prspCreatePerspectiveTable();

    console.log(`Perspective WebSocket server is running on ws://localhost:8080`);

    // Set up a timer to periodically query TDengine and update the Perspective table
    setInterval(async () => {
        try {
            const data = await taosQuery(conn);
            await table.update(data);
            // console.log(`Perspective table refreshed: ${data.length} rows.`);
        } catch (err) {
            console.error(`Error updating Perspective table: ${err.message}`);
            console.error(`Exiting...`);
            await table.clear();
            await conn.close();
            await taos.destroy();
            process.exit(1);
        }
    }, PRSP_TABLE_REFRESH_INTERVAL);
}


// Run the main function
main();

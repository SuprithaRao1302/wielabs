/*
Here's the pseudocode i followed to complete this challenge
Download the .tar.gz file using Node.js's Streams API and save it to the tmp/ folder.
Decompress the GZIP part of the file and extract the TAR archive, saving the resulting folder in tmp/.
Set up a SQLite database at out/database.sqlite using knex.
Read the two CSVs in the extracted folder using a streaming API.
Add each row to the SQL database. For efficiency, add about 100 rows at a time using a batch insert.
Ensure that the SQL tables have a unique, auto-incrementing primary key column called id and that all columns are non-nullable.
*/
import * as fs from 'fs';
import * as path from 'path';
import * as https from 'https';
import * as zlib from 'zlib';
import * as tar from 'tar';
import * as csv from 'fast-csv';
import knex, { Knex } from 'knex';
import { DUMP_DOWNLOAD_URL, SQLITE_DB_PATH } from './resources';

interface Customer {
    id: number;
    customerId: string;
    firstName: string;
    lastName: string;
    company: string;
    city: string;
    country: string;
    phone1: string;
    phone2: string;
    email: string;
    subscriptionDate: string;
    website: string;
}

interface Organization {
    id: number;
    organizationId: number;
    name: string;
    website: string;
    country: string;
    description: string;
    founded: string;
    industry: string;
    numberOfEmployees: number;
}

/**
 * Downloads a file from the specified URL and saves it to the destination path.
 * 
 * @param url - The URL of the file to download.
 * @param destination - The path where the downloaded file should be saved.
 * @returns A Promise that resolves when the file is successfully downloaded and saved, or rejects with an error if there was a problem.
 */
async function downloadFile(url: string, destination: string): Promise<void> {
    console.log('downloading file function...');
    /// Create a stream to write data to the file on disk
    return new Promise<void>((resolve, reject) => {
        const file = fs.createWriteStream(destination);
        let downloadedBytes = 0;
        /**
         * Makes a GET request to the specified URL using HTTPS and downloads the response to a file.
         * @param {string} url - The URL to make the GET request to.
         * @param {http.IncomingMessage} response - The response object from the GET request.
         * @returns None
         */
        https.get(url, response => {
            const totalBytes = Number(response.headers['content-length']);

            response.on('data', (chunk) => {
                downloadedBytes += chunk.length;
                const percentage = ((downloadedBytes / totalBytes) * 100).toFixed(2);
                /**
                 * Logs a message indicating the percentage of a download completed.
                 * @param {number} percentage - The percentage of the download completed.
                 * @returns None
                 */
                console.log(`Downloaded: ${percentage}%`);
                
            });
            response.pipe(file);
            /**
             * Event listener for the 'finish' event of a file stream.
             * Closes the file and resolves the promise.
             * @returns None
             */
            file.on('finish', () => {
                file.close();
                resolve();
            });
        }).on('error', err => {
            fs.unlinkSync(destination);
            reject(err.message);
        });
    });
}

/**
 * Extracts the contents of an archive file to a specified destination.
 * 
 * @param archivePath - The path to the archive file.
 * @param destination - The destination directory where the contents of the archive will be extracted.
 * @returns A Promise that resolves when the extraction is complete, or rejects with an error.
 */
async function extractArchive(archivePath: string, destination: string): Promise<void> {
   
    return new Promise<void>((resolve, reject) => {
        fs.createReadStream(archivePath)
            .pipe(zlib.createGunzip())
            .on('error', reject)
            .pipe(tar.extract({ cwd: destination }))
            .on('error', reject)
            .on('end', resolve);
    });
}

/**
 * Parses a CSV file and returns an array of objects of type T.
 * 
 * @param filePath The path to the CSV file.
 * @returns A promise that resolves to an array of objects of type T.
 */
async function parseCSV<T>(filePath: string): Promise<T[]> {
    return new Promise<T[]>((resolve, reject) => {
        const data: T[] = [];
        const indices = new Set();
        /**
         * Reads a CSV file from the specified file path, parses it with headers, and processes each row of data.
         * If a duplicate index is found, it logs an error message.
         * @param {string} filePath - The path to the CSV file to be read.
         * @returns None
         */
        fs.createReadStream(filePath)
            .pipe(csv.parse({ headers: true }))
            .on('error', reject)
            .on('data', row => {
                row.Index = Number(row.Index);
                data.push(row);
            })
            .on('end', () => resolve(data));
    });
}

/**
 * Initializes the database and creates necessary tables if they don't exist.
 * @returns A Promise that resolves to a Knex instance representing the database connection.
 */
async function initializeDatabase(): Promise<Knex<any, unknown[]>> {
    const db: Knex = knex({
        client: 'sqlite3',
        connection: {
            filename: SQLITE_DB_PATH
        },
        useNullAsDefault: true
    });
/// Create table queries for customers and organizations
    await db.schema.createTableIfNotExists('customers', table => {
        table.integer('Index').notNullable().primary();
        table.string('Customer Id').notNullable();
        table.string('First Name').notNullable();
        table.string('Last Name').notNullable();
        table.string('Company').notNullable();
        table.string('City').notNullable(); // Add 'city' column here
        table.string('Country').notNullable();
        table.string('Phone 1').notNullable();
        table.string('Phone 2').notNullable();
        table.string('Email').notNullable();
        table.datetime('Subscription Date').notNullable();
        table.string('Website').notNullable();
    });

    await db.schema.createTableIfNotExists('organizations', table => {
        table.integer('Index').primary();
        table.integer('Organization Id').notNullable();
        table.string('Name').notNullable();
        table.string('Website').notNullable();
        table.string('Country').notNullable();
        table.string('Description').notNullable();
        table.string('Founded').notNullable();
        table.string('Industry').notNullable();
        table.integer('Number of employees').notNullable();
    });

    return db;
}

/**
 * Inserts data into a database table in batches.
 * 
 * @param db - The Knex instance representing the database connection.
 * @param tableName - The name of the table where the data will be inserted.
 * @param data - An array of data to be inserted into the table.
 * @returns A Promise that resolves when the data has been inserted successfully.
 */
async function insertDataToDatabase<T>(db: Knex<any, unknown[]>, tableName: string, data: T[]): Promise<void> {
    const batchSize = 100;
    const totalBatches = Math.ceil(data.length / batchSize);

    for (let i = 0; i < data.length; i += batchSize) {
        try {
            await db(tableName).insert(data.slice(i, i + batchSize));
            const currentBatch = Math.floor(i / batchSize) + 1;
            console.log(`Inserted batch ${currentBatch} of ${totalBatches} of data into the ${tableName} table.`);
        } catch (error) {
            console.log("🚀 ~ error:", error)
        }
    }
}

/**
 * Downloads a file, extracts an archive, initializes a database, clears tables,
 * parses and inserts data into the database.
 * @returns A Promise that resolves when the process is completed successfully.
 */
export async function processDataDump(): Promise<void> {
    const tmpDir = path.join(__dirname, 'tmp');
    const archivePath = path.join(tmpDir, 'dump.tar.gz');
    const extractionPath = path.join(tmpDir, 'extracted');
    // Step 1: Download the file
    console.log('Downloading the file...');
    await downloadFile('https://fiber-challenges.s3.amazonaws.com/dump.tar.gz', archivePath);
    console.log('Download completed.');

    // Step 2: Extract the archive
    console.log('Extracting the archive...');
    await extractArchive(archivePath, extractionPath);
    console.log('Extraction completed.');

    // Step 3: Initialize the database
    console.log('Initializing the database...');
    const db = await initializeDatabase();
    console.log('Database initialized.');

    //  Clear tables if at all any data exists in them
    console.log('Clearing tables...');
    await db('customers').truncate();
    await db('organizations').truncate();
    console.log('Tables cleared.');

    // Step 4: Parse and insert data into the database
    console.log('Parsing the extracted file...');
    const customers = await parseCSV<Customer>(path.join(extractionPath, 'dump', 'customers.csv'));
    const organizations = await parseCSV<Organization>(path.join(extractionPath, 'dump', 'organizations.csv'));
    console.log('Data parsed.');
    console.log('Inserting customer data into the database...')
    await insertDataToDatabase(db, 'customers', customers);
    console.log('Inserting organization data into the database...')
    await insertDataToDatabase(db, 'organizations', organizations);

    console.log('Data insertion completed.');

    // Close the database connection
    await db.destroy();

    console.log('Process completed successfully.');
}


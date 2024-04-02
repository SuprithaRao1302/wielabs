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
    lastName:string;
    company:string;
    city:string;
    country:string;
    phone1: string;
    phone2: string;
    email: string;
    subscriptionDate:string;
    website: string;
    // Add other fields as needed
}

interface Organization {
    id: number;
    organizationId:number;
    name: string;
    website: string;
    country: string;
    description: string;
    founded: string;
    industry: string;
    numberOfEmployees:number;
    // Add other fields as needed
}

async function downloadFile(url: string, destination: string): Promise<void> {
    return new Promise<void>((resolve, reject) => {
        const file = fs.createWriteStream(destination);
        https.get(url, response => {
            response.pipe(file);
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

async function parseCSV<T>(filePath: string): Promise<T[]> {
    return new Promise<T[]>((resolve, reject) => {
        const data: T[] = [];
        fs.createReadStream(filePath)
            .pipe(csv.parse({ headers: true }))
            .on('error', reject)
            .on('data', row => data.push(row))
            .on('end', () => resolve(data));
    });
}

async function initializeDatabase(): Promise<Knex<any, unknown[]>> {
    const db :Knex = knex({
        client: 'sqlite3',
        connection: {
            filename: SQLITE_DB_PATH
        },
        useNullAsDefault: true
    });

    await db.schema.createTableIfNotExists('customers', table => {
        table.increments('Index').primary();
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
        // Define other columns as needed
    });

    await db.schema.createTableIfNotExists('organizations', table => {
        table.increments('Index').primary();
        table.integer('Organization Id').notNullable();
        table.string('Name').notNullable();
        table.string('Website').notNullable();
        table.string('Country').notNullable();
        table.string('Description').notNullable();
        table.string('Founded').notNullable();
        table.string('Industry').notNullable();
        table.integer('Number of employees').notNullable();
        // Define other columns as needed
    });

    return db;
}

async function insertDataToDatabase<T>(db: Knex<any, unknown[]>, tableName: string, data: T[]): Promise<void> {
    const batchSize = 100;
    for (let i = 0; i < data.length; i += batchSize) {
        await db(tableName).insert(data.slice(i, i + batchSize));
    }
}

export async function processDataDump(): Promise<void> {
    const tmpDir = path.join(__dirname, 'tmp');
    const archivePath = path.join(tmpDir, 'dump.tar.gz');
    const extractionPath = path.join(tmpDir, 'extracted');
    // Step 1: Download the file
    console.log('Downloading the filed...');
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

    // Step 4: Parse and insert data into the database
    console.log('Parsing and inserting data into the database...');
    const customers = await parseCSV<Customer>(path.join(extractionPath, 'dump','customers.csv'));
    const organizations = await parseCSV<Organization>(path.join(extractionPath,'dump','organizations.csv'));

    await insertDataToDatabase(db, 'customers', customers);
    await insertDataToDatabase(db, 'organizations', organizations);

    console.log('Data insertion completed.');

    // Close the database connection
    await db.destroy();

    console.log('Process completed successfully.');
}

// Call processDataDump function to execute the pipeline
processDataDump().catch(error => {
    console.error('An error occurred:', error);
    process.exit(1);
});

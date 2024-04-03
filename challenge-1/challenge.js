"use strict";
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
var __generator = (this && this.__generator) || function (thisArg, body) {
    var _ = { label: 0, sent: function() { if (t[0] & 1) throw t[1]; return t[1]; }, trys: [], ops: [] }, f, y, t, g;
    return g = { next: verb(0), "throw": verb(1), "return": verb(2) }, typeof Symbol === "function" && (g[Symbol.iterator] = function() { return this; }), g;
    function verb(n) { return function (v) { return step([n, v]); }; }
    function step(op) {
        if (f) throw new TypeError("Generator is already executing.");
        while (g && (g = 0, op[0] && (_ = 0)), _) try {
            if (f = 1, y && (t = op[0] & 2 ? y["return"] : op[0] ? y["throw"] || ((t = y["return"]) && t.call(y), 0) : y.next) && !(t = t.call(y, op[1])).done) return t;
            if (y = 0, t) op = [op[0] & 2, t.value];
            switch (op[0]) {
                case 0: case 1: t = op; break;
                case 4: _.label++; return { value: op[1], done: false };
                case 5: _.label++; y = op[1]; op = [0]; continue;
                case 7: op = _.ops.pop(); _.trys.pop(); continue;
                default:
                    if (!(t = _.trys, t = t.length > 0 && t[t.length - 1]) && (op[0] === 6 || op[0] === 2)) { _ = 0; continue; }
                    if (op[0] === 3 && (!t || (op[1] > t[0] && op[1] < t[3]))) { _.label = op[1]; break; }
                    if (op[0] === 6 && _.label < t[1]) { _.label = t[1]; t = op; break; }
                    if (t && _.label < t[2]) { _.label = t[2]; _.ops.push(op); break; }
                    if (t[2]) _.ops.pop();
                    _.trys.pop(); continue;
            }
            op = body.call(thisArg, _);
        } catch (e) { op = [6, e]; y = 0; } finally { f = t = 0; }
        if (op[0] & 5) throw op[1]; return { value: op[0] ? op[1] : void 0, done: true };
    }
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.processDataDump = void 0;
/*
Here's the pseudocode i followed to complete this challenge
Download the .tar.gz file using Node.js's Streams API and save it to the tmp/ folder.
Decompress the GZIP part of the file and extract the TAR archive, saving the resulting folder in tmp/.
Set up a SQLite database at out/database.sqlite using knex.
Read the two CSVs in the extracted folder using a streaming API.
Add each row to the SQL database. For efficiency, add about 100 rows at a time using a batch insert.
Ensure that the SQL tables have a unique, auto-incrementing primary key column called id and that all columns are non-nullable.
*/
var fs = require("fs");
var path = require("path");
var https = require("https");
var zlib = require("zlib");
var tar = require("tar");
var csv = require("fast-csv");
var knex_1 = require("knex");
var resources_1 = require("./resources");
/**
 * Downloads a file from the specified URL and saves it to the destination path.
 *
 * @param url - The URL of the file to download.
 * @param destination - The path where the downloaded file should be saved.
 * @returns A Promise that resolves when the file is successfully downloaded and saved, or rejects with an error if there was a problem.
 */
function downloadFile(url, destination) {
    return __awaiter(this, void 0, void 0, function () {
        return __generator(this, function (_a) {
            console.log('downloading file function...');
            /// Create a stream to write data to the file on disk
            return [2 /*return*/, new Promise(function (resolve, reject) {
                    var file = fs.createWriteStream(destination);
                    var downloadedBytes = 0;
                    /**
                     * Makes a GET request to the specified URL using HTTPS and downloads the response to a file.
                     * @param {string} url - The URL to make the GET request to.
                     * @param {http.IncomingMessage} response - The response object from the GET request.
                     * @returns None
                     */
                    https.get(url, function (response) {
                        var totalBytes = Number(response.headers['content-length']);
                        response.on('data', function (chunk) {
                            downloadedBytes += chunk.length;
                            var percentage = ((downloadedBytes / totalBytes) * 100).toFixed(2);
                            /**
                             * Logs a message indicating the percentage of a download completed.
                             * @param {number} percentage - The percentage of the download completed.
                             * @returns None
                             */
                            console.log("Downloaded: ".concat(percentage, "%"));
                        });
                        response.pipe(file);
                        /**
                         * Event listener for the 'finish' event of a file stream.
                         * Closes the file and resolves the promise.
                         * @returns None
                         */
                        file.on('finish', function () {
                            file.close();
                            resolve();
                        });
                    }).on('error', function (err) {
                        fs.unlinkSync(destination);
                        reject(err.message);
                    });
                })];
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
function extractArchive(archivePath, destination) {
    return __awaiter(this, void 0, void 0, function () {
        return __generator(this, function (_a) {
            return [2 /*return*/, new Promise(function (resolve, reject) {
                    fs.createReadStream(archivePath)
                        .pipe(zlib.createGunzip())
                        .on('error', reject)
                        .pipe(tar.extract({ cwd: destination }))
                        .on('error', reject)
                        .on('end', resolve);
                })];
        });
    });
}
/**
 * Parses a CSV file and returns an array of objects of type T.
 *
 * @param filePath The path to the CSV file.
 * @returns A promise that resolves to an array of objects of type T.
 */
function parseCSV(filePath) {
    return __awaiter(this, void 0, void 0, function () {
        return __generator(this, function (_a) {
            return [2 /*return*/, new Promise(function (resolve, reject) {
                    var data = [];
                    var indices = new Set();
                    /**
                     * Reads a CSV file from the specified file path, parses it with headers, and processes each row of data.
                     * If a duplicate index is found, it logs an error message.
                     * @param {string} filePath - The path to the CSV file to be read.
                     * @returns None
                     */
                    fs.createReadStream(filePath)
                        .pipe(csv.parse({ headers: true }))
                        .on('error', reject)
                        .on('data', function (row) {
                        var index = Number(row.Index);
                        if (indices.has(index)) {
                            console.error("Duplicate index found: ".concat(index));
                        }
                        else {
                            indices.add(index);
                            row.Index = index;
                            data.push(row);
                        }
                    })
                        .on('end', function () { return resolve(data); });
                })];
        });
    });
}
/**
 * Initializes the database and creates necessary tables if they don't exist.
 * @returns A Promise that resolves to a Knex instance representing the database connection.
 */
function initializeDatabase() {
    return __awaiter(this, void 0, void 0, function () {
        var db;
        return __generator(this, function (_a) {
            switch (_a.label) {
                case 0:
                    db = (0, knex_1.default)({
                        client: 'sqlite3',
                        connection: {
                            filename: resources_1.SQLITE_DB_PATH
                        },
                        useNullAsDefault: true
                    });
                    /// Create table queries for customers and organizations
                    return [4 /*yield*/, db.schema.createTableIfNotExists('customers', function (table) {
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
                        })];
                case 1:
                    /// Create table queries for customers and organizations
                    _a.sent();
                    return [4 /*yield*/, db.schema.createTableIfNotExists('organizations', function (table) {
                            table.integer('Index').primary();
                            table.integer('Organization Id').notNullable();
                            table.string('Name').notNullable();
                            table.string('Website').notNullable();
                            table.string('Country').notNullable();
                            table.string('Description').notNullable();
                            table.string('Founded').notNullable();
                            table.string('Industry').notNullable();
                            table.integer('Number of employees').notNullable();
                        })];
                case 2:
                    _a.sent();
                    return [2 /*return*/, db];
            }
        });
    });
}
/**
 * Inserts data into a database table in batches.
 *
 * @param db - The Knex instance representing the database connection.
 * @param tableName - The name of the table where the data will be inserted.
 * @param data - An array of data to be inserted into the table.
 * @returns A Promise that resolves when the data has been inserted successfully.
 */
function insertDataToDatabase(db, tableName, data) {
    return __awaiter(this, void 0, void 0, function () {
        var batchSize, i, error_1;
        return __generator(this, function (_a) {
            switch (_a.label) {
                case 0:
                    batchSize = 100;
                    i = 0;
                    _a.label = 1;
                case 1:
                    if (!(i < data.length)) return [3 /*break*/, 6];
                    _a.label = 2;
                case 2:
                    _a.trys.push([2, 4, , 5]);
                    return [4 /*yield*/, db(tableName).insert(data.slice(i, i + batchSize))];
                case 3:
                    _a.sent();
                    return [3 /*break*/, 5];
                case 4:
                    error_1 = _a.sent();
                    console.log("🚀 ~ error:", error_1);
                    return [3 /*break*/, 5];
                case 5:
                    i += batchSize;
                    return [3 /*break*/, 1];
                case 6: return [2 /*return*/];
            }
        });
    });
}
/**
 * Downloads a file, extracts an archive, initializes a database, clears tables,
 * parses and inserts data into the database.
 * @returns A Promise that resolves when the process is completed successfully.
 */
function processDataDump() {
    return __awaiter(this, void 0, void 0, function () {
        var tmpDir, archivePath, extractionPath, db, customers, organizations;
        return __generator(this, function (_a) {
            switch (_a.label) {
                case 0:
                    tmpDir = path.join(__dirname, 'tmp');
                    archivePath = path.join(tmpDir, 'dump.tar.gz');
                    extractionPath = path.join(tmpDir, 'extracted');
                    // Step 1: Download the file
                    console.log('Downloading the file...');
                    return [4 /*yield*/, downloadFile('https://fiber-challenges.s3.amazonaws.com/dump.tar.gz', archivePath)];
                case 1:
                    _a.sent();
                    console.log('Download completed.');
                    // Step 2: Extract the archive
                    console.log('Extracting the archive...');
                    return [4 /*yield*/, extractArchive(archivePath, extractionPath)];
                case 2:
                    _a.sent();
                    console.log('Extraction completed.');
                    // Step 3: Initialize the database
                    console.log('Initializing the database...');
                    return [4 /*yield*/, initializeDatabase()];
                case 3:
                    db = _a.sent();
                    console.log('Database initialized.');
                    //  Clear tables if at all any data exists in them
                    console.log('Clearing tables...');
                    return [4 /*yield*/, db('customers').truncate()];
                case 4:
                    _a.sent();
                    return [4 /*yield*/, db('organizations').truncate()];
                case 5:
                    _a.sent();
                    console.log('Tables cleared.');
                    // Step 4: Parse and insert data into the database
                    console.log('Parsing and inserting data into the database...');
                    return [4 /*yield*/, parseCSV(path.join(extractionPath, 'dump', 'customers.csv'))];
                case 6:
                    customers = _a.sent();
                    return [4 /*yield*/, parseCSV(path.join(extractionPath, 'dump', 'organizations.csv'))];
                case 7:
                    organizations = _a.sent();
                    return [4 /*yield*/, insertDataToDatabase(db, 'customers', customers)];
                case 8:
                    _a.sent();
                    return [4 /*yield*/, insertDataToDatabase(db, 'organizations', organizations)];
                case 9:
                    _a.sent();
                    console.log('Data insertion completed.');
                    // Close the database connection
                    return [4 /*yield*/, db.destroy()];
                case 10:
                    // Close the database connection
                    _a.sent();
                    console.log('Process completed successfully.');
                    return [2 /*return*/];
            }
        });
    });
}
exports.processDataDump = processDataDump;
// Call processDataDump function to execute the pipeline
processDataDump().catch(function (error) {
    console.error('An error occurred:', error);
    process.exit(1);
});

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
var fs = require("fs");
var path = require("path");
var https = require("https");
var zlib = require("zlib");
var tar = require("tar");
var csv = require("fast-csv");
var knex_1 = require("knex");
var resources_1 = require("./resources");
function downloadFile(url, destination) {
    return __awaiter(this, void 0, void 0, function () {
        return __generator(this, function (_a) {
            return [2 /*return*/, new Promise(function (resolve, reject) {
                    var file = fs.createWriteStream(destination);
                    https.get(url, function (response) {
                        response.pipe(file);
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
function parseCSV(filePath) {
    return __awaiter(this, void 0, void 0, function () {
        return __generator(this, function (_a) {
            return [2 /*return*/, new Promise(function (resolve, reject) {
                    var data = [];
                    fs.createReadStream(filePath)
                        .pipe(csv.parse({ headers: true }))
                        .on('error', reject)
                        .on('data', function (row) {
                        row.Index = Number(row.Index);
                        data.push(row);
                    })
                        .on('end', function () { return resolve(data); });
                })];
        });
    });
}
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
                            // Define other columns as needed
                        })];
                case 1:
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
                            // Define other columns as needed
                        })];
                case 2:
                    _a.sent();
                    return [2 /*return*/, db];
            }
        });
    });
}
function insertDataToDatabase(db, tableName, data) {
    return __awaiter(this, void 0, void 0, function () {
        var batchSize, i;
        return __generator(this, function (_a) {
            switch (_a.label) {
                case 0:
                    batchSize = 100;
                    i = 0;
                    _a.label = 1;
                case 1:
                    if (!(i < data.length)) return [3 /*break*/, 4];
                    return [4 /*yield*/, db(tableName).insert(data.slice(i, i + batchSize))];
                case 2:
                    _a.sent();
                    _a.label = 3;
                case 3:
                    i += batchSize;
                    return [3 /*break*/, 1];
                case 4: return [2 /*return*/];
            }
        });
    });
}
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
                    console.log('Downloading the filed...');
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
                    // Step 4: Parse and insert data into the database
                    console.log('Parsing and inserting data into the database...');
                    return [4 /*yield*/, parseCSV(path.join(extractionPath, 'dump', 'customers.csv'))];
                case 4:
                    customers = _a.sent();
                    return [4 /*yield*/, parseCSV(path.join(extractionPath, 'dump', 'organizations.csv'))];
                case 5:
                    organizations = _a.sent();
                    return [4 /*yield*/, insertDataToDatabase(db, 'customers', customers)];
                case 6:
                    _a.sent();
                    return [4 /*yield*/, insertDataToDatabase(db, 'organizations', organizations)];
                case 7:
                    _a.sent();
                    console.log('Data insertion completed.');
                    // Close the database connection
                    return [4 /*yield*/, db.destroy()];
                case 8:
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

import { processCompanyList } from "./challenge";

/**
 * This is the entry point for the challenge.
 * This will run your code.
 */
async function run() {
    await processCompanyList();
    console.log("✅ Done!");
}

run();
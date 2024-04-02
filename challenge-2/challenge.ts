import fs from 'fs/promises';
import cheerio from 'cheerio';
import { CheerioCrawler, CheerioCrawlerOptions } from 'crawlee'; // Import the CheerioCrawler class from the correct package
import axios from 'axios';
import Papa from 'papaparse';

interface Company {
    companyName: string;
    tagline: string;
    description: string;
    location: string;
    linkedin: string;
    website: string;
    twitter: string;
    github: string;
    facebook: string;
    founded?: string;
    teamSize?: string;
    logo: string;
    jobs: Job[];
    founders: Founder[];
    launchPosts?: LaunchPost[];
    newsPosts: NewsPost[];
    // Add more fields as needed
}

interface Job {
    role: string;
    location: string;
    salary: string;
    equity: string;
    experience: string;
    applyAt: string;
    // Add more fields as needed
}

interface Founder {
    name: string;
    linkedIn?: string;
    image?: string;
    description?: string;
    twitter?: string;
    // Add more fields as needed
}

interface LaunchPost {
    title: string;
    tagline: string;
    author: string;
    authorImage: string;
    date: string;
    tags: string[];
    url: string;
    // Add more fields as needed
}

interface NewsPost {
    title: string;
    link: string;
    date: string;
    // Add more fields as needed
}

export async function processCompanyList(): Promise<void> {
    // Read the list of companies from the CSV file
    console.log('Reading company list from CSV file...');
    const csvData = await fs.readFile('inputs/companies.csv', 'utf-8');
    // Parse the CSV data to extract company names and URLs

    const companyList: { name: string; url: string }[] = parseCsv(csvData);
    console.log('Scraping company information...');
    // Initialize an array to store scraped company information
    const scrapedCompanies: Company[] = [];

    // Iterate over each company and scrape its information
    for (const { name, url } of companyList) {
        const companyInfo = await scrapeCompanyInfo(name, url);
        scrapedCompanies.push(companyInfo);
    }
    console.log('Writing scraped company information to JSON file...');
    // Write the scraped company information to a JSON file
    await fs.writeFile('out/scraped.json', JSON.stringify(scrapedCompanies, null, 2));
    console.log('Scraping completed successfully!');
}

function parseCsv(csvData: string): { name: string; url: string }[] {
    const parsedData = Papa.parse(csvData, {
        delimiter: ",", // Set the delimiter to comma
        quoteChar: '"', // Specify the quote character
        header: true, // Indicates that the first row is the header
        skipEmptyLines: true, // Skip empty lines
    });

    return parsedData.data.map((row: any) => {
        return { name: row['Company Name'], url: row['YC URL'] };
    });
}

async function scrapeCompanyInfo(name: string, url: string): Promise<Company> {
    console.log("🚀 ~ scrapeCompanyInfo ~ url:", url)
    const company: Company = {
        companyName: '',
        jobs: [],
        founders: [],
        launchPosts: [],
        tagline: '',
        description: '',
        location: '',
        linkedin: '',
        website: '',
        twitter: '',
        github: '',
        facebook: '',
        logo: '',
        newsPosts: []
    };

    const crawler = new CheerioCrawler({
        minConcurrency: 10,
        maxConcurrency: 50,

        // On error, retry each page at most once.
        maxRequestRetries: 1,

        // Increase the timeout for processing of each page.
        requestHandlerTimeoutSecs: 30,

        // Remove the limit on the number of requests per crawl
        // or increase it to a larger number if you know how many companies you have.
        maxRequestsPerCrawl: 200,
        requestHandler: async ({ request, response, $ }) => {

            // Extract relevant information from the company's page using Cheerio selectors
            // Example:
            company.companyName = $('h1.font-extralight').text().trim();
            company.tagline = $('div.text-xl').text().trim();
            company.founded = $('div.flex.flex-row.justify-between span:contains("Founded:")').next().text().trim();
            company.teamSize = $('div.flex.flex-row.justify-between span:contains("Team Size:")').next().text().trim();
            company.location = $('div.flex.flex-row.justify-between span:contains("Location:")').next().text().trim();
            company.logo = $('img.h-full.w-full').attr('src') ?? '';
            company.linkedin = $('a.inline-block.w-5.h-5.bg-contain.bg-image-linkedin').attr('href') ?? '';
            company.twitter = $('a.inline-block w-5 h-5 bg-contain bg-image-twitter').attr('href') ?? '';
            company.github = $('a.inline-block w-5 h-5 bg-contain bg-image-github').attr('href') ?? '';
            company.facebook = $('a.inline-block w-5 h-5 bg-contain bg-image-facebook').attr('href') ?? '';
            company.website = $('a[href^="http"]').attr('href') ?? '';
            company.description = $('p.whitespace-pre-line').text().trim();
            $('div.flex.w-full.flex-col.justify-between.divide-y.divide-gray-200 > div.flex.w-full.flex-row.justify-between.py-4').each((index, element) => {
                const jobTitle = $(element).find('div.ycdc-with-link-color.pr-4.text-lg.font-bold > a').text().trim();
                const jobDetails = $(element).find('div.justify-left.flex.flex-row.gap-x-7 > div').map((i, el) => $(el).text().trim()).get();

                // Initialize empty strings for all fields
                let location = '';
                let salary = '';
                let equity = '';
                let years = '';

                // Iterate over job details and assign them dynamically
                jobDetails.forEach(detail => {
                    if (detail.includes('€') || detail.includes('$')) {
                        salary = detail;
                    } else if (detail.includes('%')) {
                        equity = detail;
                    } else if (detail.includes('years') || detail.includes('year')) {
                        years = detail;
                    } else {
                        location = detail;
                    }
                });

                const applyLink = $(element).find('div.APPLY > a').attr('href');

                const job: Job = {
                    role: jobTitle,
                    location,
                    salary,
                    equity,
                    experience: years,
                    applyAt: applyLink!,
                };

                company.jobs.push(job);
            });
            $('div.flex.flex-row.flex-col.items-start.gap-3.md\\:flex-row').each((index, element) => {
                const name = $(element).find('h3.text-lg.font-bold').text().trim();
                const description = $(element).find('p.prose.max-w-full.whitespace-pre-line').text().trim();
                const linkedIn = $(element).find('a[href^="https://linkedin.com"]').attr('href') || '';
                const image = $(element).find('img').attr('src') || '';
                const twitter = $(element).find('a[href^="https://twitter.com"]').attr('href') || '';

                const founder: Founder = {
                    name,
                    linkedIn,
                    image,
                    description,
                    twitter
                };

                company.founders.push(founder);
            });
            const launchPostElements = $('div.prose.max-w-full a.ycdc-with-link-color').toArray();
            console.log("🚀 ~ handlePageFunction: ~ launchPostElements:", launchPostElements)
            for (const element of launchPostElements) {
                const launchInfo = await scrapeLaunchInfo(element.attribs.href);
                if (launchInfo) {
                    if (!company.launchPosts) {
                        company.launchPosts = []; // Initialize the launchPosts array if it's undefined
                    }
                    company.launchPosts.push(launchInfo);
                }
            }

            $('#news div.ycdc-with-link-color').each((index, element) => {
                const link = $(element).find('a').attr('href') ?? '';
                const title = $(element).find('a').text().trim();
                const date = $(element).find('time').text().trim();
                company.newsPosts.push({ title, link, date });
            });
        },
    });

    await crawler.run([url]); // Use the run method from the CheerioCrawler class
    console.log('one company scraped successfully!')
    return company;
}
async function scrapeLaunchInfo(anchorTagUrl: string): Promise<LaunchPost | null> {
    console.log("🚀 ~ scrapeLaunchInfo ~ anchorTagUrl:", anchorTagUrl)
    try {
        const response = await axios.get('https://www.ycombinator.com/launches');
        const $ = cheerio.load(response.data);
        const launchPage = await axios.get(`https://www.ycombinator.com${anchorTagUrl}`);
        const $launch = cheerio.load(launchPage.data);
        const hashtags: string[] = [];
        const title = $launch('div.row.space-between.align-start.width-100 h1').text().trim();
        const authorImage = $launch('div.user-image.background-image').attr('style') ?? '';
        const authorName = $launch('div.flex div b').text().trim();
        const tagline = $launch('p.tagline').text().trim();
        const date = $launch('time.timeago').attr('title') ?? '';
        $launch('div.row.hashtags span').each((index, element) => {
            const hashtag = $(element).text();
            hashtags.push(hashtag);
        });
        const url = $launch('a.post-url').attr('href') ?? '';
        return {
            title, author: authorName, tagline, authorImage,
            date,
            tags: hashtags,
            url
        };
    } catch (error) {
        console.error('Error scraping launch info:', error);
        return null;
    }
}

// Call the function to start the processing
processCompanyList().catch(console.error);

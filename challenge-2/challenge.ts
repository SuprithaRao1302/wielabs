/*
Here's the pseudocode i followed to complete this challenge
Parse the given list of company names and URLs from inputs/companies.csv using a CSV parsing library like fast-csv or papaparse.
For each company, use the CheerioCrawler from the crawlee library to scrape the company's YC profile page.
Use cheerio to parse the HTML of the page and extract key information about the company into a TypeScript interface.
If a company's "Launch" post is available, visit that URL and scrape it as well, extracting the information into another TypeScript interface.
Combine all the information about each company into a single interface.
Write the resulting array of objects (one object per company) to out/scraped.json.
*/


import fs from "fs/promises";
import * as cheerio from "cheerio";
import { CheerioCrawler, CheerioCrawlerOptions } from "crawlee"; // Import the CheerioCrawler class from the correct package
import axios from "axios";
import Papa from "papaparse";

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
}

interface Job {
  role: string;
  location: string;
  salary: string;
  equity: string;
  experience: string;
  applyAt: string;
}

interface Founder {
  name: string;
  linkedIn?: string;
  image?: string;
  description?: string;
  twitter?: string;
}

interface LaunchPost {
  linkToPost: string;
  title: string;
  tagline: string;
  author: string;
  authorImage: string;
  date: string;
  tags: string[];
  url: string;
}

interface NewsPost {
  title: string;
  link: string;
  date: string;
}

/**
 * Processes the list of companies by reading from a CSV file, scraping company information,
 * and writing the scraped information to a JSON file.
 * @returns A Promise that resolves when the processing is complete.
 */
export async function processCompanyList(): Promise<void> {
  // Read the list of companies from the CSV file
  console.log("Reading company list from CSV file...");
  const csvData = await fs.readFile("inputs/companies.csv", "utf-8");
  // Parse the CSV data to extract company names and URLs

  const companyList: { name: string; url: string }[] = parseCsv(csvData);
  console.log("Scraping company information...");
  // Initialize an array to store scraped company information
  const scrapedCompanies: Company[] = [];
  let a = [];
  // Iterate over each company and scrape its information
  for (const { name, url } of companyList) {
    const companyInfo = await scrapeCompanyInfo(name, url);
    scrapedCompanies.push(companyInfo);
  }
  console.log("Writing scraped company information to JSON file...");
  // Write the scraped company information to a JSON file
  await fs.writeFile(
    "out/scraped.json",
    JSON.stringify(scrapedCompanies, null, 2)
  );
  console.log(
    "Scraping completed successfully!",

  );
}

/**
 * Parses a CSV string and returns an array of objects containing the name and URL.
 * @param csvData - The CSV data to parse.
 * @returns An array of objects with the name and URL extracted from the CSV data.
 */
function parseCsv(csvData: string): { name: string; url: string }[] {
  // Use the PapaParse library to parse the CSV data
  const parsedData = Papa.parse(csvData, {
    delimiter: ",", // Set the delimiter to comma
    quoteChar: '"', // Specify the quote character
    header: true, // Indicates that the first row is the header
    skipEmptyLines: true, // Skip empty lines
  });

  return parsedData.data.map((row: any) => {
    return { name: row["Company Name"], url: row["YC URL"] };
  });
}

/**
 * Scrapes company information from a given URL.
 *
 * @param name - The name of the company.
 * @param url - The URL of the company's website.
 * @returns A Promise that resolves to a Company object containing the scraped information.
 */
async function scrapeCompanyInfo(name: string, url: string): Promise<Company> {
  console.log("🚀 ~ scrapeCompanyInfo ~ url:", url);
  const company: Company = {
    companyName: "",
    jobs: [],
    founders: [],
    launchPosts: [],
    tagline: "",
    description: "",
    location: "",
    linkedin: "",
    website: "",
    twitter: "",
    github: "",
    facebook: "",
    logo: "",
    newsPosts: [],
  };
  // Create a new instance of the CheerioCrawler class with the appropriate configuration
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
      const comp = $("h1.font-extralight");
      company.companyName = $("h1.font-extralight").text().trim();
      company.tagline = $("div.text-xl").text().trim();
      company.founded = $(
        'div.flex.flex-row.justify-between span:contains("Founded:")'
      )
        .next()
        .text()
        .trim();
      company.teamSize = $(
        'div.flex.flex-row.justify-between span:contains("Team Size:")'
      )
        .next()
        .text()
        .trim();
      company.location = $(
        'div.flex.flex-row.justify-between span:contains("Location:")'
      )
        .next()
        .text()
        .trim();
      company.logo = $("img.h-full.w-full").attr("src") ?? "";
      company.linkedin =
        $("a.inline-block.w-5.h-5.bg-contain.bg-image-linkedin").attr("href") ??
        "";
      company.twitter =
        $("a.inline-block.w-5.h-5.bg-contain.bg-image-twitter").attr("href") ??
        "";
      company.github =
        $("a.inline-block.w-5.h-5.bg-contain.bg-image-github").attr("href") ??
        "";
      company.facebook =
        $("a.inline-block.w-5.h-5.bg-contain.bg-image-facebook").attr("href") ??
        "";
      company.website = $('a[href^="http"]').attr("href") ?? "";
      company.description = $("p.whitespace-pre-line").text().trim();
      // Scrape job listings
      $(
        "div.flex.w-full.flex-col.justify-between.divide-y.divide-gray-200 > div.flex.w-full.flex-row.justify-between.py-4"
      ).each((index, element) => {
        const jobTitle = $(element)
          .find("div.ycdc-with-link-color.pr-4.text-lg.font-bold > a")
          .text()
          .trim();
        const jobDetails = $(element)
          .find("div.justify-left.flex.flex-row.gap-x-7 > div")
          .map((i, el) => $(el).text().trim())
          .get();

        // Initialize empty strings for all fields
        let location = "";
        let salary = "";
        let equity = "";
        let years = "";

        // Iterate over job details and assign them dynamically
        jobDetails.forEach((detail) => {
          if (detail.includes("€") || detail.includes("$")) {
            salary = detail;
          } else if (detail.includes("%")) {
            equity = detail;
          } else if (detail.includes("years") || detail.includes("year")) {
            years = detail;
          } else {
            location = detail;
          }
        });

        const applyLink = $(element).find("div.APPLY > a").attr("href");

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
      // Scrape founders information
      $("div.flex.flex-row.flex-col.items-start.gap-3.md\\:flex-row").each(
        (index, element) => {
          const name = $(element).find("h3.text-lg.font-bold").text().trim();
          const description = $(element)
            .find("p.prose.max-w-full.whitespace-pre-line")
            .text()
            .trim();
          const linkedIn =
            $(element).find('a[href^="https://linkedin.com"]').attr("href") ||
            "";
          const image = $(element).find("img").attr("src") || "";
          const twitter =
            $(element).find('a[href^="https://twitter.com"]').attr("href") ||
            "";

          const founder: Founder = {
            name,
            linkedIn,
            image,
            description,
            twitter,
          };

          company.founders.push(founder);
        }
      );
      // Scrape launch posts
      const launchPostElements = $(
        "div.prose.max-w-full a.ycdc-with-link-color"
      ).toArray();
      for (const element of launchPostElements) {
        const launchInfo = await scrapeLaunchInfo(element.attribs.href);
        if (launchInfo) {
          if (!company.launchPosts) {
            company.launchPosts = []; // Initialize the launchPosts array if it's undefined
          }
          company.launchPosts.push(launchInfo);
        }
      }
      // Scrape news posts
      $("#news div.ycdc-with-link-color").each((index, element) => {
        const link = $(element).find("a").attr("href") ?? "";
        const title = $(element).find("a").text().trim();
        const date = $(element).find("div.mb-4.text-sm").text().trim();
        company.newsPosts.push({ title, link, date });
      });
    },
  });

  await crawler.run([url]); // Use the run method from the CheerioCrawler class
  console.log(name, " scraped successfully!");
  return company;
}
/**
 * Scrapes launch information from a given URL and returns a `LaunchPost` object.
 * @param anchorTagUrl - The URL of the anchor tag to scrape launch information from.
 * @returns A promise that resolves to a `LaunchPost` object if the scraping is successful, or `null` if there was an error.
 */
async function scrapeLaunchInfo(
  anchorTagUrl: string
): Promise<LaunchPost | null> {
  try {
    /**
     * Fetches data from two URLs, parses the HTML content using Cheerio, and extracts
     * information such as title, author image, author name, tagline, and date.
     * @returns None
     */
    const response = await axios.get("https://www.ycombinator.com/launches");
    const $ = cheerio.load(response.data);
    const urlOfLaunchPage = `https://www.ycombinator.com${anchorTagUrl}`
    const launchPage = await axios.get(urlOfLaunchPage);
    const $launch = cheerio.load(launchPage.data);
    const hashtags: string[] = [];
    const title = $launch("div.row.space-between.align-start.width-100 h1")
      .text()
      .trim();
    const authorImage =
      $launch("div.user-image.background-image").attr("style") ?? "";
    const authorName = $launch("div.flex div b").text().trim();
    const tagline = $launch("p.tagline").text().trim();
    const date = $launch("time.timeago").attr("title") ?? "";
    // Extract hashtags from the launch post
    $launch("div.row.hashtags span").each((index, element) => {
      const hashtag = $(element).text();
      hashtags.push(hashtag);
    });
    const url = $launch("a.post-url").attr("href") ?? "";
    return {
      linkToPost:urlOfLaunchPage,
      title,
      author: authorName,
      tagline,
      authorImage,
      date,
      tags: hashtags,
      url,
    };
  } catch (error) {
    console.error("Error scraping launch info:", error);
    return null;
  }
}

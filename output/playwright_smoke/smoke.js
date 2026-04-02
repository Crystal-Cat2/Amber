const { chromium } = require('playwright');
const path = require('node:path');
const { pathToFileURL } = require('node:url');

(async () => {
  const htmlPath = path.join(process.cwd(), 'index.html');
  const screenshotPath = path.join(process.cwd(), 'smoke.png');
  const browser = await chromium.launch({ headless: true });
  const page = await browser.newPage({ viewport: { width: 1280, height: 720 } });
  await page.goto(pathToFileURL(htmlPath).href, { waitUntil: 'load' });
  const text = await page.locator('#app').textContent();
  await page.screenshot({ path: screenshotPath });
  console.log(`DOM_TEXT=${text}`);
  console.log(`SCREENSHOT=${screenshotPath}`);
  await browser.close();
})();

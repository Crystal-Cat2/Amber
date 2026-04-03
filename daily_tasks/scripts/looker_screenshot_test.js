const { chromium } = require('playwright');
const path = require('path');

(async () => {
  // Connect to running Chrome via CDP
  // First try default debug port
  let browser;
  try {
    browser = await chromium.connectOverCDP('http://127.0.0.1:9222');
    console.log('Connected to Chrome via CDP');
  } catch (e) {
    console.error('Cannot connect to Chrome CDP on port 9222.');
    console.error('Please restart Chrome with: chrome.exe --remote-debugging-port=9222');
    console.error('Or run: Start-Process chrome -ArgumentList "--remote-debugging-port=9222"');
    process.exit(1);
  }

  const contexts = browser.contexts();
  const context = contexts[0];
  const page = await context.newPage();

  const url = 'https://lookerstudio.google.com/u/0/reporting/1d949eec-106c-4e46-b837-7ee6d7f356c4/page/p_d6ubr7390c';
  console.log('Navigating to:', url);

  await page.goto(url, { waitUntil: 'networkidle', timeout: 60000 });
  console.log('Page loaded, waiting for render...');
  await page.waitForTimeout(8000);

  const desktop = path.join(process.env.USERPROFILE, 'Desktop');
  await page.screenshot({
    path: path.join(desktop, 'looker_test_fullpage.png'),
    fullPage: true,
  });
  console.log('Full page screenshot saved to Desktop/looker_test_fullpage.png');

  await page.close();
  // Don't close browser - it's the user's running Chrome
})();

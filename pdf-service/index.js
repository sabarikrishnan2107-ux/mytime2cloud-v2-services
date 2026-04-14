const express = require("express");
const cors = require("cors");
const path = require("path");
const puppeteer = require("puppeteer");

const app = express();
app.use(cors({ origin: "*", methods: ["GET", "POST", "OPTIONS"], allowedHeaders: ["Content-Type"] }));
app.use(express.json({ limit: "10mb" }));
app.use("/templates", express.static(path.resolve(__dirname, "..", "summary-report")));
app.use("/attendance-report", express.static(path.resolve(__dirname, "..", "summary-report", "attendance-report")));

app.post("/pdf", async (req, res) => {
  req.setTimeout(300000); // 5 minute timeout
  res.setTimeout(300000);
  const { url, landscape, format } = req.body;
  if (!url) return res.status(400).json({ error: "url is required" });

  console.log("Generating PDF for:", url);
  let browser;
  try {
    browser = await puppeteer.launch({
      headless: "new",
      args: ["--no-sandbox", "--disable-setuid-sandbox", "--disable-web-security", "--disable-dev-shm-usage"],
      protocolTimeout: 300000,
    });
    const page = await browser.newPage();
    const isLandscapeView = landscape === true || url.includes('attendance-report');
    await page.setViewport({ width: isLandscapeView ? 1400 : 1280, height: 900 });

    // Log errors for debugging
    page.on("pageerror", (err) => console.log("PAGE ERROR:", err.message));
    page.on("requestfailed", (req) => console.log("FAILED REQUEST:", req.url()));

    console.log("Loading page...");
    await page.goto(url, { waitUntil: "networkidle2", timeout: 300000 });
    console.log("Page loaded (networkidle0)");

    // Override CSS for attendance reports - landscape
    if (isLandscapeView) {
      await page.addStyleTag({ content: '@page { size: A4 landscape !important; }' });
    }

    // Wait for table to appear (means data has loaded and rendered)
    try {
      await page.waitForSelector("table tbody tr", { timeout: 30000 });
      console.log("Table rows found");
    } catch (e) {
      console.log("No table rows found, waiting extra time...");
    }

    // Extra wait for any animations/transitions
    await new Promise((r) => setTimeout(r, 2000));

    // Check what's on the page
    const info = await page.evaluate(() => {
      const tables = document.querySelectorAll("table");
      const rows = document.querySelectorAll("table tbody tr");
      const body = document.body;
      return {
        tables: tables.length,
        rows: rows.length,
        bodyHeight: body.scrollHeight,
        bodyWidth: body.scrollWidth,
        title: document.title,
        rootHTML: document.getElementById("root")?.innerHTML?.substring(0, 200) || "EMPTY",
      };
    });
    console.log("Page info:", JSON.stringify(info));

    // Auto-detect landscape for attendance reports, or use request param
    const isLandscape = landscape === true || url.includes('attendance-report');
    const pdf = await page.pdf({
      format: format || "A4",
      landscape: isLandscape,
      printBackground: true,
      margin: { top: "5mm", bottom: "5mm", left: "5mm", right: "5mm" },
    });

    console.log("PDF generated:", pdf.length, "bytes");
    res.set({ "Content-Type": "application/pdf", "Content-Disposition": "attachment" });
    res.send(pdf);
  } catch (err) {
    console.error("PDF error:", err.message);
    res.status(500).json({ error: err.message });
  } finally {
    if (browser) await browser.close();
  }
});

const PORT = 3002;
app.listen(PORT, () => console.log(`PDF service running on http://localhost:${PORT}`));
